/*-------------------------------------------------------------------------
 *
 * mapbuf.c
 *    Shared buffer access and I/O for Umbra active-slot pages.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/map_internal.h"
#include "storage/um_defs.h"
#include "storage/umfile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#define MAP_PAGE_MAX_USAGE_COUNT 5

static void MapPageReleaseResource(Datum res);
static void MapPageReleaseIOResource(Datum res);
static void MapPageUnpinBufferRaw(int slot_id);
static bool MapPageTagEquals(const MapPageTag *left,
							 const MapPageTag *right);
static void MapPageWaitIO(MapPageDesc *desc);
static void MapPageLockPartitions(uint32 old_hash, bool old_valid,
							  uint32 new_hash);
static void MapPageUnlockPartitions(uint32 old_hash, bool old_valid,
								uint32 new_hash);
static void MapPageLoad(UmbraFileContext *ctx, const MapPageTag *tag,
						bool extend, bool skipFsync, char *page);
static bool MapPageDiscardFailedLoad(MapPageDesc *desc,
								 const MapPageTag *tag, uint32 hashcode);
static void MapPageRememberIO(int slot_id);
static void MapPageForgetIO(int slot_id);
static BlockNumber MapSelectorPageBlock(ForkNumber forknum,
									 uint64 page_index);
static void MapSelectorLocation(ForkNumber forknum, BlockNumber logical_block,
								BlockNumber *map_block, int *byte_offset,
								int *bit_offset);
static uint8 MapSelectorRead(const char *page, int byte_offset,
							 int bit_offset);
static void MapSelectorWrite(char *page, int byte_offset, int bit_offset,
							 uint8 active_slot);
static bool MapPagePinBufferRaw(int slot_id, bool adjust_usage);

static const ResourceOwnerDesc map_page_resowner_desc =
{
	.name = "Umbra MAP page buffer",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_BUFFER_PINS,
	.ReleaseResource = MapPageReleaseResource,
	.DebugPrint = NULL
};

static const ResourceOwnerDesc map_page_io_resowner_desc =
{
	.name = "Umbra MAP page I/O",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_BUFFER_IOS,
	.ReleaseResource = MapPageReleaseIOResource,
	.DebugPrint = NULL
};

MapPageBuffer
MapPageBufferRead(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				  BlockNumber map_block, bool extend, bool skipFsync,
				  LWLockMode mode)
{
	MapPageTag tag = {0};
	uint32      hashcode;

	Assert(ctx != NULL);
	Assert(map_block >= UMBRA_MAP_SELECTOR_FIRST_BLOCK);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	MapPageEnsureInitialized();
	tag.rlocator = rlocator;
	tag.map_block = map_block;
	hashcode = MapPageCacheHashCode(&tag);

	for (;;)
	{
		MapPageBuffer buffer = {NULL, false};
		MapPageDesc *desc;
		MapPageTag old_tag = {0};
		uint32      old_hash = 0;
		uint64      state;
		int         existing_slot;
		int         slot_id;
		bool        old_valid;

		ResourceOwnerEnlarge(CurrentResourceOwner);
		LWLockAcquire(MapPageCachePartitionLock(hashcode), LW_SHARED);
		slot_id = MapPageCacheLookup(&tag, hashcode);
		if (slot_id >= 0)
			MapPagePinBuffer(slot_id, true);
		LWLockRelease(MapPageCachePartitionLock(hashcode));

		if (slot_id >= 0)
		{
			desc = &MapPageDescriptors[slot_id];
			MapPageWaitIO(desc);
			state = pg_atomic_read_u64(&desc->state);
			if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) !=
				(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID))
			{
				bool        discarded;

				discarded = MapPageDiscardFailedLoad(desc, &tag, hashcode);
				if (discarded)
					MapPageClockFreeBuffer(slot_id);
				MapPageUnpinBuffer(slot_id);
				continue;
			}

			LWLockAcquire(&desc->content_lock, mode);
			state = pg_atomic_read_u64(&desc->state);
			if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
				(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) &&
				MapPageTagEquals(&desc->tag, &tag))
			{
				buffer.desc = desc;
				return buffer;
			}
			LWLockRelease(&desc->content_lock);
			MapPageUnpinBuffer(slot_id);
			continue;
		}

		slot_id = MapPageClockGetBuffer();
		desc = &MapPageDescriptors[slot_id];
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_DIRTY) != 0 &&
			!MapPageFlushBuffer(slot_id, NULL, NULL, true, false))
		{
			MapPageUnpinBuffer(slot_id);
			continue;
		}

		ResourceOwnerEnlarge(CurrentResourceOwner);
		if (!LWLockConditionalAcquire(&desc->io_lock, LW_EXCLUSIVE))
		{
			MapPageUnpinBuffer(slot_id);
			continue;
		}
		if (!LWLockConditionalAcquire(&desc->content_lock, LW_EXCLUSIVE))
		{
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBuffer(slot_id);
			continue;
		}

		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 1 ||
			(state & (MAP_PAGE_DIRTY | MAP_PAGE_IO_IN_PROGRESS)) != 0)
		{
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBuffer(slot_id);
			continue;
		}

		old_valid = (state & MAP_PAGE_TAG_VALID) != 0;
		if (old_valid)
		{
			old_tag = desc->tag;
			old_hash = MapPageCacheHashCode(&old_tag);
		}

		MapPageLockPartitions(old_hash, old_valid, hashcode);
		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 1 ||
			(state & (MAP_PAGE_DIRTY | MAP_PAGE_IO_IN_PROGRESS)) != 0 ||
			old_valid != ((state & MAP_PAGE_TAG_VALID) != 0) ||
			(old_valid && !MapPageTagEquals(&desc->tag, &old_tag)))
		{
			MapPageUnlockPartitions(old_hash, old_valid, hashcode);
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBuffer(slot_id);
			continue;
		}

		MapPageRememberIO(slot_id);
		existing_slot = MapPageCacheInsert(&tag, hashcode, slot_id);
		if (existing_slot >= 0)
		{
			MapPageForgetIO(slot_id);
			MapPageUnlockPartitions(old_hash, old_valid, hashcode);
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBuffer(slot_id);
			continue;
		}
		if (old_valid)
			MapPageCacheDelete(&old_tag, old_hash, slot_id);

		desc->tag = tag;
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		pg_atomic_write_u64(&desc->state,
							MAP_PAGE_GET_REFCOUNT(state) |
							MAP_PAGE_USAGE_ONE |
							MAP_PAGE_TAG_VALID |
							MAP_PAGE_IO_IN_PROGRESS);
		MapPageUnlockPartitions(old_hash, old_valid, hashcode);
		LWLockRelease(&desc->content_lock);

		/* ResourceOwner cleanup clears an unfinished input I/O on ERROR. */
		MapPageLoad(ctx, &tag, extend, skipFsync, MapPageGetBlock(slot_id));

		LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
		MapPageUpdateState(desc, MAP_PAGE_VALID,
						   MAP_PAGE_IO_IN_PROGRESS | MAP_PAGE_IO_ERROR);
		LWLockRelease(&desc->content_lock);
		MapPageForgetIO(slot_id);
		LWLockRelease(&desc->io_lock);

		LWLockAcquire(&desc->content_lock, mode);
		buffer.desc = desc;
		return buffer;
	}
}

/*
 * A critical section cannot grow CurrentResourceOwner.  This path owns its
 * descriptor pins directly.  A MapPageLoad failure in this path is PANIC.
 */
static MapPageBuffer
MapPageBufferReadRaw(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 BlockNumber map_block, bool extend, bool skipFsync,
					 LWLockMode mode)
{
	MapPageTag tag = {0};
	uint32      hashcode;
	bool        known_exists = false;

	Assert(CritSectionCount > 0);
	Assert(ctx != NULL);
	Assert(map_block >= UMBRA_MAP_SELECTOR_FIRST_BLOCK);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	MapPageEnsureInitialized();
	tag.rlocator = rlocator;
	tag.map_block = map_block;
	hashcode = MapPageCacheHashCode(&tag);

	for (;;)
	{
		MapPageBuffer buffer = {NULL, true};
		MapPageDesc *desc;
		MapPageTag old_tag = {0};
		uint32      old_hash = 0;
		uint64      state;
		int         existing_slot;
		int         slot_id;
		bool        old_valid;

		LWLockAcquire(MapPageCachePartitionLock(hashcode), LW_SHARED);
		slot_id = MapPageCacheLookup(&tag, hashcode);
		if (slot_id >= 0)
		{
			if (!MapPagePinBufferRaw(slot_id, true))
				elog(ERROR, "Umbra MAP page buffer reference count overflow");
		}
		LWLockRelease(MapPageCachePartitionLock(hashcode));

		if (slot_id >= 0)
		{
			desc = &MapPageDescriptors[slot_id];
			MapPageWaitIO(desc);
			state = pg_atomic_read_u64(&desc->state);
			if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) !=
				(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID))
			{
				bool        discarded;

				discarded = MapPageDiscardFailedLoad(desc, &tag, hashcode);
				if (discarded)
					MapPageClockFreeBuffer(slot_id);
				MapPageUnpinBufferRaw(slot_id);
				continue;
			}

			LWLockAcquire(&desc->content_lock, mode);
			state = pg_atomic_read_u64(&desc->state);
			if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
				(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) &&
				MapPageTagEquals(&desc->tag, &tag))
			{
				buffer.desc = desc;
				return buffer;
			}
			LWLockRelease(&desc->content_lock);
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}

		/* Check the metadata fork only after a cache miss. */
		if (!extend && !known_exists)
		{
			LWLock     *extension_lock = MapPageExtensionLock(rlocator);

			LWLockAcquire(extension_lock, LW_SHARED);
			known_exists = map_block <
				umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);
			LWLockRelease(extension_lock);
			if (!known_exists)
			{
				buffer.desc = NULL;
				return buffer;
			}
		}

		slot_id = MapPageClockGetBufferRaw();
		desc = &MapPageDescriptors[slot_id];
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_DIRTY) != 0 &&
			!MapPageFlushBuffer(slot_id, NULL, NULL, true, false))
		{
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}

		if (!LWLockConditionalAcquire(&desc->io_lock, LW_EXCLUSIVE))
		{
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}
		if (!LWLockConditionalAcquire(&desc->content_lock, LW_EXCLUSIVE))
		{
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}

		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 1 ||
			(state & (MAP_PAGE_DIRTY | MAP_PAGE_IO_IN_PROGRESS)) != 0)
		{
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}

		old_valid = (state & MAP_PAGE_TAG_VALID) != 0;
		if (old_valid)
		{
			old_tag = desc->tag;
			old_hash = MapPageCacheHashCode(&old_tag);
		}

		MapPageLockPartitions(old_hash, old_valid, hashcode);
		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 1 ||
			(state & (MAP_PAGE_DIRTY | MAP_PAGE_IO_IN_PROGRESS)) != 0 ||
			old_valid != ((state & MAP_PAGE_TAG_VALID) != 0) ||
			(old_valid && !MapPageTagEquals(&desc->tag, &old_tag)))
		{
			MapPageUnlockPartitions(old_hash, old_valid, hashcode);
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}

		existing_slot = MapPageCacheInsert(&tag, hashcode, slot_id);
		if (existing_slot >= 0)
		{
			MapPageUnlockPartitions(old_hash, old_valid, hashcode);
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			MapPageUnpinBufferRaw(slot_id);
			continue;
		}
		if (old_valid)
			MapPageCacheDelete(&old_tag, old_hash, slot_id);

		desc->tag = tag;
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		pg_atomic_write_u64(&desc->state,
							MAP_PAGE_GET_REFCOUNT(state) |
							MAP_PAGE_USAGE_ONE |
							MAP_PAGE_TAG_VALID |
							MAP_PAGE_IO_IN_PROGRESS);
		MapPageUnlockPartitions(old_hash, old_valid, hashcode);
		LWLockRelease(&desc->content_lock);

		MapPageLoad(ctx, &tag, extend, skipFsync, MapPageGetBlock(slot_id));

		LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
		MapPageUpdateState(desc, MAP_PAGE_VALID,
						   MAP_PAGE_IO_IN_PROGRESS | MAP_PAGE_IO_ERROR);
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);

		LWLockAcquire(&desc->content_lock, mode);
		buffer.desc = desc;
		return buffer;
	}
}

char *
MapPageBufferGetData(MapPageBuffer buffer)
{
	MapPageDesc *desc = buffer.desc;
	uint64      state;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	state = pg_atomic_read_u64(&desc->state);
	Assert((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
		   (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID));
	return MapPageGetBlock(desc->slot_id);
}

void
MapPageMarkBufferDirty(MapPageBuffer buffer, XLogRecPtr wal_flush_lsn,
					   bool skipFsync)
{
	MapPageDesc *desc = buffer.desc;
	uint64      set_bits = MAP_PAGE_DIRTY | MAP_PAGE_JUST_DIRTIED;

	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	Assert((pg_atomic_read_u64(&desc->state) & MAP_PAGE_VALID) != 0);
	if (!skipFsync)
		set_bits |= MAP_PAGE_NEEDS_FSYNC;
	if (XLogRecPtrIsValid(wal_flush_lsn))
		desc->wal_flush_lsn = Max(desc->wal_flush_lsn, wal_flush_lsn);
	MapPageUpdateState(desc, set_bits, MAP_PAGE_IO_ERROR);
}

void
MapPageReleaseBuffer(MapPageBuffer buffer)
{
	MapPageDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	LWLockRelease(&desc->content_lock);
	if (buffer.raw_pin)
		MapPageUnpinBufferRaw(desc->slot_id);
	else
		MapPageUnpinBuffer(desc->slot_id);
}

uint8
MapGetActiveSlot(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				 ForkNumber forknum, BlockNumber logical_block)
{
	return MapGetActiveSlotWithPresence(ctx, rlocator, forknum, logical_block,
										NULL);
}

uint8
MapGetActiveSlotWithPresence(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 ForkNumber forknum, BlockNumber logical_block,
							 bool *selector_page_present)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int         byte_offset;
	int         bit_offset;
	uint8       active_slot;
	LWLock     *extension_lock;

	MapSelectorLocation(forknum, logical_block, &map_block, &byte_offset,
						&bit_offset);

	if (selector_page_present != NULL)
		*selector_page_present = false;

	/* A missing selector page has the on-disk default: every entry is slot 0. */
	extension_lock = MapPageExtensionLock(rlocator);
	LWLockAcquire(extension_lock, LW_SHARED);
	if (!umfile_exists(ctx, UMBRA_METADATA_FORKNUM) ||
		map_block >= umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM))
	{
		LWLockRelease(extension_lock);
		return 0;
	}
	buffer = MapPageBufferRead(ctx, rlocator, map_block, false, false,
						   LW_SHARED);
	LWLockRelease(extension_lock);
	if (selector_page_present != NULL)
		*selector_page_present = true;
	active_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							 bit_offset);
	MapPageReleaseBuffer(buffer);
	if (active_slot >= 3)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid Umbra active slot %u for fork %d logical block %u",
							active_slot, (int) forknum, logical_block)));
	return active_slot;
}

void
MapEnsureActiveSlotPages(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 ForkNumber forknum, BlockNumber first_block,
						 BlockNumber nblocks,
						 bool skipFsync)
{
	uint64		logical_end;
	uint64		first_page_index;
	uint64		last_page_index;
	uint64		page_index;

	Assert(ctx != NULL);
	Assert(CritSectionCount == 0);
	if (nblocks == 0)
		return;
	if (!umfile_exists(ctx, UMBRA_METADATA_FORKNUM))
		umfile_create(ctx, UMBRA_METADATA_FORKNUM, false);
	logical_end = (uint64) first_block + nblocks - 1;
	if (logical_end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot page range overflow")));
	first_page_index = (uint64) first_block /
		UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	last_page_index = logical_end / UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	for (page_index = first_page_index; page_index <= last_page_index;
		 page_index++)
	{
		BlockNumber	map_block = MapSelectorPageBlock(forknum, page_index);
		MapPageBuffer buffer;

		buffer = MapPageBufferRead(ctx, rlocator, map_block, true, skipFsync,
								   LW_SHARED);
		MapPageReleaseBuffer(buffer);
	}
}

void
MapResetActiveSlots(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					ForkNumber forknum, BlockNumber first_block,
					BlockNumber nblocks,
					bool skipFsync)
{
	uint64		logical_end;
	uint64		first_page_index;
	uint64		last_page_index;
	uint64		page_index;
	LWLock	   *extension_lock;

	Assert(ctx != NULL);
	Assert(CritSectionCount == 0);
	if (nblocks == 0 || !umfile_exists(ctx, UMBRA_METADATA_FORKNUM))
		return;
	logical_end = (uint64) first_block + nblocks - 1;
	if (logical_end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot reset range overflow")));
	first_page_index = (uint64) first_block /
		UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	last_page_index = logical_end / UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	extension_lock = MapPageExtensionLock(rlocator);

	for (page_index = first_page_index; page_index <= last_page_index;
		 page_index++)
	{
		BlockNumber	map_block = MapSelectorPageBlock(forknum, page_index);
		MapPageBuffer buffer;
		uint64		first_entry = 0;
		uint64		last_entry = UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE - 1;
		bool		changed = false;
		char	   *page;

		/* A missing selector page already represents the slot-0 default. */
		LWLockAcquire(extension_lock, LW_SHARED);
		if (map_block >= umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM))
		{
			LWLockRelease(extension_lock);
			continue;
		}
		buffer = MapPageBufferRead(ctx, rlocator, map_block, false, skipFsync,
								   LW_EXCLUSIVE);
		LWLockRelease(extension_lock);
		if (page_index == first_page_index)
			first_entry = (uint64) first_block %
				UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
		if (page_index == last_page_index)
			last_entry = logical_end % UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
		page = MapPageBufferGetData(buffer);
		for (uint64 entry = first_entry; entry <= last_entry; entry++)
		{
			uint64		bit_index = entry * UMBRA_MAP_SELECTOR_BITS;
			int			byte_offset = bit_index / BITS_PER_BYTE;
			int			bit_offset = bit_index % BITS_PER_BYTE;

			if (MapSelectorRead(page, byte_offset, bit_offset) != 0)
			{
				MapSelectorWrite(page, byte_offset, bit_offset, 0);
				changed = true;
			}
		}
		if (changed)
			MapPageMarkBufferDirty(buffer, InvalidXLogRecPtr, skipFsync);
		MapPageReleaseBuffer(buffer);
	}
}

void
MapPublishSlotShift(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					ForkNumber forknum, BlockNumber logical_block,
					uint8 source_slot,
					uint8 target_slot, XLogRecPtr lsn)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int			byte_offset;
	int			bit_offset;
	uint8		current_slot;

	Assert(ctx != NULL);
	Assert(source_slot < 3);
	Assert(target_slot < 3);
	Assert(target_slot == (source_slot + 1) % 3);
	Assert(XLogRecPtrIsValid(lsn));
	MapSelectorLocation(forknum, logical_block, &map_block, &byte_offset,
						&bit_offset);
	/* Publication creates the default-zero selector page when necessary. */
	Assert(CritSectionCount > 0);
	buffer = MapPageBufferReadRaw(ctx, rlocator, map_block, true, false,
							  LW_EXCLUSIVE);
	if (buffer.desc == NULL)
		elog(PANIC, "missing Umbra active-slot page %u", map_block);
	current_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							   bit_offset);
	if (current_slot != source_slot)
		elog(PANIC, "Umbra active slot changed before publication");
	MapSelectorWrite(MapPageBufferGetData(buffer), byte_offset, bit_offset,
					 target_slot);
	MapPageMarkBufferDirty(buffer, lsn, false);
	MapPageReleaseBuffer(buffer);
}

void
MapRedoSetActiveSlot(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 ForkNumber forknum, BlockNumber logical_block,
					 uint8 active_slot)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int			byte_offset;
	int			bit_offset;
	uint8		current_slot;

	Assert(ctx != NULL);
	Assert(active_slot < 3);
	MapSelectorLocation(forknum, logical_block, &map_block, &byte_offset,
						&bit_offset);
	buffer = MapPageBufferRead(ctx, rlocator, map_block, true, false,
						   LW_EXCLUSIVE);
	current_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							 bit_offset);
	/*
	 * Recovery order is authoritative, so a replay can restore its source
	 * selector before materializing that page or publish its target afterward.
	 */
	if (current_slot != active_slot)
	{
		MapSelectorWrite(MapPageBufferGetData(buffer), byte_offset, bit_offset,
						 active_slot);
		MapPageMarkBufferDirty(buffer, InvalidXLogRecPtr, false);
	}
	MapPageReleaseBuffer(buffer);
}

void
MapRedoSlotShift(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				 ForkNumber forknum, BlockNumber logical_block,
				 uint8 source_slot,
				 uint8 target_slot)
{
	Assert(source_slot < 3);
	Assert(target_slot < 3);
	Assert(source_slot != target_slot);
	(void) source_slot;
	MapRedoSetActiveSlot(ctx, rlocator, forknum, logical_block, target_slot);
}

void
MapPagePinBuffer(int slot_id, bool adjust_usage)
{
	if (!MapPagePinBufferRaw(slot_id, adjust_usage))
		elog(ERROR, "Umbra MAP page buffer reference count overflow");
	MapPageRememberPin(slot_id);
}

static bool
MapPagePinBufferRaw(int slot_id, bool adjust_usage)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	uint64		old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		uint64		new_state;

		if (MAP_PAGE_GET_REFCOUNT(old_state) == UINT32_MAX)
			return false;
		new_state = old_state + 1;
		if (adjust_usage && MAP_PAGE_GET_USAGE(old_state) < MAP_PAGE_MAX_USAGE_COUNT)
			new_state += MAP_PAGE_USAGE_ONE;
		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
									   new_state))
			return true;
	}
}

void
MapPageRememberPin(int slot_id)
{
	ResourceOwnerRemember(CurrentResourceOwner, Int32GetDatum(slot_id),
					  &map_page_resowner_desc);
}

void
MapPageUnpinBuffer(int slot_id)
{
	ResourceOwnerForget(CurrentResourceOwner, Int32GetDatum(slot_id),
					 &map_page_resowner_desc);
	MapPageUnpinBufferRaw(slot_id);
}

bool
MapPageTryClaimBuffer(int slot_id)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	uint64      old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		if (MAP_PAGE_GET_REFCOUNT(old_state) != 0)
			return false;
		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
									   old_state + 1))
			return true;
	}
}

void
MapPageReleaseClaimBuffer(int slot_id)
{
	MapPageUnpinBufferRaw(slot_id);
}

void
MapPageUpdateState(MapPageDesc *desc, uint64 set_bits, uint64 clear_bits)
{
	uint64      old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		uint64      new_state = (old_state | set_bits) & ~clear_bits;

		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
									   new_state))
			return;
	}
}

bool
MapPageFlushBuffer(int slot_id, const MapPageTag *expected_tag,
				   UmbraFileContext *ctx, bool conditional, bool checkpoint)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	UmbraFileContext *volatile temporary_ctx = NULL;
	UmbraFileContext *write_ctx;
	const void *buffers[1];
	uint64      state;
	bool        content_locked = false;

	Assert(ctx == NULL || expected_tag != NULL);
	if (conditional)
	{
		if (!LWLockConditionalAcquire(&desc->io_lock, LW_EXCLUSIVE))
			return false;
	}
	else
		LWLockAcquire(&desc->io_lock, LW_EXCLUSIVE);

	state = pg_atomic_read_u64(&desc->state);
	if ((state & MAP_PAGE_DIRTY) == 0 ||
		(checkpoint && (state & MAP_PAGE_CHECKPOINT_NEEDED) == 0))
	{
		LWLockRelease(&desc->io_lock);
		return true;
	}
	if (conditional)
		content_locked = LWLockConditionalAcquire(&desc->content_lock,
											  LW_EXCLUSIVE);
	else
	{
		LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
		content_locked = true;
	}
	if (!content_locked)
	{
		LWLockRelease(&desc->io_lock);
		return false;
	}

	state = pg_atomic_read_u64(&desc->state);
	if (expected_tag != NULL &&
		((state & MAP_PAGE_TAG_VALID) == 0 ||
		 !MapPageTagEquals(&desc->tag, expected_tag)))
	{
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		return true;
	}
	if ((state & MAP_PAGE_DIRTY) == 0 ||
		(checkpoint && (state & MAP_PAGE_CHECKPOINT_NEEDED) == 0))
	{
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		return true;
	}
	Assert((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
		   (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID));
	/* A subsequent dirtying identifies work for a later checkpoint. */
	MapPageUpdateState(desc, 0, MAP_PAGE_JUST_DIRTIED);

	PG_TRY();
	{
		if (ctx == NULL)
		{
			temporary_ctx = umfile_open_temporary(desc->tag.rlocator);
			write_ctx = temporary_ctx;
		}
		else
			write_ctx = ctx;
		if (XLogRecPtrIsValid(desc->wal_flush_lsn))
			XLogFlush(desc->wal_flush_lsn);
		buffers[0] = MapPageGetBlock(slot_id);
		umfile_writev(write_ctx, UMBRA_METADATA_FORKNUM,
					  desc->tag.map_block, buffers, 1,
					  (state & MAP_PAGE_NEEDS_FSYNC) == 0);
		if (temporary_ctx != NULL)
		{
			umfile_destroy(temporary_ctx);
			temporary_ctx = NULL;
		}
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		state = pg_atomic_read_u64(&desc->state);
		MapPageUpdateState(desc, 0,
						   ((state & MAP_PAGE_JUST_DIRTIED) == 0 ?
							MAP_PAGE_DIRTY : 0) |
						   MAP_PAGE_NEEDS_FSYNC |
						   MAP_PAGE_CHECKPOINT_NEEDED |
						   MAP_PAGE_IO_ERROR);
	}
	PG_CATCH();
	{
		if (temporary_ctx != NULL)
			umfile_destroy(temporary_ctx);
		MapPageUpdateState(desc, MAP_PAGE_IO_ERROR, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(&desc->content_lock);
	LWLockRelease(&desc->io_lock);
	return true;
}

static void
MapPageReleaseResource(Datum res)
{
	int         slot_id = DatumGetInt32(res);
	MapPageDesc *desc = &MapPageDescriptors[slot_id];

	if (LWLockHeldByMe(&desc->content_lock))
		LWLockRelease(&desc->content_lock);
	MapPageUnpinBufferRaw(slot_id);
}

static void
MapPageReleaseIOResource(Datum res)
{
	MapPageDesc *desc = &MapPageDescriptors[DatumGetInt32(res)];
	uint64      old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	while ((old_state & MAP_PAGE_IO_IN_PROGRESS) != 0)
	{
		uint64      new_state = (old_state | MAP_PAGE_IO_ERROR) &
			~MAP_PAGE_IO_IN_PROGRESS;

		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
									   new_state))
			break;
	}
	if (LWLockHeldByMe(&desc->io_lock))
		LWLockRelease(&desc->io_lock);
}

static void
MapPageUnpinBufferRaw(int slot_id)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	uint64      old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		if (MAP_PAGE_GET_REFCOUNT(old_state) == 0)
			elog(PANIC, "Umbra MAP page buffer reference count underflow");
		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
									   old_state - 1))
			return;
	}
}

static bool
MapPageTagEquals(const MapPageTag *left, const MapPageTag *right)
{
	return RelFileLocatorBackendEquals(left->rlocator, right->rlocator) &&
		left->map_block == right->map_block;
}

static void
MapPageWaitIO(MapPageDesc *desc)
{
	while ((pg_atomic_read_u64(&desc->state) & MAP_PAGE_IO_IN_PROGRESS) != 0)
	{
		LWLockAcquire(&desc->io_lock, LW_SHARED);
		LWLockRelease(&desc->io_lock);
	}
}

static void
MapPageLockPartitions(uint32 old_hash, bool old_valid, uint32 new_hash)
{
	int         new_partition = MapPageCachePartition(new_hash);

	if (!old_valid || MapPageCachePartition(old_hash) == new_partition)
	{
		LWLockAcquire(MapPageCachePartitionLock(new_hash), LW_EXCLUSIVE);
		return;
	}
	if (MapPageCachePartition(old_hash) < new_partition)
	{
		LWLockAcquire(MapPageCachePartitionLock(old_hash), LW_EXCLUSIVE);
		LWLockAcquire(MapPageCachePartitionLock(new_hash), LW_EXCLUSIVE);
	}
	else
	{
		LWLockAcquire(MapPageCachePartitionLock(new_hash), LW_EXCLUSIVE);
		LWLockAcquire(MapPageCachePartitionLock(old_hash), LW_EXCLUSIVE);
	}
}

static void
MapPageUnlockPartitions(uint32 old_hash, bool old_valid, uint32 new_hash)
{
	int         new_partition = MapPageCachePartition(new_hash);

	LWLockRelease(MapPageCachePartitionLock(new_hash));
	if (old_valid && MapPageCachePartition(old_hash) != new_partition)
		LWLockRelease(MapPageCachePartitionLock(old_hash));
}

static void
MapPageLoad(UmbraFileContext *ctx, const MapPageTag *tag,
			bool extend, bool skipFsync, char *page)
{
	PGIOAlignedBlock empty_page;
	BlockNumber nblocks;
	LWLock     *extension_lock;
	void       *buffers[1];
	bool        extended_target = false;

	nblocks = umfile_exists(ctx, UMBRA_METADATA_FORKNUM) ?
		umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM) : 0;
	if (tag->map_block >= nblocks)
	{
		if (!extend)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("missing Umbra active-slot page %u", tag->map_block)));
		extension_lock = MapPageExtensionLock(tag->rlocator);
		LWLockAcquire(extension_lock, LW_EXCLUSIVE);
		nblocks = umfile_exists(ctx, UMBRA_METADATA_FORKNUM) ?
			umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM) : 0;
		MemSet(empty_page.data, 0, BLCKSZ);
		while (nblocks <= tag->map_block)
		{
			umfile_extend(ctx, UMBRA_METADATA_FORKNUM, nblocks,
						  empty_page.data, skipFsync);
			extended_target = nblocks == tag->map_block;
			nblocks++;
		}
		LWLockRelease(extension_lock);
	}

	if (extended_target)
		MemSet(page, 0, BLCKSZ);
	else
	{
		buffers[0] = page;
		umfile_readv(ctx, UMBRA_METADATA_FORKNUM, tag->map_block, buffers, 1);
	}
}

static bool
MapPageDiscardFailedLoad(MapPageDesc *desc, const MapPageTag *tag,
							 uint32 hashcode)
{
	uint64      state;
	bool        discarded = false;

	LWLockAcquire(&desc->io_lock, LW_EXCLUSIVE);
	LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
	LWLockAcquire(MapPageCachePartitionLock(hashcode), LW_EXCLUSIVE);
	state = pg_atomic_read_u64(&desc->state);
	if ((state & MAP_PAGE_TAG_VALID) != 0 &&
		(state & (MAP_PAGE_VALID | MAP_PAGE_IO_IN_PROGRESS)) == 0 &&
		MapPageTagEquals(&desc->tag, tag))
	{
		MapPageCacheDelete(tag, hashcode, desc->slot_id);
		MemSet(&desc->tag, 0, sizeof(desc->tag));
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		/* The failed loader can still release its pin concurrently. */
		MapPageUpdateState(desc, 0, ~MAP_PAGE_REFCOUNT_MASK);
		discarded = true;
	}
	LWLockRelease(MapPageCachePartitionLock(hashcode));
	LWLockRelease(&desc->content_lock);
	LWLockRelease(&desc->io_lock);
	return discarded;
}

static void
MapPageRememberIO(int slot_id)
{
	ResourceOwnerRemember(CurrentResourceOwner, Int32GetDatum(slot_id),
					  &map_page_io_resowner_desc);
}

static void
MapPageForgetIO(int slot_id)
{
	ResourceOwnerForget(CurrentResourceOwner, Int32GetDatum(slot_id),
					 &map_page_io_resowner_desc);
}

static void
MapSelectorLocation(ForkNumber forknum, BlockNumber logical_block,
					BlockNumber *map_block,
					int *byte_offset, int *bit_offset)
{
	uint64      page_index;
	uint64      entry_index;
	uint64      bit_index;

	Assert(map_block != NULL);
	page_index = (uint64) logical_block / UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	entry_index = (uint64) logical_block % UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	bit_index = entry_index * UMBRA_MAP_SELECTOR_BITS;
	*map_block = MapSelectorPageBlock(forknum, page_index);
	if (byte_offset != NULL)
		*byte_offset = bit_index / BITS_PER_BYTE;
	if (bit_offset != NULL)
		*bit_offset = bit_index % BITS_PER_BYTE;
}

static BlockNumber
MapSelectorPageBlock(ForkNumber forknum, uint64 page_index)
{
	uint64		group;
	uint64		within_group;
	uint64		map_block;

	switch (forknum)
	{
		case FSM_FORKNUM:
			group = page_index;
			within_group = 0;
			break;
		case VISIBILITYMAP_FORKNUM:
			group = page_index;
			within_group = UMBRA_MAP_SELECTOR_FSM_PAGES_PER_GROUP;
			break;
		case MAIN_FORKNUM:
			group = page_index / UMBRA_MAP_SELECTOR_MAIN_PAGES_PER_GROUP;
			within_group = UMBRA_MAP_SELECTOR_FSM_PAGES_PER_GROUP +
				UMBRA_MAP_SELECTOR_VM_PAGES_PER_GROUP +
				(page_index % UMBRA_MAP_SELECTOR_MAIN_PAGES_PER_GROUP);
			break;
		default:
			elog(ERROR, "unsupported fork number %d in Umbra selector lookup",
				 (int) forknum);
			return InvalidBlockNumber;
	}

	map_block = UMBRA_MAP_SELECTOR_FIRST_BLOCK +
		group * UMBRA_MAP_SELECTOR_GROUP_PAGES + within_group;
	if (map_block > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot page address overflow for fork %d",
						(int) forknum)));
	return (BlockNumber) map_block;
}

static uint8
MapSelectorRead(const char *page, int byte_offset, int bit_offset)
{
	return (((const uint8 *) page)[byte_offset] >> bit_offset) & 0x03;
}

static void
MapSelectorWrite(char *page, int byte_offset, int bit_offset,
				 uint8 active_slot)
{
	uint8		mask = UINT8_C(0x03) << bit_offset;
	uint8		*entry = (uint8 *) page + byte_offset;

	Assert(active_slot < 3);
	*entry = (*entry & ~mask) | (active_slot << bit_offset);
}
