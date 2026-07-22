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
static void MapPageUnpinBufferNoOwner(int slot_id);
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
static void MapSelectorLocation(BlockNumber logical_block,
							BlockNumber *map_block, int *byte_offset,
							int *bit_offset);
static uint8 MapSelectorRead(const char *page, int byte_offset,
							 int bit_offset);
static void MapSelectorWrite(char *page, int byte_offset, int bit_offset,
							 uint8 active_slot);
static bool MapPagePinBufferNoOwner(int slot_id, bool adjust_usage);

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
		MapPageBuffer buffer;
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
			!MapPageFlushBuffer(slot_id, NULL, NULL, true))
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

/*
 * Take a raw pin on a resident selector page without allocating or starting
 * I/O.  WAL preparation can run inside a critical section, so a cache miss or
 * lock conflict leaves the caller with its ordinary full-page image.
 */
bool
MapPageBufferTryReadCached(RelFileLocatorBackend rlocator,
						   BlockNumber map_block, LWLockMode mode,
						   MapPageBuffer *buffer)
{
	MapPageTag tag = {0};
	MapPageDesc *desc;
	uint32		hashcode;
	uint64		state;
	int			slot_id;

	Assert(buffer != NULL);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	buffer->desc = NULL;
	if (!MapPagePoolIsInitialized())
		return false;

	tag.rlocator = rlocator;
	tag.map_block = map_block;
	hashcode = MapPageCacheHashCode(&tag);
	if (!LWLockConditionalAcquire(MapPageCachePartitionLock(hashcode),
							  LW_SHARED))
		return false;
	slot_id = MapPageCacheLookup(&tag, hashcode);
	if (slot_id >= 0 && !MapPagePinBufferNoOwner(slot_id, false))
		slot_id = -1;
	LWLockRelease(MapPageCachePartitionLock(hashcode));
	if (slot_id < 0)
		return false;

	desc = &MapPageDescriptors[slot_id];
	state = pg_atomic_read_u64(&desc->state);
	if ((state & MAP_PAGE_IO_IN_PROGRESS) != 0 ||
		!LWLockConditionalAcquire(&desc->content_lock, mode))
	{
		MapPageUnpinBufferNoOwner(slot_id);
		return false;
	}
	state = pg_atomic_read_u64(&desc->state);
	if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) !=
		(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) ||
		!MapPageTagEquals(&desc->tag, &tag))
	{
		LWLockRelease(&desc->content_lock);
		MapPageUnpinBufferNoOwner(slot_id);
		return false;
	}

	buffer->desc = desc;
	return true;
}

void
MapPageMarkBufferDirty(MapPageBuffer buffer, XLogRecPtr wal_flush_lsn,
					   bool skipFsync)
{
	MapPageDesc *desc = buffer.desc;
	uint64      set_bits = MAP_PAGE_DIRTY;

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
	MapPageUnpinBuffer(desc->slot_id);
}

void
MapPageReleaseBufferNoOwner(MapPageBuffer buffer)
{
	MapPageDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	LWLockRelease(&desc->content_lock);
	MapPageUnpinBufferNoOwner(desc->slot_id);
}

void
MapPageUnlockBufferKeepPin(MapPageBuffer buffer)
{
	MapPageDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	LWLockRelease(&desc->content_lock);
}

uint8
MapGetActiveSlot(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				 BlockNumber logical_block)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int         byte_offset;
	int         bit_offset;
	uint8       active_slot;
	LWLock     *extension_lock;

	MapSelectorLocation(logical_block, &map_block, &byte_offset, &bit_offset);

	/* A missing selector page has the on-disk default: every entry is slot 0. */
	extension_lock = MapPageExtensionLock(rlocator);
	LWLockAcquire(extension_lock, LW_SHARED);
	if (map_block >= umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM))
	{
		LWLockRelease(extension_lock);
		return 0;
	}
	buffer = MapPageBufferRead(ctx, rlocator, map_block, false, false,
						   LW_SHARED);
	LWLockRelease(extension_lock);
	active_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							 bit_offset);
	MapPageReleaseBuffer(buffer);
	if (active_slot >= 3)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid Umbra active slot %u for logical block %u",
						active_slot, logical_block)));
	return active_slot;
}

void
MapEnsureActiveSlotPages(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 BlockNumber first_block, BlockNumber nblocks,
						 bool skipFsync)
{
	uint64		logical_end;
	BlockNumber first_map_block;
	BlockNumber last_map_block;

	Assert(ctx != NULL);
	Assert(CritSectionCount == 0);
	if (nblocks == 0)
		return;
	logical_end = (uint64) first_block + nblocks - 1;
	if (logical_end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot page range overflow")));
	MapSelectorLocation(first_block, &first_map_block, NULL, NULL);
	MapSelectorLocation((BlockNumber) logical_end, &last_map_block, NULL,
						NULL);
	for (BlockNumber map_block = first_map_block;
		 map_block <= last_map_block; map_block++)
	{
		MapPageBuffer buffer =
			MapPageBufferRead(ctx, rlocator, map_block, true, skipFsync,
							  LW_SHARED);

		MapPageReleaseBuffer(buffer);
	}
}

bool
MapPrepareSlotShift(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					BlockNumber logical_block, MapSlotShift *shift)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int			byte_offset;
	int			bit_offset;
	uint8		source_slot;

	Assert(shift != NULL);
	MemSet(shift, 0, sizeof(*shift));
	shift->map_slot_id = -1;
	(void) ctx;
	MapSelectorLocation(logical_block, &map_block, &byte_offset, &bit_offset);
	if (!MapPageBufferTryReadCached(rlocator, map_block, LW_SHARED, &buffer))
		return false;

	source_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							  bit_offset);
	if (source_slot >= 3)
	{
		MapPageReleaseBufferNoOwner(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid Umbra active slot %u for logical block %u",
						source_slot, logical_block)));
	}
	shift->rlocator = rlocator;
	shift->logical_block = logical_block;
	shift->source_slot = source_slot;
	shift->target_slot = (source_slot + 1) % 3;
	shift->map_slot_id = buffer.desc->slot_id;
	shift->prepared = true;
	MapPageUnlockBufferKeepPin(buffer);
	return true;
}

void
MapAbortSlotShift(MapSlotShift *shift)
{
	Assert(shift != NULL);
	if (shift->prepared)
	{
		MapPageUnpinBufferNoOwner(shift->map_slot_id);
		shift->prepared = false;
		shift->map_slot_id = -1;
	}
}

void
MapPublishSlotShift(MapSlotShift *shift, XLogRecPtr lsn)
{
	MapPageBuffer buffer;
	MapPageDesc *desc;
	BlockNumber map_block;
	int			byte_offset;
	int			bit_offset;
	uint8		source_slot;
	uint64		state;

	Assert(shift != NULL);
	Assert(shift->prepared);
	Assert(XLogRecPtrIsValid(lsn));
	desc = &MapPageDescriptors[shift->map_slot_id];
	buffer.desc = desc;
	MapSelectorLocation(shift->logical_block, &map_block, &byte_offset,
						&bit_offset);
	LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
	state = pg_atomic_read_u64(&desc->state);
	if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) !=
		(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) ||
		desc->tag.map_block != map_block ||
		!RelFileLocatorBackendEquals(desc->tag.rlocator, shift->rlocator))
		elog(PANIC, "Umbra prepared slot selector changed before publication");
	source_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							  bit_offset);
	if (source_slot != shift->source_slot)
		elog(PANIC, "Umbra active slot changed before publication");
	MapSelectorWrite(MapPageBufferGetData(buffer), byte_offset, bit_offset,
					 shift->target_slot);
	MapPageMarkBufferDirty(buffer, lsn, false);
	MapPageReleaseBufferNoOwner(buffer);
	shift->prepared = false;
	shift->map_slot_id = -1;
}

void
MapRedoSlotShift(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				 BlockNumber logical_block, uint8 source_slot,
				 uint8 target_slot)
{
	MapPageBuffer buffer;
	BlockNumber map_block;
	int			byte_offset;
	int			bit_offset;
	uint8		current_slot;

	Assert(ctx != NULL);
	Assert(source_slot < 3);
	Assert(target_slot < 3);
	Assert(source_slot != target_slot);
	(void) source_slot;
	MapSelectorLocation(logical_block, &map_block, &byte_offset, &bit_offset);
	buffer = MapPageBufferRead(ctx, rlocator, map_block, true, false,
						   LW_EXCLUSIVE);
	current_slot = MapSelectorRead(MapPageBufferGetData(buffer), byte_offset,
							 bit_offset);
	/*
	 * A prior failed recovery can leave a later selector value on disk.  WAL
	 * order is authoritative, so replay this record's target unconditionally.
	 */
	if (current_slot != target_slot)
	{
		MapSelectorWrite(MapPageBufferGetData(buffer), byte_offset, bit_offset,
						 target_slot);
		MapPageMarkBufferDirty(buffer, InvalidXLogRecPtr, false);
	}
	MapPageReleaseBuffer(buffer);
}

void
MapPagePinBuffer(int slot_id, bool adjust_usage)
{
	if (!MapPagePinBufferNoOwner(slot_id, adjust_usage))
		elog(ERROR, "Umbra MAP page buffer reference count overflow");
	MapPageRememberPin(slot_id);
}

static bool
MapPagePinBufferNoOwner(int slot_id, bool adjust_usage)
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
	MapPageUnpinBufferNoOwner(slot_id);
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
	MapPageUnpinBufferNoOwner(slot_id);
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
				   UmbraFileContext *ctx, bool conditional)
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
	if ((state & MAP_PAGE_DIRTY) == 0)
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
	if ((state & MAP_PAGE_DIRTY) == 0)
	{
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		return true;
	}
	Assert((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
		   (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID));

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
		MapPageUpdateState(desc, 0,
						   MAP_PAGE_DIRTY | MAP_PAGE_NEEDS_FSYNC |
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
	MapPageUnpinBufferNoOwner(slot_id);
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
MapPageUnpinBufferNoOwner(int slot_id)
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

	nblocks = umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);
	if (tag->map_block >= nblocks)
	{
		if (!extend)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("missing Umbra active-slot page %u", tag->map_block)));
		extension_lock = MapPageExtensionLock(tag->rlocator);
		LWLockAcquire(extension_lock, LW_EXCLUSIVE);
		nblocks = umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);
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
		pg_atomic_write_u64(&desc->state, MAP_PAGE_GET_REFCOUNT(state));
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
MapSelectorLocation(BlockNumber logical_block, BlockNumber *map_block,
					int *byte_offset, int *bit_offset)
{
	uint64      page_index;
	uint64      entry_index;
	uint64      bit_index;

	Assert(map_block != NULL);
	page_index = (uint64) logical_block / UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	entry_index = (uint64) logical_block % UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE;
	if (page_index >= (uint64) InvalidBlockNumber -
		UMBRA_MAP_SELECTOR_FIRST_BLOCK)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot page address overflow")));
	bit_index = entry_index * UMBRA_MAP_SELECTOR_BITS;
	*map_block = UMBRA_MAP_SELECTOR_FIRST_BLOCK + (BlockNumber) page_index;
	if (byte_offset != NULL)
		*byte_offset = bit_index / BITS_PER_BYTE;
	if (bit_offset != NULL)
		*bit_offset = bit_index % BITS_PER_BYTE;
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
