/*-------------------------------------------------------------------------
 *
 * mapbuf.c
 *	  Buffer access and I/O for ordinary Umbra MAP pages.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapbuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map_internal.h"
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
									 const MapPageTag *tag,
									 uint32 hashcode);
static void MapPageRememberIO(int slot_id);
static void MapPageForgetIO(int slot_id);

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
				  BlockNumber map_blkno, bool extend, bool skipFsync,
				  LWLockMode mode)
{
	MapPageTag	tag = {0};
	uint32		hashcode;

	Assert(ctx != NULL);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	MapPageEnsureInitialized();
	tag.rlocator = rlocator;
	tag.map_blkno = map_blkno;
	hashcode = MapPageCacheHashCode(&tag);

	for (;;)
	{
		MapPageBuffer buffer;
		MapPageDesc *desc;
		LWLock	   *partition_lock;
		MapPageTag	old_tag = {0};
		uint32		old_hash = 0;
		uint64		state;
		int			existing_slot;
		int			slot_id;
		bool		old_valid;

		ResourceOwnerEnlarge(CurrentResourceOwner);
		partition_lock = MapPageCachePartitionLock(hashcode);
		LWLockAcquire(partition_lock, LW_SHARED);
		slot_id = MapPageCacheLookup(&tag, hashcode);
		if (slot_id >= 0)
			MapPagePinBuffer(slot_id, true);
		LWLockRelease(partition_lock);

		if (slot_id >= 0)
		{
			desc = &MapPageDescriptors[slot_id];
			MapPageWaitIO(desc);
			state = pg_atomic_read_u64(&desc->state);
			if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) !=
				(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID))
			{
				bool		discarded;

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
		pg_atomic_write_u64(&desc->state,
							MAP_PAGE_GET_REFCOUNT(state) |
							MAP_PAGE_USAGE_ONE |
							MAP_PAGE_TAG_VALID |
							MAP_PAGE_IO_IN_PROGRESS);
		MapPageUnlockPartitions(old_hash, old_valid, hashcode);
		LWLockRelease(&desc->content_lock);

		/* ERROR cleanup ends I/O before releasing the pin. */
		MapPageLoad(ctx, &tag, extend, skipFsync,
					MapPageGetBlock(slot_id));

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
	uint64		state;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	state = pg_atomic_read_u64(&desc->state);
	Assert((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
		   (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID));

	return MapPageGetBlock(desc->slot_id);
}

void
MapPageMarkBufferDirty(MapPageBuffer buffer, bool skipFsync)
{
	MapPageDesc *desc = buffer.desc;
	uint64		set_bits = MAP_PAGE_DIRTY;

	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	Assert((pg_atomic_read_u64(&desc->state) & MAP_PAGE_VALID) != 0);
	if (!skipFsync)
		set_bits |= MAP_PAGE_NEEDS_FSYNC;
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
MapPagePinBuffer(int slot_id, bool adjust_usage)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	uint64		old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		uint64		new_state;

		if (MAP_PAGE_GET_REFCOUNT(old_state) == UINT32_MAX)
			elog(ERROR, "Umbra MAP page buffer reference count overflow");
		new_state = old_state + 1;
		if (adjust_usage &&
			MAP_PAGE_GET_USAGE(old_state) < MAP_PAGE_MAX_USAGE_COUNT)
			new_state += MAP_PAGE_USAGE_ONE;
		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
										   new_state))
			break;
	}
	MapPageRememberPin(slot_id);
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
	uint64		old_state;

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
	uint64		old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	for (;;)
	{
		uint64		new_state = (old_state | set_bits) & ~clear_bits;

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
	uint64		state;
	bool		content_locked = false;

	Assert(ctx == NULL || expected_tag != NULL);

	/*
	 * Output is synchronous while both locks are held.  Consequently there is
	 * no output I/O state to publish: an ERROR leaves the page dirty, and
	 * lock cleanup makes it available for a later retry.
	 */

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
		buffers[0] = MapPageGetBlock(slot_id);
		umfile_writev(write_ctx, UMBRA_MAP_FORKNUM,
					  desc->tag.map_blkno, buffers, 1,
					  (state & MAP_PAGE_NEEDS_FSYNC) == 0);
		if (temporary_ctx != NULL)
		{
			umfile_destroy(temporary_ctx);
			temporary_ctx = NULL;
		}
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
	int			slot_id = DatumGetInt32(res);
	MapPageDesc *desc = &MapPageDescriptors[slot_id];

	/* Match shared-buffer cleanup if a caller leaked a content lock. */
	if (LWLockHeldByMe(&desc->content_lock))
		LWLockRelease(&desc->content_lock);
	MapPageUnpinBufferNoOwner(slot_id);
}

static void
MapPageReleaseIOResource(Datum res)
{
	MapPageDesc *desc = &MapPageDescriptors[DatumGetInt32(res)];
	uint64		old_state;

	old_state = pg_atomic_read_u64(&desc->state);
	while ((old_state & MAP_PAGE_IO_IN_PROGRESS) != 0)
	{
		uint64		new_state;

		new_state = (old_state | MAP_PAGE_IO_ERROR) &
			~MAP_PAGE_IO_IN_PROGRESS;
		if (pg_atomic_compare_exchange_u64(&desc->state, &old_state,
										   new_state))
			break;
	}
	/* Abort normally released LWLocks already; cover commit-time leaks too. */
	if (LWLockHeldByMe(&desc->io_lock))
		LWLockRelease(&desc->io_lock);
}

static void
MapPageUnpinBufferNoOwner(int slot_id)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];
	uint64		old_state;

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
		left->map_blkno == right->map_blkno;
}

static void
MapPageWaitIO(MapPageDesc *desc)
{
	while ((pg_atomic_read_u64(&desc->state) &
			MAP_PAGE_IO_IN_PROGRESS) != 0)
	{
		LWLockAcquire(&desc->io_lock, LW_SHARED);
		LWLockRelease(&desc->io_lock);
	}
}

static void
MapPageLockPartitions(uint32 old_hash, bool old_valid, uint32 new_hash)
{
	int			new_partition = MapPageCachePartition(new_hash);

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
	int			new_partition = MapPageCachePartition(new_hash);

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
	LWLock	   *extension_lock;
	void	   *buffers[1];
	bool		extended_target = false;

	nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	if (tag->map_blkno >= nblocks)
	{
		if (!extend)
			elog(ERROR, "missing Umbra MAP page %u", tag->map_blkno);

		extension_lock = MapPageExtensionLock(tag->rlocator);
		LWLockAcquire(extension_lock, LW_EXCLUSIVE);
		nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
		MemSet(empty_page.data, 0xFF, BLCKSZ);
		while (nblocks <= tag->map_blkno)
		{
			umfile_extend(ctx, UMBRA_MAP_FORKNUM, nblocks,
						  empty_page.data, skipFsync);
			extended_target = nblocks == tag->map_blkno;
			nblocks++;
		}
		LWLockRelease(extension_lock);
	}

	if (extended_target)
		MemSet(page, 0xFF, BLCKSZ);
	else
	{
		buffers[0] = page;
		umfile_readv(ctx, UMBRA_MAP_FORKNUM, tag->map_blkno, buffers, 1);
		if (pg_memory_is_all_zeros(page, BLCKSZ))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Umbra MAP page %u is all zeroes",
							tag->map_blkno)));
	}
}

static bool
MapPageDiscardFailedLoad(MapPageDesc *desc, const MapPageTag *tag,
						 uint32 hashcode)
{
	uint64		state;
	bool		discarded = false;

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
