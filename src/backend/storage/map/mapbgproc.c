/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  MAP background maintenance and coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper_internal.h"
#include "storage/proc.h"

static bool MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum,
								 bool background_mode);

uint32
MapAllocPressurePeek(void)
{
	return pg_atomic_read_u32(&MapShared->num_allocs);
}

void
MapStrategyNotifyWriter(int mapwriter_procno)
{
	SpinLockAcquire(&MapShared->clock_lock);
	MapShared->mapwriter_procno = mapwriter_procno;
	SpinLockRelease(&MapShared->clock_lock);
}

void
MapWakeWriter(void)
{
	int			mapwriter_procno = -1;

	SpinLockAcquire(&MapShared->clock_lock);
	mapwriter_procno = MapShared->mapwriter_procno;
	if (mapwriter_procno != -1)
		MapShared->mapwriter_procno = -1;
	SpinLockRelease(&MapShared->clock_lock);

	if (mapwriter_procno != -1)
		SetLatch(&ProcGlobal->allProcs[mapwriter_procno].procLatch);
}

bool
MapMaybePreallocateFork(UmbraFileContext *map_ctx, RelFileLocator rnode,
						ForkNumber forknum, bool background_mode)
{
	MapSuperEntry *entry;
	BlockNumber		soft_low;
	BlockNumber		hard_low;
	BlockNumber		batch_blocks;
	BlockNumber		next;
	BlockNumber		capacity;
	BlockNumber		remaining;
	BlockNumber		target_nblocks;
	uint32			prealloc_flag;
	bool			prealloc_ok = false;
	bool			started = false;
	uint64			target64;

	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapForkPreallocSettings(forknum, &soft_low, &hard_low, &batch_blocks))
		return false;

	if (!MapSBlockEnsureLoaded(map_ctx, rnode))
		return false;

	prealloc_flag = MapSuperPreallocFlag(forknum);
	Assert(prealloc_flag != 0);

	if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
		return false;

	if (!entry->in_use || (entry->flags & MAPSUPER_FLAG_VALID) == 0)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	if ((entry->flags & MAPSUPER_FLAG_CORRUPT) ||
		!MapSuperblockHasValidIdentity(&entry->super) ||
		((entry->flags & MAPSUPER_FLAG_DIRTY) == 0 &&
		 !MapSuperblockCheckCRC(&entry->super)))
	{
		LWLockRelease(&entry->lock);
		if (!InRecovery)
			MapSBlockReportCorrupt(rnode, "invalid identity or CRC");
		return false;
	}

	Assert(MapNormalizeForkBlockCount(forknum,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		forknum)) <=
		   MapSuperGetReservedNextFree(entry, forknum));
	next = MapSuperGetReservedNextFree(entry, forknum);
	capacity = MapSuperblockGetPhysCapacity(&entry->super, forknum);
	capacity = MapNormalizeForkBlockCount(forknum, capacity);

	if (next < soft_low)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	remaining = (capacity > next) ? (capacity - next) : 0;
	if (remaining > soft_low)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	if (!background_mode && remaining > hard_low)
	{
		LWLockRelease(&entry->lock);
		MapWakeWriter();
		return false;
	}

	if ((entry->runtime_flags & prealloc_flag) != 0)
	{
		LWLockRelease(&entry->lock);
		if (!background_mode)
			MapWakeWriter();
		return false;
	}

	target64 = Max((uint64) capacity + (uint64) batch_blocks,
				   (uint64) next + (uint64) batch_blocks);
	if (target64 > (uint64) (InvalidBlockNumber - 1))
	{
		LWLockRelease(&entry->lock);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot preallocate physical blocks beyond %u for relation %u/%u/%u fork %d",
						InvalidBlockNumber - 1,
						rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum)));
	}
	target_nblocks = (BlockNumber) target64;

	if (target_nblocks <= capacity)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	entry->runtime_flags |= prealloc_flag;
	LWLockRelease(&entry->lock);
	started = true;

	PG_TRY();
	{
		if (umfile_ctx_fork_exists(map_ctx, forknum, UMFILE_EXISTS_SPARSE))
			prealloc_ok = umfile_ctx_preallocate_blocks(map_ctx, forknum,
														UMFILE_NBLOCKS_SPARSE,
														target_nblocks);
	}
	PG_CATCH();
	{
		if (started && MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
		{
			entry->runtime_flags &= ~prealloc_flag;
			LWLockRelease(&entry->lock);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
	{
		if (prealloc_ok &&
			entry->in_use &&
			(entry->flags & MAPSUPER_FLAG_VALID) != 0 &&
			MapNormalizeForkBlockCount(forknum,
									   MapSuperblockGetPhysCapacity(&entry->super,
																   forknum)) < target_nblocks)
		{
			XLogRecPtr	map_lsn = GetXLogWriteRecPtr();

			MapSuperblockSetPhysCapacity(&entry->super, forknum, target_nblocks);
			MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
			entry->page_lsn = map_lsn;
			entry->flags |= MAPSUPER_FLAG_DIRTY;
		}
		entry->runtime_flags &= ~prealloc_flag;
		LWLockRelease(&entry->lock);
	}

	if (!background_mode && !prealloc_ok)
		MapWakeWriter();

	return prealloc_ok;
}

static bool
MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum,
					 bool background_mode)
{
	BlockNumber		soft_low;
	BlockNumber		hard_low;
	BlockNumber		batch_blocks;
	BlockNumber		next;
	BlockNumber		capacity;
	BlockNumber		remaining;
	uint32			prealloc_flag;

	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapForkPreallocSettings(forknum, &soft_low, &hard_low, &batch_blocks))
		return false;

	if (!MapSuperForkExists(&entry->super, forknum))
		return false;

	prealloc_flag = MapSuperPreallocFlag(forknum);
	Assert(prealloc_flag != 0);

	Assert(MapNormalizeForkBlockCount(forknum,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		forknum)) <=
		   MapSuperGetReservedNextFree(entry, forknum));
	next = MapSuperGetReservedNextFree(entry, forknum);
	capacity = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&entry->super,
																   forknum));

	if (next < soft_low)
		return false;

	remaining = (capacity > next) ? (capacity - next) : 0;
	if (remaining > soft_low)
		return false;

	if (!background_mode && remaining > hard_low)
		return false;

	if ((entry->runtime_flags & prealloc_flag) != 0)
		return false;

	return true;
}

int
MapPreallocStep(int max_relations)
{
	static int	scan_slot = 0;
	int			max_scan;
	int			scanned = 0;
	int			visited = 0;
	int			prealloc_ops = 0;

	if (InRecovery || max_relations <= 0 || MapSuperCapacity <= 0)
		return 0;

	max_scan = Min(MapSuperCapacity, Max(64, max_relations * 8));

	while (scanned < max_scan && visited < max_relations)
	{
		MapSuperEntry *entry;
		RelFileLocator	rnode;
		RelFileLocatorBackend rlocator;
		UmbraFileContext *ctx;
		bool		prealloc_main;
		bool		prealloc_fsm;
		bool		prealloc_vm;

		entry = MapSuperEntryBySlot(scan_slot);
		scan_slot = (scan_slot + 1) % MapSuperCapacity;
		scanned++;

		LWLockAcquire(&entry->lock, LW_SHARED);
		if (!entry->in_use)
		{
			LWLockRelease(&entry->lock);
			continue;
		}
		rnode = entry->key.rnode;
		if ((entry->flags & MAPSUPER_FLAG_VALID) == 0 ||
			(entry->flags & MAPSUPER_FLAG_CORRUPT) != 0 ||
			!MapSuperblockHasValidIdentity(&entry->super) ||
			((entry->flags & MAPSUPER_FLAG_DIRTY) == 0 &&
			 !MapSuperblockCheckCRC(&entry->super)))
		{
			LWLockRelease(&entry->lock);
			continue;
		}
		prealloc_main = MapForkNeedsPrealloc(entry, MAIN_FORKNUM, true);
		prealloc_fsm = MapForkNeedsPrealloc(entry, FSM_FORKNUM, true);
		prealloc_vm = MapForkNeedsPrealloc(entry, VISIBILITYMAP_FORKNUM, true);
		LWLockRelease(&entry->lock);
		visited++;

		if (!prealloc_main && !prealloc_fsm && !prealloc_vm)
			continue;

		rlocator.locator = rnode;
		rlocator.backend = INVALID_PROC_NUMBER;
		ctx = umfile_ctx_acquire(rlocator);
		if (ctx == NULL)
			continue;

		if (prealloc_main &&
			MapMaybePreallocateFork(ctx, rnode, MAIN_FORKNUM, true))
			prealloc_ops++;
		if (prealloc_fsm &&
			MapMaybePreallocateFork(ctx, rnode, FSM_FORKNUM, true))
			prealloc_ops++;
		if (prealloc_vm &&
			MapMaybePreallocateFork(ctx, rnode, VISIBILITYMAP_FORKNUM, true))
			prealloc_ops++;
	}

	return prealloc_ops;
}
