/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  MAP background writer coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "storage/map.h"
#include "storage/mapsuper_internal.h"
#include "storage/proc.h"
#include "storage/umbra.h"

static bool MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum);
static bool MapMaybePreallocateFork(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									ForkNumber forknum);
static bool MapForkPreallocSettings(ForkNumber forknum, BlockNumber *soft_low,
									BlockNumber *batch_blocks);

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

static bool
MapForkPreallocSettings(ForkNumber forknum, BlockNumber *soft_low,
						BlockNumber *batch_blocks)
{
	int			low;
	int			batch;

	switch (forknum)
	{
		case MAIN_FORKNUM:
			low = map_prealloc_main_low;
			batch = map_prealloc_main_batch;
			break;
		case FSM_FORKNUM:
			low = map_prealloc_fsm_low;
			batch = map_prealloc_fsm_batch;
			break;
		case VISIBILITYMAP_FORKNUM:
			low = map_prealloc_vm_low;
			batch = map_prealloc_vm_batch;
			break;
		default:
			return false;
	}

	if (low <= 0 || batch <= 0)
		return false;

	*soft_low = (BlockNumber) low;
	*batch_blocks = (BlockNumber) batch;
	return true;
}

static bool
MapMaybePreallocateFork(UmbraFileContext *map_ctx, RelFileLocator rnode,
						ForkNumber forknum)
{
	MapSuperEntry *entry;
	BlockNumber		soft_low;
	BlockNumber		batch_blocks;
	BlockNumber		capacity;
	BlockNumber		demand;
	BlockNumber		remaining;
	BlockNumber		target_nblocks;
	uint32			prealloc_flag;
	bool			prealloc_ok = false;
	bool			started = false;
	uint64			target64;

	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapForkPreallocSettings(forknum, &soft_low, &batch_blocks))
		return false;

	if (!MapSBlockEnsureLoaded(map_ctx, rnode))
		return false;

	prealloc_flag = MapSuperPreallocFlag(forknum);
	Assert(prealloc_flag != 0);

	if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
		return false;

	if (!entry->in_use ||
		(entry->flags & MAPSUPER_FLAG_VALID) == 0 ||
		(entry->flags & MAPSUPER_FLAG_CORRUPT) != 0 ||
		!MapSuperForkExists(&entry->super, forknum))
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	if ((entry->flags & MAPSUPER_FLAG_DIRTY) == 0 &&
		!MapSuperblockCheckCRC(&entry->super))
	{
		LWLockRelease(&entry->lock);
		if (!InRecovery)
			MapSBlockReportCorrupt(rnode, "invalid identity or CRC");
		return false;
	}

	capacity = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&entry->super,
																	   forknum));
	demand = MapSuperGetPreallocTarget(entry, forknum);
	remaining = (capacity > demand) ? (capacity - demand) : 0;

	if (demand == 0 || remaining > soft_low)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	if ((entry->runtime_flags & prealloc_flag) != 0)
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	target64 = Max((uint64) demand + (uint64) soft_low,
				   (uint64) capacity + (uint64) batch_blocks);
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

	entry->runtime_flags |= prealloc_flag;
	LWLockRelease(&entry->lock);
	started = true;

	PG_TRY();
	{
		if (umfile_ctx_fork_exists(map_ctx, forknum))
		{
			if (umbra_chunk_zero_fill_all_slots)
				prealloc_ok = MapSBlockEnsurePhysicalNblocksZeroFill(map_ctx,
																	 rnode,
																	 forknum,
																	 target_nblocks,
																	 false);
			else
				prealloc_ok = MapSBlockEnsurePhysicalNblocks(map_ctx,
															 rnode,
															 forknum,
															 target_nblocks,
															 false);
		}
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
			MapSuperGetPreallocTarget(entry, forknum) <= target_nblocks)
			MapSuperSetPreallocTarget(entry, forknum, 0);
		entry->runtime_flags &= ~prealloc_flag;
		LWLockRelease(&entry->lock);
	}

	return prealloc_ok;
}

static bool
MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum)
{
	BlockNumber		soft_low;
	BlockNumber		batch_blocks;
	BlockNumber		capacity;
	BlockNumber		demand;
	BlockNumber		remaining;
	uint32			prealloc_flag;

	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapForkPreallocSettings(forknum, &soft_low, &batch_blocks))
		return false;

	if (!MapSuperForkExists(&entry->super, forknum))
		return false;

	prealloc_flag = MapSuperPreallocFlag(forknum);
	Assert(prealloc_flag != 0);

	demand = MapSuperGetPreallocTarget(entry, forknum);
	if (demand == 0)
		return false;

	capacity = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&entry->super,
																	   forknum));
	remaining = (capacity > demand) ? (capacity - demand) : 0;
	if (remaining > soft_low)
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

	max_scan = Min(MapSuperCapacity, Max(1024, max_relations * 64));

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
		prealloc_main = MapForkNeedsPrealloc(entry, MAIN_FORKNUM);
		prealloc_fsm = MapForkNeedsPrealloc(entry, FSM_FORKNUM);
		prealloc_vm = MapForkNeedsPrealloc(entry, VISIBILITYMAP_FORKNUM);
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
			MapMaybePreallocateFork(ctx, rnode, MAIN_FORKNUM))
			prealloc_ops++;
		if (prealloc_fsm &&
			MapMaybePreallocateFork(ctx, rnode, FSM_FORKNUM))
			prealloc_ops++;
		if (prealloc_vm &&
			MapMaybePreallocateFork(ctx, rnode, VISIBILITYMAP_FORKNUM))
			prealloc_ops++;
	}

	if (visited == 0)
		scan_slot = 0;

	return prealloc_ops;
}
