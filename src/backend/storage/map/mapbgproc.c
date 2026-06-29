/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  MAP background maintenance and coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "storage/map.h"
#include "storage/mapsuper_internal.h"
#include "storage/proc.h"
#include "storage/relfilelocator.h"
#include "storage/umfile.h"
#include "storage/umbra.h"

static bool MapForkPreallocSettings(ForkNumber forknum,
									BlockNumber *soft_low,
									BlockNumber *batch_blocks);
static bool MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum,
								 BlockNumber *demand_p);
static bool MapMaybePreallocateFork(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									ForkNumber forknum);
static BlockNumber MapPreallocRoundGroups(BlockNumber nblocks);
static bool MapPreallocDemandForFork(const MapSuperEntry *entry,
									 ForkNumber forknum,
									 BlockNumber *demand_p);

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

	*soft_low = MapPreallocRoundGroups((BlockNumber) low);
	*batch_blocks = MapPreallocRoundGroups((BlockNumber) batch);
	return true;
}

static BlockNumber
MapPreallocRoundGroups(BlockNumber nblocks)
{
	uint64		group_blocks;
	uint64		rounded;

	if (nblocks == 0)
		return 0;

	group_blocks = UMBRA_CHUNK_ACTIVE_SLOTS *
		(uint64) UMBRA_CHUNK_PAIRED_PAGES;
	rounded = ((uint64) nblocks + group_blocks - 1) / group_blocks;
	rounded *= group_blocks;
	if (rounded > (uint64) (InvalidBlockNumber - 1))
		return InvalidBlockNumber - 1;
	return (BlockNumber) rounded;
}

static bool
MapPreallocDemandForFork(const MapSuperEntry *entry, ForkNumber forknum,
						 BlockNumber *demand_p)
{
	BlockNumber logical_nblocks;

	Assert(entry != NULL);
	Assert(demand_p != NULL);

	if (!MapSuperForkExists(&entry->super, forknum))
		return false;

	logical_nblocks = MapNormalizeForkBlockCount(forknum,
												 MapSuperblockGetLogicalNblocks(&entry->super,
																				forknum));
	if (logical_nblocks == 0)
		return false;
	if (!UmbraChunkPairedPhysicalCapacity(logical_nblocks, demand_p))
		return false;
	return *demand_p > 0;
}

static bool
MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum,
					 BlockNumber *demand_p)
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
	if (!MapPreallocDemandForFork(entry, forknum, &demand))
		return false;

	capacity = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&entry->super,
																	   forknum));
	remaining = (capacity > demand) ? (capacity - demand) : 0;
	if (remaining > soft_low)
		return false;

	prealloc_flag = MapSuperPreallocFlag(forknum);
	Assert(prealloc_flag != 0);
	if ((entry->runtime_flags & prealloc_flag) != 0)
		return false;

	if (demand_p != NULL)
		*demand_p = demand;
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
	BlockNumber		actual_nblocks;
	BlockNumber		demand;
	BlockNumber		remaining;
	BlockNumber		target_nblocks;
	BlockNumber		published_nblocks = 0;
	uint32			prealloc_flag;
	bool			prealloc_ok = false;
	bool			started = false;
	uint64			target64;

	if (RecoveryInProgress())
		return false;
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
		if (!RecoveryInProgress())
			MapSBlockReportCorrupt(rnode, "invalid identity or CRC");
		return false;
	}

	if (!MapPreallocDemandForFork(entry, forknum, &demand))
	{
		LWLockRelease(&entry->lock);
		return false;
	}

	capacity = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&entry->super,
																	   forknum));
	remaining = (capacity > demand) ? (capacity - demand) : 0;
	if (remaining > soft_low)
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
	target64 = (uint64) MapPreallocRoundGroups((BlockNumber)
											   Min(target64,
												   (uint64) (InvalidBlockNumber - 1)));
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
			actual_nblocks = umfile_nblocks(map_ctx, forknum);

			if (umbra_chunk_zero_fill_all_slots)
			{
				BlockNumber	zero_start = actual_nblocks;

				while (zero_start < target_nblocks)
				{
					int			zero_blocks;

					zero_blocks = (int) Min(target_nblocks - zero_start,
											 (BlockNumber) INT_MAX);
					umfile_zeroextend(map_ctx, forknum, zero_start,
									  zero_blocks, false);
					zero_start += (BlockNumber) zero_blocks;
				}
				prealloc_ok = true;
				published_nblocks = target_nblocks;
			}
			else
			{
				prealloc_ok = MapSBlockEnsurePhysicalNblocks(map_ctx, rnode,
															 forknum,
															 target_nblocks,
															 false);
				if (prealloc_ok)
					published_nblocks = target_nblocks;
			}
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
			(entry->flags & MAPSUPER_FLAG_VALID) != 0)
		{
			XLogRecPtr	map_lsn = GetXLogWriteRecPtr();

			if (MapNormalizeForkBlockCount(forknum,
										   MapSuperblockGetPhysCapacity(&entry->super,
																		forknum)) < published_nblocks)
			{
				MapSuperblockSetPhysCapacity(&entry->super, forknum,
											 published_nblocks);
				MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
				entry->page_lsn = map_lsn;
				entry->flags |= MAPSUPER_FLAG_DIRTY;
			}
		}
		entry->runtime_flags &= ~prealloc_flag;
		LWLockRelease(&entry->lock);
	}

	return prealloc_ok;
}

int
MapPreallocStep(int max_relations)
{
	static int	scan_slot = 0;
	int			max_scan;
	int			scanned = 0;
	int			visited = 0;
	int			prealloc_ops = 0;

	if (RecoveryInProgress() || max_relations <= 0 || MapSuperCapacity <= 0)
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
		prealloc_main = MapForkNeedsPrealloc(entry, MAIN_FORKNUM, NULL);
		prealloc_fsm = MapForkNeedsPrealloc(entry, FSM_FORKNUM, NULL);
		prealloc_vm = MapForkNeedsPrealloc(entry, VISIBILITYMAP_FORKNUM, NULL);
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
