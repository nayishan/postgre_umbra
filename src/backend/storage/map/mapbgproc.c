/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  MAP background maintenance and coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/umbra_xlog.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper_internal.h"
#include "storage/proc.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

static bool MapForkNeedsPrealloc(const MapSuperEntry *entry, ForkNumber forknum,
								 bool background_mode);
static bool MapSegmentHasLiveReferences(UmbraFileContext *map_ctx,
										RelFileLocator rnode,
										ForkNumber forknum,
										BlockNumber segno);
static void MapReclaimInitFileTag(FileTag *tag, RelFileLocator rnode,
								  ForkNumber forknum, BlockNumber segno);
static bool MapReclaimRegisterUnlinkRequest(RelFileLocator rnode,
											ForkNumber forknum,
											BlockNumber segno);
static bool MapReclaimEnqueueSegment(UmbraFileContext *map_ctx,
									 RelFileLocator rnode, ForkNumber forknum,
									 BlockNumber segno);
static void MapReclaimRegisterRelationFilterRequest(RelFileLocator rnode);
static bool MapReclaimEnqueue(UmbraFileContext *map_ctx, RelFileLocator rnode,
							  ForkNumber forknum,
							  BlockNumber extent_no,
							  BlockNumber extent_blocks);
static bool MapSegmentFullyBelowReclaimBoundary(BlockNumber boundary_pblk,
												BlockNumber segno);
static bool MapExtentFullyBelowReclaimBoundary(BlockNumber boundary_pblk,
											   BlockNumber extent_no,
											   BlockNumber extent_blocks);
static BlockNumber MapCompactorAdvanceBoundaryFromSet(BlockNumber boundary_pblk,
													  BlockNumber next_free_pblk,
													  HTAB *committed_pblks);
static bool MapCompactorRelocateEntry(UmbraFileContext *map_ctx,
									  RelFileLocator rnode,
									  ForkNumber forknum,
									  BlockNumber lblkno,
									  BlockNumber old_pblkno);
static int	MapCompactorAnalyzeFork(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									ForkNumber forknum,
									int max_moves,
									int *moves_done);

typedef struct MapExtentLiveEntry
{
	BlockNumber	extent_no;
	uint32		live_blocks;
} MapExtentLiveEntry;

typedef struct MapBoundaryPblkEntry
{
	BlockNumber	pblkno;
} MapBoundaryPblkEntry;

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

void
MapStrategyNotifyCompactor(int mapcompactor_procno)
{
	SpinLockAcquire(&MapShared->clock_lock);
	MapShared->mapcompactor_procno = mapcompactor_procno;
	SpinLockRelease(&MapShared->clock_lock);
}

void
MapWakeCompactor(void)
{
	int			mapcompactor_procno = -1;

	SpinLockAcquire(&MapShared->clock_lock);
	mapcompactor_procno = MapShared->mapcompactor_procno;
	if (mapcompactor_procno != -1)
		MapShared->mapcompactor_procno = -1;
	SpinLockRelease(&MapShared->clock_lock);

	if (mapcompactor_procno != -1)
		SetLatch(&ProcGlobal->allProcs[mapcompactor_procno].procLatch);
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
									   MapSuperblockGetPhysCapacity(&entry->super, forknum)) < target_nblocks)
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
MapSegmentFullyBelowReclaimBoundary(BlockNumber boundary_pblk, BlockNumber segno)
{
	uint64		seg_end;

	seg_end = ((uint64) segno + 1) * (uint64) RELSEG_SIZE;
	return seg_end <= (uint64) boundary_pblk;
}

static bool
MapExtentFullyBelowReclaimBoundary(BlockNumber boundary_pblk,
								   BlockNumber extent_no,
								   BlockNumber extent_blocks)
{
	uint64		extent_end;

	if (extent_blocks == 0)
		return false;

	extent_end = ((uint64) extent_no + 1) * (uint64) extent_blocks;
	return extent_end <= (uint64) boundary_pblk;
}

static BlockNumber
MapCompactorAdvanceBoundaryFromSet(BlockNumber boundary_pblk,
								   BlockNumber next_free_pblk,
								   HTAB *committed_pblks)
{
	while (boundary_pblk < next_free_pblk)
	{
		MapBoundaryPblkEntry *entry;

		entry = (MapBoundaryPblkEntry *) hash_search(committed_pblks,
													 &boundary_pblk,
													 HASH_FIND,
													 NULL);
		if (entry == NULL)
			break;
		boundary_pblk++;
	}

	return boundary_pblk;
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

static bool
MapSegmentHasLiveReferences(UmbraFileContext *map_ctx, RelFileLocator rnode,
							ForkNumber forknum, BlockNumber segno)
{
	BlockNumber	n_lblknos = 0;
	BlockNumber	n_map_pages;
	BlockNumber	current_page = InvalidBlockNumber;
	BlockNumber	page_idx;
	BlockNumber	page_count;
	int			current_slot = -1;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos) ||
		n_lblknos == 0)
		return false;

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM,
								UMFILE_EXISTS_DENSE))
		return false;
	n_map_pages = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM,
										 UMFILE_NBLOCKS_DENSE);
	if (n_map_pages == 0)
		return false;

	page_count = (n_lblknos + MAP_ENTRIES_PER_PAGE - 1) / MAP_ENTRIES_PER_PAGE;
	for (page_idx = 0; page_idx < page_count; page_idx++)
	{
		BlockNumber	page_no = MapForkPageIndexToMapBlkno(forknum, page_idx);
		int			entry_idx;
		int			limit_idx;
		MapPage	   *page;
		MapBufferDesc *buf;

		if (page_no >= n_map_pages)
			break;

		if (page_no != current_page)
		{
			if (current_slot >= 0)
				MapUnpinBuffer(current_slot);
			current_slot = MapReadBuffer(map_ctx, rnode, forknum, page_no);
			current_page = page_no;
		}

		buf = &MapBuffers[current_slot];
		page = MapGetPage(current_slot);
		limit_idx = MAP_ENTRIES_PER_PAGE;
		if (page_idx == page_count - 1 && (n_lblknos % MAP_ENTRIES_PER_PAGE) != 0)
			limit_idx = n_lblknos % MAP_ENTRIES_PER_PAGE;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		for (entry_idx = 0; entry_idx < limit_idx; entry_idx++)
		{
			BlockNumber pblkno = page->pblknos[entry_idx];

			if (pblkno != InvalidBlockNumber &&
				pblkno / ((BlockNumber) RELSEG_SIZE) == segno)
			{
				LWLockRelease(&buf->buffer_lock);
				if (current_slot >= 0)
					MapUnpinBuffer(current_slot);
				return true;
			}
		}
		LWLockRelease(&buf->buffer_lock);
	}

	if (current_slot >= 0)
		MapUnpinBuffer(current_slot);

	return false;
}

static void
MapReclaimInitFileTag(FileTag *tag, RelFileLocator rnode,
					  ForkNumber forknum, BlockNumber segno)
{
	tag->handler = SYNC_HANDLER_UMBRA;
	tag->forknum = forknum;
	tag->rlocator = rnode;
	tag->segno = (uint64) segno;
}

static bool
MapReclaimRegisterUnlinkRequest(RelFileLocator rnode, ForkNumber forknum,
								BlockNumber segno)
{
	FileTag		tag;

	MapReclaimInitFileTag(&tag, rnode, forknum, segno);
	return RegisterSyncRequest(&tag, SYNC_RECLAIM_REQUEST,
							   true /* retryOnError */ );
}

static bool
MapReclaimEnqueueSegment(UmbraFileContext *map_ctx, RelFileLocator rnode,
						 ForkNumber forknum, BlockNumber segno)
{
	BlockNumber	reclaim_boundary_pblk;

	if (!MapSBlockTryGetReclaimBoundary(map_ctx, rnode, forknum,
										&reclaim_boundary_pblk))
		return false;
	if (!MapSegmentFullyBelowReclaimBoundary(reclaim_boundary_pblk, segno))
		return false;

	if (!MapReclaimRegisterUnlinkRequest(rnode, forknum, segno))
		return false;

	elog(DEBUG1,
		 "map reclaim enqueue rel %u/%u/%u fork %u seg %u",
		 rnode.spcOid, rnode.dbOid, rnode.relNumber,
		 forknum, segno);

	MapStatsAddReclaimEnqueued(1);
	return true;
}

static void
MapReclaimRegisterRelationFilterRequest(RelFileLocator rnode)
{
	FileTag		tag;

	MapReclaimInitFileTag(&tag, rnode, InvalidForkNumber, InvalidBlockNumber);
	(void) RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST,
							   true /* retryOnError */ );
}

static bool
MapReclaimEnqueue(UmbraFileContext *map_ctx, RelFileLocator rnode,
				  ForkNumber forknum,
				  BlockNumber extent_no, BlockNumber extent_blocks)
{
	uint64		start64;
	BlockNumber	segno;

	if (extent_blocks == 0)
		return false;

	start64 = (uint64) extent_no * (uint64) extent_blocks;
	if (start64 > (uint64) MaxBlockNumber)
		return false;
	segno = ((BlockNumber) start64) / ((BlockNumber) RELSEG_SIZE);

	return MapReclaimEnqueueSegment(map_ctx, rnode, forknum, segno);
}

void
MapReclaimForgetRelation(RelFileLocator rnode)
{
	MapReclaimRegisterRelationFilterRequest(rnode);
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

static bool
MapCompactorRelocateEntry(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  ForkNumber forknum, BlockNumber lblkno,
						  BlockNumber old_pblkno)
{
	BlockNumber	map_blkno;
	int			entry_idx;
	int			slot_id;
	MapPage	   *page;
	MapBufferDesc *buf;
	BlockNumber	cur_pblkno;
	BlockNumber	new_pblkno;
	XLogRecPtr	map_lsn;
	bool		delay_chkp_start_set = false;
	char		pagebuf[BLCKSZ];

	if (!MapTryReserveFreshPblkno(map_ctx, rnode, forknum, lblkno,
								  &new_pblkno, true))
	{
		return false;
	}

	if (!umfile_ctx_block_exists(map_ctx, forknum, old_pblkno))
	{
		MapInflightRelease(rnode, forknum, lblkno);
		return false;
	}

	/*
	 * Compactor relocation is a raw physical copy, not a shared-buffer page
	 * copy. If a newer image is still resident/dirty in buffer pool, this path
	 * will not refresh that buffer's PageLSN or cached contents.
	 */
	umfile_ctx_prefetch(map_ctx, forknum, old_pblkno);
	umfile_ctx_read(map_ctx, forknum, old_pblkno, pagebuf, BLCKSZ);
	umfile_ctx_extend(map_ctx, forknum, new_pblkno, pagebuf);

	umfile_ctx_register_dirty(map_ctx, forknum, new_pblkno, false, false);

	map_blkno = MapLblknoToMapBlkno(forknum, lblkno);
	entry_idx = map_blkno % MAP_ENTRIES_PER_PAGE;
	map_blkno = map_blkno / MAP_ENTRIES_PER_PAGE;

	slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);
	if (!LWLockConditionalAcquire(&buf->buffer_lock, LW_EXCLUSIVE))
	{
		MapUnpinBuffer(slot_id);
		MapInflightRelease(rnode, forknum, lblkno);
		return false;
	}

	cur_pblkno = page->pblknos[entry_idx];
	if (cur_pblkno != old_pblkno)
	{
		LWLockRelease(&buf->buffer_lock);
		MapUnpinBuffer(slot_id);
		MapInflightRelease(rnode, forknum, lblkno);
		return false;
	}

	START_CRIT_SECTION();
	if ((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0)
	{
		MyProc->delayChkptFlags |= DELAY_CHKPT_START;
		delay_chkp_start_set = true;
	}

	map_lsn = log_umbra_map_set(rnode, forknum, lblkno, old_pblkno, new_pblkno);
	page->pblknos[entry_idx] = new_pblkno;
	MapMarkBufferDirty(map_ctx, buf, map_lsn);

	LWLockRelease(&buf->buffer_lock);
	MapUnpinBuffer(slot_id);
	MapSBlockBumpPhysicalState(map_ctx, rnode, forknum, new_pblkno + 1,
							   true, true, map_lsn);

	if (delay_chkp_start_set)
		MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	END_CRIT_SECTION();

	MapInflightRelease(rnode, forknum, lblkno);
	MapStatsAddCompactorRelocations(1);

	return true;
}

static int
MapCompactorAnalyzeFork(UmbraFileContext *map_ctx, RelFileLocator rnode,
						ForkNumber forknum,
						int max_moves, int *moves_done)
{
	HASHCTL		ctl;
	HASHCTL		boundary_ctl;
	HTAB	   *extent_live;
	HTAB	   *boundary_committed = NULL;
	bool	   *segment_live = NULL;
	MemoryContext extent_ctx;
	HASH_SEQ_STATUS seq;
	MapExtentLiveEntry *live_entry;
	BlockNumber	n_lblknos = 0;
	BlockNumber	n_map_pages;
	BlockNumber	current_page = InvalidBlockNumber;
	BlockNumber	page_idx;
	BlockNumber	page_count;
	BlockNumber	max_extent_no = InvalidBlockNumber;
	BlockNumber	extent_blocks;
	BlockNumber	next_free_pblk = 0;
	BlockNumber	reclaim_boundary_pblk = 0;
	BlockNumber	advanced_boundary_pblk = 0;
	BlockNumber	reclaim_seg_limit = 0;
	int			current_slot = -1;
	int			sparse_count = 0;
	int			live_threshold;
	int			moves_limit;
	int			moved_here = 0;

	if (!map_compactor_enable)
		return 0;
	if (!MapForkHasMappedState(forknum))
		return 0;
	if (map_compactor_extent_blocks <= 0)
		return 0;
	extent_blocks = (BlockNumber) map_compactor_extent_blocks;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos) ||
		n_lblknos == 0)
		return 0;
	if (!MapSBlockTryGetNextFreePhysBlock(map_ctx, rnode, forknum,
										  &next_free_pblk) ||
		next_free_pblk == 0)
		return 0;
	if (!MapSBlockTryGetReclaimBoundary(map_ctx, rnode, forknum,
										&reclaim_boundary_pblk))
		return 0;
	if (reclaim_boundary_pblk > next_free_pblk)
		reclaim_boundary_pblk = next_free_pblk;
	reclaim_seg_limit = reclaim_boundary_pblk / ((BlockNumber) RELSEG_SIZE);

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM,
								UMFILE_EXISTS_DENSE))
		return 0;
	n_map_pages = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM,
										 UMFILE_NBLOCKS_DENSE);
	if (n_map_pages == 0)
		return 0;
	page_count = (n_lblknos + MAP_ENTRIES_PER_PAGE - 1) / MAP_ENTRIES_PER_PAGE;
	if (page_count == 0)
		return 0;

	extent_ctx = AllocSetContextCreate(CurrentMemoryContext,
									   "MapCompactorExtentLiveContext",
									   ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BlockNumber);
	ctl.entrysize = sizeof(MapExtentLiveEntry);
	ctl.hcxt = extent_ctx;
	extent_live = hash_create("Map Compactor Extent Live",
							  1024,
							  &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	if (reclaim_seg_limit > 0)
		segment_live = MemoryContextAllocZero(extent_ctx,
											  sizeof(bool) * reclaim_seg_limit);
	if (reclaim_boundary_pblk < next_free_pblk)
	{
		MemSet(&boundary_ctl, 0, sizeof(boundary_ctl));
		boundary_ctl.keysize = sizeof(BlockNumber);
		boundary_ctl.entrysize = sizeof(MapBoundaryPblkEntry);
		boundary_ctl.hcxt = extent_ctx;
		boundary_committed = hash_create("Map Compactor Boundary Committed",
										 1024,
										 &boundary_ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	for (page_idx = 0; page_idx < page_count; page_idx++)
	{
		BlockNumber	page_no = MapForkPageIndexToMapBlkno(forknum, page_idx);
		int			entry_idx;
		int			limit_idx;
		MapPage	   *page;
		MapBufferDesc *buf;
		BlockNumber	extent_no;
		BlockNumber	segno;
		bool		found;

		if (page_no >= n_map_pages)
			break;

		if (page_no != current_page)
		{
			if (current_slot >= 0)
				MapUnpinBuffer(current_slot);
			current_slot = MapReadBuffer(map_ctx, rnode, forknum, page_no);
			current_page = page_no;
		}

		buf = &MapBuffers[current_slot];
		page = MapGetPage(current_slot);
		if (!LWLockConditionalAcquire(&buf->buffer_lock, LW_SHARED))
			continue;
		limit_idx = MAP_ENTRIES_PER_PAGE;
		if (page_idx == page_count - 1 && (n_lblknos % MAP_ENTRIES_PER_PAGE) != 0)
			limit_idx = n_lblknos % MAP_ENTRIES_PER_PAGE;
		for (entry_idx = 0; entry_idx < limit_idx; entry_idx++)
		{
			BlockNumber pblkno = page->pblknos[entry_idx];

			if (pblkno == InvalidBlockNumber)
				continue;

			if (boundary_committed != NULL &&
				pblkno >= reclaim_boundary_pblk &&
				pblkno < next_free_pblk)
			{
				(void) hash_search(boundary_committed,
								   &pblkno,
								   HASH_ENTER,
								   NULL);
			}

			if (pblkno >= reclaim_boundary_pblk)
				continue;

			extent_no = pblkno / extent_blocks;
			segno = pblkno / ((BlockNumber) RELSEG_SIZE);
			if (max_extent_no == InvalidBlockNumber || extent_no > max_extent_no)
				max_extent_no = extent_no;

			live_entry = (MapExtentLiveEntry *) hash_search(extent_live,
															&extent_no,
															HASH_ENTER,
															&found);
			if (!found)
				live_entry->live_blocks = 0;
			if (live_entry->live_blocks < extent_blocks)
				live_entry->live_blocks++;

			if (segment_live != NULL && segno < reclaim_seg_limit)
				segment_live[segno] = true;
		}
		LWLockRelease(&buf->buffer_lock);
	}

	if (current_slot >= 0)
		MapUnpinBuffer(current_slot);

	live_threshold = map_compactor_low_live_percent;
	if (live_threshold < 1)
		live_threshold = 1;
	if (live_threshold > 100)
		live_threshold = 100;
	moves_limit = Max(0, max_moves);

	hash_seq_init(&seq, extent_live);
	while ((live_entry = (MapExtentLiveEntry *) hash_seq_search(&seq)) != NULL)
	{
		int			live_pct;

		if (max_extent_no != InvalidBlockNumber &&
			live_entry->extent_no == max_extent_no)
			continue;
		if (!MapExtentFullyBelowReclaimBoundary(reclaim_boundary_pblk,
												live_entry->extent_no,
												extent_blocks))
			continue;

		live_pct = (int) (((uint64) live_entry->live_blocks * 100) /
						  Max((uint64) 1, (uint64) extent_blocks));
		if (live_pct > live_threshold)
			continue;

		sparse_count++;
	}

	for (BlockNumber segno = 0; segno < reclaim_seg_limit; segno++)
	{
		if (segment_live != NULL && segment_live[segno])
			continue;
		if (!umfile_ctx_segment_exists(map_ctx, forknum, segno))
			continue;
		if (MapSegmentHasLiveReferences(map_ctx, rnode, forknum, segno))
			continue;

		(void) MapReclaimEnqueueSegment(map_ctx, rnode, forknum, segno);
	}

	if (sparse_count > 0 && moves_limit > 0)
	{
		BlockNumber	pass_page = InvalidBlockNumber;
		int			pass_slot = -1;

		for (page_idx = 0; page_idx < page_count; page_idx++)
		{
			BlockNumber	page_no = MapForkPageIndexToMapBlkno(forknum, page_idx);
			int			entry_idx;
			int			limit_idx;

			if (moved_here >= moves_limit)
				break;
			if (moves_done != NULL && *moves_done >= moves_limit)
				break;
			if (page_no >= n_map_pages)
				break;

			if (page_no != pass_page)
			{
				if (pass_slot >= 0)
					MapUnpinBuffer(pass_slot);
				pass_slot = MapReadBuffer(map_ctx, rnode, forknum, page_no);
				pass_page = page_no;
			}

			limit_idx = MAP_ENTRIES_PER_PAGE;
			if (page_idx == page_count - 1 && (n_lblknos % MAP_ENTRIES_PER_PAGE) != 0)
				limit_idx = n_lblknos % MAP_ENTRIES_PER_PAGE;
			for (entry_idx = 0; entry_idx < limit_idx; entry_idx++)
			{
				BlockNumber lblkno = page_idx * MAP_ENTRIES_PER_PAGE + entry_idx;
				MapPage	   *page;
				MapBufferDesc *buf;
				BlockNumber pblkno;
				BlockNumber extent_no;
				bool candidate = false;

				buf = &MapBuffers[pass_slot];
				page = MapGetPage(pass_slot);
				if (!LWLockConditionalAcquire(&buf->buffer_lock, LW_SHARED))
					continue;
				pblkno = page->pblknos[entry_idx];
				LWLockRelease(&buf->buffer_lock);

				if (pblkno == InvalidBlockNumber)
					continue;

				extent_no = pblkno / extent_blocks;
				if (max_extent_no != InvalidBlockNumber &&
					extent_no == max_extent_no)
					continue;
				if (!MapExtentFullyBelowReclaimBoundary(reclaim_boundary_pblk,
														extent_no,
														extent_blocks))
					continue;

				live_entry = (MapExtentLiveEntry *) hash_search(extent_live,
																&extent_no,
																HASH_FIND,
																NULL);
				if (live_entry != NULL)
				{
					int live_pct = (int) (((uint64) live_entry->live_blocks * 100) /
										  Max((uint64) 1, (uint64) extent_blocks));
					if (live_pct <= live_threshold)
						candidate = true;
				}

				if (!candidate)
					continue;

				if (MapCompactorRelocateEntry(map_ctx, rnode, forknum,
											  lblkno, pblkno))
				{
					moved_here++;
					if (moves_done != NULL)
						(*moves_done)++;

					live_entry = (MapExtentLiveEntry *) hash_search(extent_live,
																	&extent_no,
																	HASH_FIND,
																	NULL);
					if (live_entry != NULL && live_entry->live_blocks > 0)
					{
						live_entry->live_blocks--;
						if (live_entry->live_blocks == 0)
						{
							BlockNumber	start_blkno = extent_no * extent_blocks;
							BlockNumber	segno = start_blkno / ((BlockNumber) RELSEG_SIZE);

							if (!MapSegmentHasLiveReferences(map_ctx, rnode, forknum, segno))
								(void) MapReclaimEnqueue(map_ctx, rnode, forknum,
														 extent_no, extent_blocks);
						}
					}
				}
			}
		}

		if (pass_slot >= 0)
			MapUnpinBuffer(pass_slot);
	}

	if (boundary_committed != NULL)
	{
		advanced_boundary_pblk =
			MapCompactorAdvanceBoundaryFromSet(reclaim_boundary_pblk,
											   next_free_pblk,
											   boundary_committed);
		if (advanced_boundary_pblk > reclaim_boundary_pblk)
			MapSBlockAdvanceReclaimBoundary(map_ctx, rnode, forknum,
											advanced_boundary_pblk);
	}

	hash_destroy(extent_live);
	MemoryContextDelete(extent_ctx);
	return sparse_count;
}

int
MapCompactorStep(int max_relations)
{
	static int	scan_slot = 0;
	int			max_scan;
	int			scanned = 0;
	int			visited = 0;
	int			total_moves = 0;
	int			move_budget;

	if (!map_compactor_enable || InRecovery || max_relations <= 0 ||
		MapSuperCapacity <= 0)
		return 0;

	move_budget = Max(0, map_compactor_max_moves);

	max_scan = Min(MapSuperCapacity, Max(64, max_relations * 8));
	while (scanned < max_scan && visited < max_relations)
	{
		MapSuperEntry *entry;
		RelFileLocator	rnode;
		RelFileLocatorBackend rlocator;
		UmbraFileContext *ctx;
		bool		has_init_fork;
		bool		has_map_fork;
		entry = MapSuperEntryBySlot(scan_slot);
		scan_slot = (scan_slot + 1) % MapSuperCapacity;
		scanned++;

		if (!LWLockConditionalAcquire(&entry->lock, LW_SHARED))
			continue;
		if (!entry->in_use)
		{
			LWLockRelease(&entry->lock);
			continue;
		}
		rnode = entry->key.rnode;
		LWLockRelease(&entry->lock);
		visited++;

		rlocator.locator = rnode;
		rlocator.backend = INVALID_PROC_NUMBER;
		ctx = umfile_ctx_acquire(rlocator);
		if (ctx == NULL)
			continue;

		has_init_fork = umfile_ctx_fork_exists(ctx, INIT_FORKNUM,
											   UMFILE_EXISTS_DENSE);
		if (has_init_fork)
			continue;
		has_map_fork = umfile_ctx_fork_exists(ctx, UMBRA_METADATA_FORKNUM,
											  UMFILE_EXISTS_DENSE);
		if (!has_map_fork)
			continue;

		MapCompactorAnalyzeFork(ctx, rnode, MAIN_FORKNUM,
							   move_budget, &total_moves);
		MapCompactorAnalyzeFork(ctx, rnode, FSM_FORKNUM,
							   move_budget, &total_moves);
		MapCompactorAnalyzeFork(ctx, rnode, VISIBILITYMAP_FORKNUM,
							   move_budget, &total_moves);
	}

	/*
	 * Fresh relations usually occupy low-numbered super slots first. If a
	 * startup-time sweep observes only inactive slots, don't keep marching the
	 * scan cursor forward in large strides, or newly created relations can sit
	 * behind a full rotation before compactor ever visits them.
	 */
	if (visited == 0)
		scan_slot = 0;

	return total_moves;
}
