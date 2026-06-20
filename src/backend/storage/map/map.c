/*-------------------------------------------------------------------------
 *
 * map.c
 *	  physical map layer implementation
 *
 * This module owns MAP metadata layout helpers and shared cache/checkpoint
 * support for MAP pages.
 *
 * src/backend/storage/map/map.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "common/hashfn.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper.h"
#include "storage/mapsuper_internal.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/sync.h"
#include "storage/umfile.h"
#include "utils/memutils.h"

typedef struct MapTruncatePreloadState
{
	bool			active;
	RelFileLocator	rnode;
	ForkNumber		forknum;
	int				nslots;
	int				capacity;
	int			   *slots;
} MapTruncatePreloadState;

static MapTruncatePreloadState MapTruncatePreload[MAX_FORKNUM + 1];

#define MAP_PENDING_WAIT_RETRIES 10000
#define MAP_PENDING_WAIT_USEC 1000

typedef enum MapCachedLookupResult
{
	MAP_CACHED_LOOKUP_MISS,
	MAP_CACHED_LOOKUP_UNMAPPED,
	MAP_CACHED_LOOKUP_MAPPED
} MapCachedLookupResult;

/* Internal functions */
static bool MapTablespaceSelected(Oid spcOid, int ntablespaces,
								  const Oid *tablespace_ids);
static bool MapTruncateEntryRange(ForkNumber forknum, BlockNumber n_lblknos,
								  BlockNumber n_map_pages,
								  BlockNumber *start_map_page,
								  int *start_entry_idx,
								  BlockNumber *end_map_page,
								  int *end_entry_idx);

static void
MapTruncatePreloadResetEntry(MapTruncatePreloadState *state)
{
	int i;

	if (!state->active)
		return;

	for (i = 0; i < state->nslots; i++)
		MapUnpinBuffer(state->slots[i]);

	state->active = false;
	state->nslots = 0;
	state->forknum = InvalidForkNumber;
	memset(&state->rnode, 0, sizeof(state->rnode));
}

static MapTruncatePreloadState *
MapTruncatePreloadEntry(RelFileLocator rnode, ForkNumber forknum)
{
	MapTruncatePreloadState *state;

	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	state = &MapTruncatePreload[forknum];

	if (state->active &&
		(!RelFileLocatorEquals(state->rnode, rnode) ||
		 state->forknum != forknum))
		MapTruncatePreloadResetEntry(state);

	return state;
}

BlockNumber MapForkPageIndexToMapBlkno(ForkNumber forknum,
									   BlockNumber fork_page_idx);
BlockNumber MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno);
static bool MapDecodeMapBlkno(BlockNumber map_blkno, ForkNumber *forknum,
							  BlockNumber *fork_page_idx);
static bool MapMapPageWithinLogicalRange(UmbraFileContext *map_ctx,
										 RelFileLocator rnode,
										 ForkNumber forknum,
										 BlockNumber map_blkno);
bool MapForkPreallocSettings(ForkNumber forknum, BlockNumber *soft_low,
							 BlockNumber *hard_low,
							 BlockNumber *batch_blocks);
static MapCachedLookupResult MapTryLookupCachedEntry(RelFileLocator rnode,
													 ForkNumber forknum,
													 BlockNumber map_blkno,
													 int entry_idx,
													 bool adjust_usage,
													 BlockNumber *pblkno);
static MapCachedLookupResult MapTryLookupCachedPblknoInternal(RelFileLocator rnode,
															  ForkNumber forknum,
															  BlockNumber lblkno,
															  bool adjust_usage,
															  BlockNumber *pblkno);
static bool MapTryReserveFreshPblknoInternal(UmbraFileContext *map_ctx,
											 RelFileLocator rnode,
											 ForkNumber forknum,
											 BlockNumber lblkno,
											 BlockNumber *new_pblkno,
											 bool nowait);
static bool MapWaitForForeignInflightToClear(RelFileLocator rnode,
											 ForkNumber forknum,
											 BlockNumber lblkno);
bool MapReserveNextPblkno(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  ForkNumber forknum, BlockNumber lblkno,
						  BlockNumber *new_pblkno,
						  bool nowait);

void
MapResetAllTruncatePreloads(void)
{
	int slot_id;

	for (slot_id = 0; slot_id <= MAX_FORKNUM; slot_id++)
	{
		MapTruncatePreload[slot_id].active = false;
		MapTruncatePreload[slot_id].nslots = 0;
	}
}

bool
MapForkPreallocSettings(ForkNumber forknum, BlockNumber *soft_low,
						BlockNumber *hard_low, BlockNumber *batch_blocks)
{
	int			low;
	int			hard;
	int			batch;

	switch (forknum)
	{
		case MAIN_FORKNUM:
			low = map_prealloc_main_low;
			hard = map_prealloc_main_hard;
			batch = map_prealloc_main_batch;
			break;
		case FSM_FORKNUM:
			low = map_prealloc_fsm_low;
			hard = map_prealloc_fsm_hard;
			batch = map_prealloc_fsm_batch;
			break;
		case VISIBILITYMAP_FORKNUM:
			low = map_prealloc_vm_low;
			hard = map_prealloc_vm_hard;
			batch = map_prealloc_vm_batch;
			break;
		default:
			return false;
	}

	if (low <= 0 || batch <= 0)
		return false;
	if (hard <= 0)
		hard = 1;
	if (hard > low)
		hard = low;

	*soft_low = (BlockNumber) low;
	*hard_low = (BlockNumber) hard;
	*batch_blocks = (BlockNumber) batch;
	return true;
}

static bool
MapTruncateEntryRange(ForkNumber forknum, BlockNumber n_lblknos,
					  BlockNumber old_n_lblknos,
					  BlockNumber *start_map_page,
					  int *start_entry_idx,
					  BlockNumber *end_map_page,
					  int *end_entry_idx)
{
	BlockNumber	start_page_idx;
	BlockNumber	end_page_idx;

	(void) forknum;

	if (old_n_lblknos <= n_lblknos)
		return false;

	start_page_idx = n_lblknos / MAP_ENTRIES_PER_PAGE;
	end_page_idx = (old_n_lblknos - 1) / MAP_ENTRIES_PER_PAGE;

	*start_map_page = start_page_idx;
	*start_entry_idx = n_lblknos % MAP_ENTRIES_PER_PAGE;
	*end_map_page = end_page_idx;
	*end_entry_idx = (old_n_lblknos - 1) % MAP_ENTRIES_PER_PAGE;
	return true;
}

BlockNumber
MapForkPageIndexToMapBlkno(ForkNumber forknum, BlockNumber fork_page_idx)
{
	uint64 group_no;
	uint64 blkno64;

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "Umbra metadata fork should not call MapForkPageIndexToMapBlkno");

	switch (forknum)
	{
		case FSM_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
		{
			uint64 group_page_idx = (uint64) fork_page_idx;
			group_no = group_page_idx / (uint64) MAP_GROUP_MAIN_PAGES;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES +
				(uint64) MAP_GROUP_VM_PAGES +
				(group_page_idx % (uint64) MAP_GROUP_MAIN_PAGES);
			break;
		}

		default:
			elog(ERROR, "unsupported fork number %d in map lookup", (int) forknum);
			return 0;
	}

	if (blkno64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address map page %u for fork %d in MAP",
						fork_page_idx, forknum)));

	return (BlockNumber) blkno64;
}

/*
 * MapLblknoToMapBlkno - convert (forknum, lblkno) to linear MAP entry index.
 *
 * The metadata fork stores repeated proportional groups:
 * [FSM page][VM page][8192 MAIN pages].
 * Each fork page still maps MAP_ENTRIES_PER_PAGE logical blocks.
 */
BlockNumber
MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno)
{
	BlockNumber	fork_page_idx;
	uint64		entry64;

	fork_page_idx = lblkno / MAP_ENTRIES_PER_PAGE;
	entry64 = (uint64) MapForkPageIndexToMapBlkno(forknum, fork_page_idx) *
		(uint64) MAP_ENTRIES_PER_PAGE +
		(uint64) (lblkno % MAP_ENTRIES_PER_PAGE);

	if (entry64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address logical block %u for fork %d in MAP",
						lblkno, forknum)));

	return (BlockNumber) entry64;
}

static bool
MapDecodeMapBlkno(BlockNumber map_blkno, ForkNumber *forknum,
				  BlockNumber *fork_page_idx)
{
	uint64 offset;
	uint64 group_no;
	uint64 in_group;

	if (map_blkno == MAP_BLOCK_SUPER || map_blkno < MAP_BLOCK_FIRST_GROUP)
		return false;

	offset = (uint64) (map_blkno - MAP_BLOCK_FIRST_GROUP);
	group_no = offset / (uint64) MAP_GROUP_TOTAL_PAGES;
	in_group = offset % (uint64) MAP_GROUP_TOTAL_PAGES;

	if (in_group < (uint64) MAP_GROUP_FSM_PAGES)
	{
		*forknum = FSM_FORKNUM;
		*fork_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) MAP_GROUP_FSM_PAGES;
	if (in_group < (uint64) MAP_GROUP_VM_PAGES)
	{
		*forknum = VISIBILITYMAP_FORKNUM;
		*fork_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) MAP_GROUP_VM_PAGES;
	if (in_group < (uint64) MAP_GROUP_MAIN_PAGES)
	{
		*forknum = MAIN_FORKNUM;
		*fork_page_idx = (BlockNumber)
			(group_no * (uint64) MAP_GROUP_MAIN_PAGES + in_group);
		return true;
	}

	return false;
}

/*
 * MapMapPageWithinLogicalRange - whether a MAP page intersects current logical
 * mapping domain of the target fork.
 *
 * This check is superblock-driven and keeps pages outside logical range from
 * being interpreted as real MAP pages.
 */
static bool
MapMapPageWithinLogicalRange(UmbraFileContext *map_ctx, RelFileLocator rnode,
							 ForkNumber forknum, BlockNumber map_blkno)
{
	BlockNumber n_lblknos;
	ForkNumber	page_forknum;
	BlockNumber	page_idx;
	uint64		page_first_lblk;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos))
		return true;

	if (!MapDecodeMapBlkno(map_blkno, &page_forknum, &page_idx))
		return false;

	if (page_forknum != forknum)
		return false;

	page_first_lblk = (uint64) page_idx * (uint64) MAP_ENTRIES_PER_PAGE;
	if (page_first_lblk >= (uint64) n_lblknos)
		return false;

	return true;
}

/*
 * MapTryLookupCachedEntry - read a cached MAP entry without performing I/O.
 *
 * The caller supplies the decoded MAP page and entry index so cache hit
 * handling stays in one place.  Result distinguishes between:
 * - cache miss / stale slot
 * - cached page with an unmapped entry
 * - cached page with a valid mapping
 */
static MapCachedLookupResult
MapTryLookupCachedEntry(RelFileLocator rnode, ForkNumber forknum,
						  BlockNumber map_blkno, int entry_idx,
						  bool adjust_usage, BlockNumber *pblkno)
{
	int				slot_id;
	MapBufferDesc   *buf;
	MapPage		   *page;
	BlockNumber		value;

	slot_id = MapCacheLookup(rnode, forknum, map_blkno);
	if (slot_id < 0)
		return MAP_CACHED_LOOKUP_MISS;

	buf = &MapBuffers[slot_id];
	MapPinBuffer(slot_id, adjust_usage);
	LWLockAcquire(&buf->buffer_lock, LW_SHARED);

	if (buf->page_number != map_blkno ||
		buf->page_number < 0 ||
		!RelFileLocatorEquals(buf->rnode, rnode) ||
		buf->forknum != forknum)
	{
		LWLockRelease(&buf->buffer_lock);
		MapUnpinBuffer(slot_id);
		return MAP_CACHED_LOOKUP_MISS;
	}

	page = MapGetPage(slot_id);
	value = page->pblknos[entry_idx];
	LWLockRelease(&buf->buffer_lock);
	MapUnpinBuffer(slot_id);

	if (value == InvalidBlockNumber)
		return MAP_CACHED_LOOKUP_UNMAPPED;

	*pblkno = value;
	return MAP_CACHED_LOOKUP_MAPPED;
}

static MapCachedLookupResult
MapTryLookupCachedPblknoInternal(RelFileLocator rnode, ForkNumber forknum,
								   BlockNumber lblkno, bool adjust_usage,
								   BlockNumber *pblkno)
{
	BlockNumber		map_blkno;
	int				entry_idx;

	Assert(pblkno != NULL);

	if (forknum == UMBRA_METADATA_FORKNUM)
		return MAP_CACHED_LOOKUP_MISS;

	map_blkno = MapLblknoToMapBlkno(forknum, lblkno);
	entry_idx = map_blkno % MAP_ENTRIES_PER_PAGE;
	map_blkno = map_blkno / MAP_ENTRIES_PER_PAGE;

	return MapTryLookupCachedEntry(rnode, forknum, map_blkno, entry_idx,
								   adjust_usage, pblkno);
}

/*
 * MapTryLookup - try to find physical block number for a logical block.
 *
 * Returns true and sets *pblkno when a valid mapping exists.
 * Returns false if MAP fork is absent or the entry is still unmapped.
 */
bool
MapTryLookup(UmbraFileContext *map_ctx, RelFileLocator rnode, ForkNumber forknum,
			 BlockNumber lblkno, BlockNumber *pblkno)
{
	BlockNumber	map_blkno;
	int			slot_id;
	uint32_t	state;
	MapPage    *page;
	MapBufferDesc *buf;
	int			entry_idx;
	MapCachedLookupResult cache_result;

	Assert(pblkno != NULL);

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "MapTryLookup does not accept Umbra metadata fork");

	cache_result = MapTryLookupCachedPblknoInternal(rnode, forknum, lblkno,
													 true, pblkno);
	if (cache_result != MAP_CACHED_LOOKUP_MISS)
		return cache_result == MAP_CACHED_LOOKUP_MAPPED;

	/* Convert (forknum, lblkno) to MAP page and entry index */
	map_blkno = MapLblknoToMapBlkno(forknum, lblkno);
	entry_idx = map_blkno % MAP_ENTRIES_PER_PAGE;
	map_blkno = map_blkno / MAP_ENTRIES_PER_PAGE;

	/* Find or load the map page - returns with buffer pinned */
	slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);

	/* Verify buffer is pinned */
	state = pg_atomic_read_u32(&buf->state);
	if (!(state & MAPBUF_VALID_MASK))
		elog(ERROR, "map buffer not pinned");

	LWLockAcquire(&buf->buffer_lock, LW_SHARED);
	*pblkno = page->pblknos[entry_idx];
	LWLockRelease(&buf->buffer_lock);
	MapUnpinBuffer(slot_id);

	return (*pblkno != InvalidBlockNumber);
}

/*
 * MapTryLookupPblkRun - find the longest contiguous mapped pblk run.
 *
 * Returns the number of blocks in the run beginning at lblkno, up to
 * maxblocks. Returns 0 if the first entry is unmapped.
 *
 * This batches translation by MAP page, so callers don't need a full
 * MapTryLookup() round trip for every block in a contiguous run.
 */
BlockNumber
MapTryLookupPblkRun(UmbraFileContext *map_ctx, RelFileLocator rnode,
					ForkNumber forknum, BlockNumber lblkno,
					BlockNumber maxblocks, BlockNumber *start_pblkno)
{
	BlockNumber current_lblk = lblkno;
	BlockNumber remaining = maxblocks;
	BlockNumber run_blocks = 0;
	BlockNumber expected_next_pblk = InvalidBlockNumber;
	BlockNumber current_map_blkno = InvalidBlockNumber;
	int			current_slot = -1;

	Assert(start_pblkno != NULL);
	Assert(maxblocks > 0);

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "MapTryLookupPblkRun does not accept Umbra metadata fork");

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return 0;

	while (remaining > 0)
	{
		BlockNumber	map_entry_no;
		BlockNumber	map_blkno;
		int			entry_idx;
		int			entries_this_page;
		MapBufferDesc *buf;
		MapPage	   *page;

		map_entry_no = MapLblknoToMapBlkno(forknum, current_lblk);
		entry_idx = map_entry_no % MAP_ENTRIES_PER_PAGE;
		map_blkno = map_entry_no / MAP_ENTRIES_PER_PAGE;
		entries_this_page = Min((BlockNumber) (MAP_ENTRIES_PER_PAGE - entry_idx),
								remaining);

		if (current_slot < 0 || current_map_blkno != map_blkno)
		{
			if (current_slot >= 0)
				MapUnpinBuffer(current_slot);
			current_slot = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
			current_map_blkno = map_blkno;
		}

		buf = &MapBuffers[current_slot];
		page = MapGetPage(current_slot);

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		for (int i = 0; i < entries_this_page; i++)
		{
			BlockNumber	pblkno = page->pblknos[entry_idx + i];

			if (pblkno == InvalidBlockNumber)
			{
				LWLockRelease(&buf->buffer_lock);
				goto done;
			}

			if (run_blocks == 0)
			{
				*start_pblkno = pblkno;
				expected_next_pblk = pblkno + 1;
				run_blocks = 1;
				current_lblk++;
				remaining--;
				continue;
			}

			if (pblkno != expected_next_pblk)
			{
				LWLockRelease(&buf->buffer_lock);
				goto done;
			}

			if (((*start_pblkno % ((BlockNumber) RELSEG_SIZE)) + run_blocks) >=
				((BlockNumber) RELSEG_SIZE))
			{
				LWLockRelease(&buf->buffer_lock);
				goto done;
			}

			expected_next_pblk++;
			run_blocks++;
			current_lblk++;
			remaining--;
		}
		LWLockRelease(&buf->buffer_lock);
	}

done:
	if (current_slot >= 0)
		MapUnpinBuffer(current_slot);

	return run_blocks;
}

/*
 * Reserve a brand-new in-flight physical target for lblkno.
 *
 * This helper does not consult existing in-flight state before consuming the
 * next frontier block. If another backend already published an in-flight
 * target for the same lblkno, publication will fail and the freshly reserved
 * frontier slot becomes a hole. Callers that want a stable winner should
 * fall back to a lookup after a false return.
 */
bool
MapTryReserveFreshPblkno(UmbraFileContext *map_ctx, RelFileLocator rnode,
						 ForkNumber forknum, BlockNumber lblkno,
						 BlockNumber *new_pblkno, bool nowait)
{
	return MapTryReserveFreshPblknoInternal(map_ctx, rnode, forknum, lblkno,
											new_pblkno, nowait);
}

static bool
MapTryReserveFreshPblknoInternal(UmbraFileContext *map_ctx, RelFileLocator rnode,
								 ForkNumber forknum, BlockNumber lblkno,
								 BlockNumber *new_pblkno, bool nowait)
{
	MapSuperEntry *entry;
	BlockNumber		next;
	uint32			flags;
	bool			reserved = false;

	Assert(new_pblkno != NULL);

	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapSBlockEnsureLoaded(map_ctx, rnode))
		return false;

	if (!MapInflightTryClaim(map_ctx, rnode, forknum, lblkno))
		return false;

	PG_TRY();
	{
		if (nowait)
		{
			if (!MapSuperFindEntryTryLocked(rnode, LW_EXCLUSIVE, &entry))
				goto reserve_done;
		}
		else
		{
			if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
				goto reserve_done;
		}

		flags = entry->flags;
		if ((flags & MAPSUPER_FLAG_CORRUPT) ||
			!MapSuperblockHasValidIdentity(&entry->super) ||
			((flags & MAPSUPER_FLAG_DIRTY) == 0 &&
			 !MapSuperblockCheckCRC(&entry->super)))
		{
			LWLockRelease(&entry->lock);
			if (!InRecovery)
				MapSBlockReportCorrupt(rnode, "invalid identity or CRC");
			goto reserve_done;
		}

		Assert(MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetNextFreePhysBlock(&entry->super,
																			forknum)) <=
			   MapSuperGetReservedNextFree(entry, forknum));
		next = MapSuperGetReservedNextFree(entry, forknum);
		if (next == InvalidBlockNumber - 1)
		{
			LWLockRelease(&entry->lock);
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cannot allocate more physical blocks for relation %u/%u/%u fork %d",
							rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum)));
		}

			MapSuperSetReservedNextFree(entry, forknum, next + 1);
		Assert(MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetNextFreePhysBlock(&entry->super,
																			forknum)) <=
			   MapSuperGetReservedNextFree(entry, forknum));

		*new_pblkno = next;
		LWLockRelease(&entry->lock);
		MapInflightFinishClaim(rnode, forknum, lblkno, next);
		reserved = true;

reserve_done:
		;
	}
	PG_CATCH();
	{
		MapInflightRelease(rnode, forknum, lblkno);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!reserved)
	{
		MapInflightRelease(rnode, forknum, lblkno);
		return false;
	}

	return true;
}

static bool
MapWaitForForeignInflightToClear(RelFileLocator rnode,
								 ForkNumber forknum,
								 BlockNumber lblkno)
{
	int			wait_retries = 0;

	for (;;)
	{
		if (!MapInflightBitIsSet(rnode, forknum, lblkno))
		{
			if (wait_retries > 0)
				elog(LOG,
					 "foreground remap waited for in-flight remap on relation %u/%u/%u fork %d block %u (%d retries, %d usec)",
					 rnode.spcOid, rnode.dbOid, rnode.relNumber,
					 forknum, lblkno,
					 wait_retries,
					 wait_retries * MAP_PENDING_WAIT_USEC);
			return wait_retries > 0;
		}

		CHECK_FOR_INTERRUPTS();
		pg_usleep(MAP_PENDING_WAIT_USEC);
		wait_retries++;

		if (wait_retries >= MAP_PENDING_WAIT_RETRIES)
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("timed out waiting for in-flight remap of relation %u/%u/%u fork %d block %u",
							rnode.spcOid, rnode.dbOid, rnode.relNumber,
							forknum, lblkno)));
	}
}

/*
 * MapReserveNextPblkno - return a locally owned in-flight pblk for lblkno,
 * creating it if needed.
 *
 * The shared MAP bit remains visible to other backends so they can observe
 * contention, but the selected pblk stays owner-local and must not be
 * borrowed by foreign callers. Foreign collisions are surfaced as false so
 * callers can fail or fallback according to their ownership rules.
 */
bool
MapReserveNextPblkno(UmbraFileContext *map_ctx, RelFileLocator rnode,
					 ForkNumber forknum, BlockNumber lblkno,
					 BlockNumber *new_pblkno, bool nowait)
{
	if (MapInflightLookupOwnedPblk(rnode, forknum, lblkno, new_pblkno))
		return true;

	if (MapTryReserveFreshPblknoInternal(map_ctx, rnode, forknum, lblkno,
										 new_pblkno, nowait))
		return true;

	return MapInflightLookupOwnedPblk(rnode, forknum, lblkno, new_pblkno);
}

/*
 * Reserve a fresh physical target without consulting or updating any current
 * mapping entry.
 *
 * Callers use this when they already own first publication for lblkno and
 * must not reuse a locally visible old mapping.
 */
bool
MapReserveFreshPblkno(UmbraFileContext *map_ctx, RelFileLocator rnode,
					  ForkNumber forknum, BlockNumber lblkno,
					  BlockNumber *new_pblkno)
{
	if (MapReserveNextPblkno(map_ctx, rnode, forknum, lblkno,
							 new_pblkno, false))
	{
		if (!InRecovery)
			(void) MapMaybePreallocateFork(map_ctx, rnode, forknum, false);
		return true;
	}

	/*
	 * "Fresh" callers are first-born owners. Seeing a foreign in-flight owner
	 * here means the caller's ownership assumption was wrong, so surface the
	 * conflict immediately and let the caller decide whether this is an
	 * invariant violation or a fast-path fallback.
	 */
	return false;
}


/*
 * MapReadBuffer - read a map page into buffer
 *
 * Returns the slot_id of the buffer, with the buffer pinned.
 *
 * This function is extern because direct mapping publication needs to load
 * and update MAP pages from outside map.c.
 */
int
MapReadBuffer(UmbraFileContext *map_ctx, RelFileLocator rnode,
			  ForkNumber forknum, BlockNumber map_blkno)
{
	int			slot_id;
	uint32_t	state;
	MapPage    *page;
	MapBufferDesc *buf;
	BlockNumber map_nblocks;
	int			old_page_number;
	ForkNumber	old_forknum;
	RelFileLocator old_rnode;

	if (map_blkno == MAP_BLOCK_SUPER)
		elog(ERROR, "MapReadBuffer cannot be used for MAP superblock");

	for (;;)
	{
		int			existing_slot_id;
		bool		retry = false;

		slot_id = MapCacheLookup(rnode, forknum, map_blkno);
		if (slot_id >= 0)
		{
			buf = &MapBuffers[slot_id];

			MapPinBuffer(slot_id, true);
			LWLockAcquire(&buf->buffer_lock, LW_SHARED);

			if (buf->page_number == map_blkno &&
				buf->page_number >= 0 &&
				RelFileLocatorEquals(buf->rnode, rnode) &&
				buf->forknum == forknum)
			{
				LWLockRelease(&buf->buffer_lock);
				return slot_id;
			}

			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		slot_id = MapClockGetBuffer();
		buf = &MapBuffers[slot_id];
		MapPinBuffer(slot_id, false);

		LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);

		if (buf->page_number == map_blkno &&
			buf->page_number >= 0 &&
			RelFileLocatorEquals(buf->rnode, rnode) &&
			buf->forknum == forknum)
		{
			LWLockRelease(&buf->buffer_lock);
			return slot_id;
		}

		state = pg_atomic_read_u32(&buf->state);
		if (MAPBUF_GET_REFCOUNT(state) != 1)
		{
			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		if (state & MAPBUF_DIRTY)
		{
			LWLockRelease(&buf->buffer_lock);
			MapFlushBuffer(slot_id);

			LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
			if (buf->page_number == map_blkno &&
				buf->page_number >= 0 &&
				RelFileLocatorEquals(buf->rnode, rnode) &&
				buf->forknum == forknum)
			{
				LWLockRelease(&buf->buffer_lock);
				return slot_id;
			}

			state = pg_atomic_read_u32(&buf->state);
			if (MAPBUF_GET_REFCOUNT(state) != 1 ||
				(state & MAPBUF_DIRTY))
			{
				LWLockRelease(&buf->buffer_lock);
				MapUnpinBuffer(slot_id);
				continue;
			}
		}

		if (buf->pending_count != 0)
		{
			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		old_page_number = buf->page_number;
		old_forknum = buf->forknum;
		old_rnode = buf->rnode;
		MemSet(buf->pending_bits, 0, sizeof(buf->pending_bits));

		existing_slot_id = MapCacheInsert(rnode, forknum, map_blkno, slot_id);
		if (existing_slot_id >= 0 && existing_slot_id != slot_id)
			retry = true;
		if (retry)
		{
			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		if (old_page_number >= 0)
			MapCacheDelete(old_rnode, old_forknum,
						   (BlockNumber) old_page_number, slot_id);

		buf->page_number = map_blkno;
		buf->rnode = rnode;
		buf->forknum = forknum;
		buf->page_lsn = 0;
		MapBufferUpdateStateBits(buf, MAPBUF_USAGECOUNT_ONE, 0);

		page = MapGetPage(slot_id);
		if (umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		{
			map_nblocks = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM);
			if (map_blkno < map_nblocks &&
				MapMapPageWithinLogicalRange(map_ctx, rnode, forknum, map_blkno))
			{
				umfile_ctx_read(map_ctx, UMBRA_METADATA_FORKNUM, map_blkno,
								(char *) page, BLCKSZ);
				MapBufferUpdateStateBits(buf, 0, MAPBUF_NOT_MATERIALIZED);

				if (pg_memory_is_all_zeros(page, BLCKSZ))
				{
					BlockNumber	n_lblknos = 0;
					ForkNumber	page_forknum;
					BlockNumber	page_idx;
					bool		need_this_page = false;

					if (MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum,
													 &n_lblknos) &&
						n_lblknos > 0 &&
						MapDecodeMapBlkno(map_blkno, &page_forknum, &page_idx) &&
						page_forknum == forknum)
					{
						uint64 page_first_lblk =
							(uint64) page_idx * (uint64) MAP_ENTRIES_PER_PAGE;

						need_this_page = page_first_lblk < (uint64) n_lblknos;
					}

					if (need_this_page)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("MAP page %u is all-zeros for relation %u/%u/%u fork %d",
										map_blkno, rnode.spcOid, rnode.dbOid,
										rnode.relNumber, forknum)));

					MemSet(page, 0xFF, BLCKSZ);
				}
			}
			else
			{
				MemSet(page, 0xFF, BLCKSZ);
				if (map_blkno >= map_nblocks)
					MapBufferUpdateStateBits(buf, MAPBUF_NOT_MATERIALIZED, 0);
				else
					MapBufferUpdateStateBits(buf, 0, MAPBUF_NOT_MATERIALIZED);
			}
		}
		else
		{
			MemSet(page, 0xFF, BLCKSZ);
			MapBufferUpdateStateBits(buf, MAPBUF_NOT_MATERIALIZED, 0);
		}

		LWLockRelease(&buf->buffer_lock);
		return slot_id;
	}
}

/*
 * MapDrop - drop mapping for a relation
 */
void
MapDrop(RelFileLocator rnode)
{
	RelFileLocatorBackend rnode_backend;

	rnode_backend.locator = rnode;
	rnode_backend.backend = INVALID_PROC_NUMBER;

	MapInvalidateRelation(rnode);
	umfile_ctx_unlinkfork(rnode_backend, UMBRA_METADATA_FORKNUM, false);
}

/*
 * MapTruncate - truncate mapping when relation is truncated
 */
void
MapTruncate(UmbraFileContext *map_ctx, RelFileLocator rnode,
			ForkNumber forknum, BlockNumber n_lblknos,
			XLogRecPtr map_lsn)
{
	BlockNumber old_n_lblknos = 0;
	BlockNumber end_page_idx;
	BlockNumber start_page_idx;
	int         start_entry_idx;
	int         end_entry_idx;
	BlockNumber page_idx;

	if (forknum == UMBRA_METADATA_FORKNUM)
		return;

	Assert(map_ctx != NULL);
	Assert(map_lsn != InvalidXLogRecPtr);
	if (map_lsn == InvalidXLogRecPtr)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid truncate WAL LSN for relation %u/%u/%u fork %d",
						rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum),
				 errdetail("truncate target logical block count: %u", n_lblknos)));

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
	{
		Assert(false);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("required MAP fork is missing during truncate for relation %u/%u/%u fork %d",
						rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum),
				 errdetail("truncate target logical block count: %u", n_lblknos)));
	}

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &old_n_lblknos))
		return;

	if (!MapTruncateEntryRange(forknum, n_lblknos, old_n_lblknos,
							   &start_page_idx, &start_entry_idx,
							   &end_page_idx, &end_entry_idx))
		return;

	for (page_idx = start_page_idx; page_idx <= end_page_idx; page_idx++)
	{
		int          slot_id;
		int          begin_idx;
		int          last_idx;
		Size         clear_bytes;
		BlockNumber  map_blkno;
		MapPage     *page;
		MapBufferDesc *buf;

		map_blkno = MapForkPageIndexToMapBlkno(forknum, page_idx);
		if (map_blkno >= umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM))
			break;

		slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
		buf = &MapBuffers[slot_id];
		page = MapGetPage(slot_id);

		begin_idx = (page_idx == start_page_idx) ? start_entry_idx : 0;
		last_idx = (page_idx == end_page_idx) ? end_entry_idx : (MAP_ENTRIES_PER_PAGE - 1);

		LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
		if (begin_idx == 0 && last_idx == (MAP_ENTRIES_PER_PAGE - 1))
		{
			/* Fast path: the whole map page range is invalidated. */
			MemSet(page->pblknos, 0xFF, MAP_ENTRIES_PER_PAGE * sizeof(uint32));
		}
		else
		{
			/* Boundary pages: invalidate only the requested subrange. */
			clear_bytes = ((Size) (last_idx - begin_idx + 1)) * sizeof(uint32);
			MemSet(&page->pblknos[begin_idx], 0xFF, clear_bytes);
		}

		/* Associate truncate-driven map rewrite with truncate WAL LSN. */
		MapMarkBufferDirty(map_ctx, buf, map_lsn);

		LWLockRelease(&buf->buffer_lock);

		MapUnpinBuffer(slot_id);
	}

	/*
	 * Keep dirty map pages in cache and let checkpoint/bgwriter flush them.
	 * Invalidating relation slots here would clear dirty state before writeback.
	 */
}

void
MapPreloadTruncatePages(UmbraFileContext *map_ctx, RelFileLocator rnode,
						ForkNumber forknum, BlockNumber n_lblknos)
{
	MapTruncatePreloadState *state;
	BlockNumber old_n_lblknos = 0;
	BlockNumber start_page_idx;
	BlockNumber end_page_idx;
	int start_entry_idx;
	int end_entry_idx;
	BlockNumber page_idx;

	if (forknum == UMBRA_METADATA_FORKNUM ||
		!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &old_n_lblknos))
		return;

	if (!MapTruncateEntryRange(forknum, n_lblknos, old_n_lblknos,
							   &start_page_idx, &start_entry_idx,
							   &end_page_idx, &end_entry_idx))
		return;

	state = MapTruncatePreloadEntry(rnode, forknum);
	MapTruncatePreloadResetEntry(state);

	state->active = true;
	state->rnode = rnode;
	state->forknum = forknum;

	for (page_idx = start_page_idx; page_idx <= end_page_idx; page_idx++)
	{
		int slot_id;
		BlockNumber map_blkno;

		if (state->nslots == state->capacity)
		{
			int newcap = state->capacity == 0 ? 4 : state->capacity * 2;

			if (state->slots == NULL)
				state->slots = MemoryContextAlloc(TopMemoryContext,
												 sizeof(int) * newcap);
			else
				state->slots = repalloc(state->slots, sizeof(int) * newcap);
			state->capacity = newcap;
		}

		map_blkno = MapForkPageIndexToMapBlkno(forknum, page_idx);
		if (map_blkno >= umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM))
			break;

		slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
		state->slots[state->nslots++] = slot_id;
	}
}

void
MapReleasePreloadedTruncatePages(RelFileLocator rnode, ForkNumber forknum)
{
	MapTruncatePreloadState *state;

	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	state = &MapTruncatePreload[forknum];

	if (!state->active)
		return;

	if (!RelFileLocatorEquals(state->rnode, rnode) || state->forknum != forknum)
		return;

	MapTruncatePreloadResetEntry(state);
}

/*
 * MapInvalidateRelation - invalidate all map cache entries for one relation.
 */
void
MapInvalidateRelation(RelFileLocator rnode)
{
	int			slot_id;

	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		MapBufferDesc *buf = &MapBuffers[slot_id];
		int			page_number;
		ForkNumber	forknum;
		RelFileLocator slot_rnode;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		page_number = buf->page_number;
		forknum = buf->forknum;
		slot_rnode = buf->rnode;
		LWLockRelease(&buf->buffer_lock);

		if (page_number < 0 || !RelFileLocatorEquals(slot_rnode, rnode))
			continue;

		MapCacheDelete(slot_rnode, forknum, (BlockNumber) page_number, slot_id);
		MapInvalidateBuffer(slot_id, slot_rnode, forknum,
							(BlockNumber) page_number);
	}

	/* Remove dedicated superblock cache entry for this relation. */
	MapSuperDeleteEntry(rnode);

	}

static bool
MapTablespaceSelected(Oid spcOid, int ntablespaces, const Oid *tablespace_ids)
{
	int			i;

	if (ntablespaces <= 0 || tablespace_ids == NULL)
		return true;

	for (i = 0; i < ntablespaces; i++)
	{
		if (tablespace_ids[i] == spcOid)
			return true;
	}

	return false;
}

/*
 * MapInvalidateDatabaseTablespaces - invalidate MAP metadata/cache for a DB.
 *
 * If ntablespaces<=0, invalidate all tablespaces of that DB.
 * If ntablespaces>0, only invalidate entries whose spcOid is in the list.
 *
 * This is needed because database OIDs and relfilenodes can be reused after
 * DROP/CREATE churn. Without DB-scope invalidation, stale MAP buffer/cache/
 * super entries can survive and be incorrectly reused by relations in the
 * recreated DB.
 */
void
MapInvalidateDatabaseTablespaces(Oid dbid, int ntablespaces,
								 const Oid *tablespace_ids)
{
	int			slot_id;

	/* Invalidate per-buffer cached pages */
	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		MapBufferDesc *buf = &MapBuffers[slot_id];
		int			page_number;
		ForkNumber	forknum;
		RelFileLocator slot_rnode;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		page_number = buf->page_number;
		forknum = buf->forknum;
		slot_rnode = buf->rnode;
		LWLockRelease(&buf->buffer_lock);

		if (page_number < 0 ||
			slot_rnode.dbOid != dbid ||
			!MapTablespaceSelected(slot_rnode.spcOid, ntablespaces, tablespace_ids))
			continue;

		MapCacheDelete(slot_rnode, forknum, (BlockNumber) page_number, slot_id);
		MapInvalidateBuffer(slot_id, slot_rnode, forknum,
							(BlockNumber) page_number);
	}

	/* Invalidate dedicated superblock cache entries for matching relations */
	{
		RelFileLocator *targets;
		int			target_cap = 256;
		int			target_count = 0;
		int			i;

		targets = palloc(sizeof(RelFileLocator) * target_cap);

		for (slot_id = 0; slot_id < MapSuperCapacity; slot_id++)
		{
			MapSuperEntry *entry = MapSuperEntryBySlot(slot_id);
			RelFileLocator rnode;

			LWLockAcquire(&entry->lock, LW_SHARED);
			if (!entry->in_use ||
				entry->key.rnode.dbOid != dbid ||
				!MapTablespaceSelected(entry->key.rnode.spcOid, ntablespaces,
									   tablespace_ids))
			{
				LWLockRelease(&entry->lock);
				continue;
			}

			rnode = entry->key.rnode;
			LWLockRelease(&entry->lock);

			if (target_count >= target_cap)
			{
				target_cap *= 2;
				targets = repalloc(targets, sizeof(RelFileLocator) * target_cap);
			}
			targets[target_count++] = rnode;
		}

			for (i = 0; i < target_count; i++)
				MapSuperDeleteEntry(targets[i]);

			pfree(targets);
	}
}

/*
 * MapInvalidateDatabase - invalidate all MAP metadata/cache for one database.
 */
void
MapInvalidateDatabase(Oid dbid)
{
	MapInvalidateDatabaseTablespaces(dbid, 0, NULL);
}

/*
 * MapGetLogicalBlockCount - get logical block count from the MAP superblock
 */
BlockNumber
MapGetLogicalBlockCount(UmbraFileContext *map_ctx, RelFileLocator rnode, ForkNumber forknum)
{
	BlockNumber n_lblknos = 0;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos))
		return 0;

	return n_lblknos;
}

/*
 * MapGetPhysicalBlockCount - physical block count needed for first n lblknos
 *
 * Returns max(mapped pblkno in [0, n_lblknos)) + 1.
 * This is used by truncate to avoid cutting off still-referenced physical
 * blocks when logical->physical mapping is non-identity.
 */
BlockNumber
MapGetPhysicalBlockCount(UmbraFileContext *map_ctx, RelFileLocator rnode,
						 ForkNumber forknum, BlockNumber n_lblknos)
{
	BlockNumber n_map_pages;
	BlockNumber current_page = InvalidBlockNumber;
	BlockNumber page_idx;
	BlockNumber page_count;
	BlockNumber max_pblkno = InvalidBlockNumber;
	int         current_slot = -1;

	if (n_lblknos == 0)
		return 0;

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return n_lblknos;

	n_map_pages = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM);
	if (n_map_pages == 0)
		return 0;
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
		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		limit_idx = MAP_ENTRIES_PER_PAGE;
		if (page_idx == page_count - 1 && (n_lblknos % MAP_ENTRIES_PER_PAGE) != 0)
			limit_idx = n_lblknos % MAP_ENTRIES_PER_PAGE;
		for (entry_idx = 0; entry_idx < limit_idx; entry_idx++)
		{
			BlockNumber pblkno = page->pblknos[entry_idx];

			if (pblkno == InvalidBlockNumber)
				continue;

			if (max_pblkno == InvalidBlockNumber || pblkno > max_pblkno)
				max_pblkno = pblkno;
		}
		LWLockRelease(&buf->buffer_lock);
	}

	if (current_slot >= 0)
		MapUnpinBuffer(current_slot);

	if (max_pblkno == InvalidBlockNumber)
		return 0;
	if (max_pblkno == InvalidBlockNumber - 1)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot represent physical block count beyond %u",
						InvalidBlockNumber - 1)));

	return max_pblkno + 1;
}

/*
 * MapGetNewPblkno - allocate new physical block and return both old and new
 *
 * This queries the current mapping, reserves a new physical block, and returns
 * both the old and new physical block numbers to the caller that owns the
 * mapping transition.
 *
 * Parameters:
 *   map_ctx: MAP fork context for file I/O operations
 *   rnode: relation identifier
 *   forknum: fork number
 *   lblkno: logical block number
 *   new_pblkno: output parameter for the new physical block number
 *   old_pblkno: output parameter for the old physical block number
 */
void MapGetNewPbkno(UmbraFileContext *map_ctx, RelFileLocator rnode, ForkNumber forknum,
				BlockNumber lblkno, BlockNumber *new_pblkno,
				BlockNumber *old_pblkno)
{
	BlockNumber cur_pblkno;

	Assert(new_pblkno != NULL);
	Assert(old_pblkno != NULL);

	/*
	 * During recovery, MAIN fork physical choices must come from WAL records.
	 * FSM/VM are hint forks and replay can touch them without dedicated remap
	 * metadata (e.g. free space updates), so we allow local allocation for
	 * them.
	 */
	if (InRecovery && forknum == MAIN_FORKNUM)
		elog(PANIC,
			 "MapGetNewPbkno called during recovery for rel %u/%u/%u fork %d blk %u",
			 rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum, lblkno);

	for (;;)
	{
		if (!MapTryLookup(map_ctx, rnode, forknum, lblkno, &cur_pblkno))
		{
			*old_pblkno = InvalidBlockNumber;
		}
		else
		{
			*old_pblkno = cur_pblkno;
		}

		if (MapReserveNextPblkno(map_ctx, rnode, forknum, lblkno,
								 new_pblkno, false))
			break;

		/*
		 * A foreign in-flight owner controls the current mapping publication
		 * decision. Wait for it to publish, then retry against committed MAP
		 * state instead of guessing from buffers or page LSNs.
		 */
		if (!MapWaitForForeignInflightToClear(rnode, forknum, lblkno))
			elog(ERROR,
				 "failed to reserve physical block for relation %u/%u/%u fork %d blk %u",
				 rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum, lblkno);
	}

	/*
	 * Frontend/backgound coordination:
	 * - low-but-not-critical watermark: wake mapwriter
	 * - critical watermark: foreground performs one-shot preallocation
	 */
	if (!InRecovery)
		(void) MapMaybePreallocateFork(map_ctx, rnode, forknum, false);
}

/*
 * MapSetMapping - set a mapping entry directly
 *
 * This function sets a mapping entry directly without allocating a new
 * physical block. Callers must already own the mapping publication decision.
 *
 * Parameters:
 *   map_ctx: MAP fork context for file I/O operations
 *   rnode: relation identifier
 *   forknum: fork number
 *   lblkno: logical block number
 *   new_pblkno: physical block number to set
 *
 * This function does NOT allocate a new physical block and does NOT advance
 * superblock frontier/watermark state. Callers that own a fresh physical
 * allocation must publish frontier changes explicitly.
 */
void
MapSetMapping(UmbraFileContext *map_ctx, RelFileLocator rnode, ForkNumber forknum,
			  BlockNumber lblkno, BlockNumber new_pblkno, XLogRecPtr map_lsn)
{
	BlockNumber  map_blkno;
	int          slot_id;
	int          entry_idx;
	MapPage     *page;
	MapBufferDesc *buf;

	/* Convert (forknum, lblkno) to Umbra metadata-fork block number */
	map_blkno = MapLblknoToMapBlkno(forknum, lblkno);
	entry_idx = map_blkno % MAP_ENTRIES_PER_PAGE;
	map_blkno = map_blkno / MAP_ENTRIES_PER_PAGE;

	/* Read or allocate the map page */
	slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);
	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);

	/* Acquire buffer lock for modifying map page content */
	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);

	/* Set the mapping directly */
	page->pblknos[entry_idx] = new_pblkno;

	/* Record LSN while holding page lock to avoid content/LSN reordering. */
	if (map_lsn == InvalidXLogRecPtr)
	{
		if (InRecovery)
			map_lsn = GetXLogReplayRecPtr(NULL);
		else
			map_lsn = GetXLogWriteRecPtr();
	}
	MapMarkBufferDirty(map_ctx, buf, map_lsn);

	/* Release buffer lock after modification */
	LWLockRelease(&buf->buffer_lock);

	/* Unpin the buffer */
	MapUnpinBuffer(slot_id);
}
