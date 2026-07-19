/*-------------------------------------------------------------------------
 *
 * mapcompact.c
 *	  Umbra physical extent compaction.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapcompact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "utils/hsearch.h"
#include "utils/injection_point.h"
#include "utils/inval.h"

typedef struct MapExtentLiveEntry
{
	BlockNumber extent_no;
	uint32		live_blocks;
} MapExtentLiveEntry;

typedef struct MapSegmentLiveEntry
{
	BlockNumber segno;
} MapSegmentLiveEntry;

bool		map_compactor_enable = false;
int			map_compactor_extent_blocks = 1024;
int			map_compactor_low_live_percent = 10;
int			map_compactor_max_moves = 16;

static const ForkNumber MapCompactorForks[] =
{
	MAIN_FORKNUM,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM
};

static void MapCompactorCountRun(HTAB *extent_live,
									 BlockNumber pblkno, BlockNumber nblocks,
									 BlockNumber extent_blocks,
									 BlockNumber *highest_extent);
static void MapCompactorCountLiveSegments(HTAB *segment_live,
										  BlockNumber pblkno,
										  BlockNumber nblocks);
static void MapCompactorDiscoverReclaimableSegments(SMgrRelation reln,
													 ForkNumber forknum,
													 BlockNumber logical_eof,
													 HTAB *segment_live,
													 bool scan_complete);
static bool MapCompactorExtentIsSparse(HTAB *extent_live,
									  BlockNumber extent_no,
									  BlockNumber highest_extent,
									  BlockNumber extent_blocks);
static bool MapCompactorRelocateBuffered(SMgrRelation reln,
										 ForkNumber forknum,
										 BlockNumber lblkno,
										 BlockNumber expected_old_pblkno,
										 BufferAccessStrategy strategy);
static int MapCompactorAnalyzeFork(SMgrRelation reln, ForkNumber forknum,
								   int max_moves,
								   BufferAccessStrategy strategy);

static void
MapCompactorCountRun(HTAB *extent_live, BlockNumber pblkno,
					 BlockNumber nblocks, BlockNumber extent_blocks,
					 BlockNumber *highest_extent)
{
	BlockNumber done = 0;

	while (done < nblocks)
	{
		BlockNumber current = pblkno + done;
		BlockNumber extent_no = current / extent_blocks;
		BlockNumber extent_offset = current % extent_blocks;
		BlockNumber chunk = Min(nblocks - done,
								extent_blocks - extent_offset);
		MapExtentLiveEntry *entry;
		bool		found;

		entry = hash_search(extent_live, &extent_no, HASH_ENTER, &found);
		if (!found)
			entry->live_blocks = 0;
		entry->live_blocks += chunk;
		if (!BlockNumberIsValid(*highest_extent) ||
			extent_no > *highest_extent)
			*highest_extent = extent_no;
		done += chunk;
	}
}

static void
MapCompactorCountLiveSegments(HTAB *segment_live, BlockNumber pblkno,
							  BlockNumber nblocks)
{
	uint64		current = pblkno;
	uint64		run_end = current + nblocks;

	while (current < run_end)
	{
		BlockNumber segno = (BlockNumber) (current / RELSEG_SIZE);
		uint64		segment_end = ((uint64) segno + 1) * RELSEG_SIZE;

		(void) hash_search(segment_live, &segno, HASH_ENTER, NULL);
		current = Min(run_end, segment_end);
	}
}

/*
 * Rebuild optional reclaim work from durable MAP state.  The scan is useful
 * only if it covered one stable logical EOF; an incomplete or concurrently
 * changed view must fail closed.  Existing segment names are inspected, but
 * their old physical page contents are never read.
 */
static void
MapCompactorDiscoverReclaimableSegments(SMgrRelation reln,
										 ForkNumber forknum,
										 BlockNumber logical_eof,
										 HTAB *segment_live,
										 bool scan_complete)
{
	UmbraFileContext *volatile ctx = NULL;
	BlockNumber *volatile segnos = NULL;
	int			nsegnos = 0;

	if (!scan_complete)
		return;

	PG_TRY();
	{
		BlockNumber current_logical_eof;
		BlockNumber physical_frontier;
		XLogRecPtr	generation_lsn;

		ctx = umfile_open_temporary(reln->smgr_rlocator);
		ummap_root_read_frontiers((UmbraFileContext *) ctx,
								  reln->smgr_rlocator, forknum,
								  &current_logical_eof, &physical_frontier);
		generation_lsn = ummap_get_generation_lsn((UmbraFileContext *) ctx,
												  reln->smgr_rlocator);
		if (BlockNumberIsValid(current_logical_eof) &&
			current_logical_eof == logical_eof &&
			BlockNumberIsValid(physical_frontier) &&
			XLogRecPtrIsValid(generation_lsn) &&
			umfile_collect_existing_segnos(reln->smgr_rlocator, forknum,
											(BlockNumber **) &segnos, &nsegnos))
		{
			for (int i = 0; i < nsegnos; i++)
			{
				BlockNumber segno = ((BlockNumber *) segnos)[i];
				uint64		segment_end = ((uint64) segno + 1) * RELSEG_SIZE;

				/* Preserve the complete or partial high-water segment. */
				if (segment_end >= (uint64) physical_frontier ||
					hash_search(segment_live, &segno, HASH_FIND, NULL) != NULL)
					continue;
				MapReclaimDiscoverSegment(reln->smgr_rlocator, forknum, segno,
										  generation_lsn);
				INJECTION_POINT("umbra-reclaim-after-discovery", NULL);
			}
		}
	}
	PG_FINALLY();
	{
		if (segnos != NULL)
			pfree((BlockNumber *) segnos);
		if (ctx != NULL)
			umfile_destroy((UmbraFileContext *) ctx);
	}
	PG_END_TRY();
}

static bool
MapCompactorExtentIsSparse(HTAB *extent_live, BlockNumber extent_no,
						   BlockNumber highest_extent,
						   BlockNumber extent_blocks)
{
	MapExtentLiveEntry *entry;
	uint64		live_percent;

	if (extent_no == highest_extent)
		return false;
	entry = hash_search(extent_live, &extent_no, HASH_FIND, NULL);
	if (entry == NULL)
		return false;
	live_percent = ((uint64) entry->live_blocks * 100) / extent_blocks;
	return live_percent <= (uint64) map_compactor_low_live_percent;
}

static bool
MapCompactorRelocateBuffered(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno,
							 BlockNumber expected_old_pblkno,
							 BufferAccessStrategy strategy)
{
	BlockNumber current_pblkno;
	Buffer		buffer;
	bool		relocated = false;

	/*
	 * Shared buffers are the only authoritative relocation source.  This read
	 * either pins the latest resident image or brings a cold page through smgr's
	 * logical mapping; it never copies old-P directly from the physical file.
	 * Recheck the mapping under the exclusive buffer lock before publishing.
	 */
	buffer = ReadBufferWithoutRelcache(reln->smgr_rlocator.locator,
										forknum, lblkno, RBM_NORMAL,
										strategy, true);
	if (!BufferIsPermanent(buffer) || !ConditionalLockBuffer(buffer))
	{
		ReleaseBuffer(buffer);
		return false;
	}

	if (UmGetBlockPhysical(reln, forknum, lblkno, &current_pblkno) &&
		current_pblkno == expected_old_pblkno)
		relocated = UmRelocateBufferedBlock(reln, buffer, forknum,
										lblkno, expected_old_pblkno);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	return relocated;
}

static int
MapCompactorAnalyzeFork(SMgrRelation reln, ForkNumber forknum, int max_moves,
						BufferAccessStrategy strategy)
{
	HASHCTL		ctl;
	HASHCTL		segment_ctl;
	HTAB	   *extent_live;
	HTAB	   *segment_live;
	BlockNumber logical_eof;
	BlockNumber highest_extent = InvalidBlockNumber;
	BlockNumber extent_blocks = (BlockNumber) map_compactor_extent_blocks;
	BlockNumber lblkno = 0;
	int			moves = 0;

	logical_eof = smgrnblocks(reln, forknum);
	if (!BlockNumberIsValid(logical_eof))
		return 0;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BlockNumber);
	ctl.entrysize = sizeof(MapExtentLiveEntry);
	ctl.hcxt = CurrentMemoryContext;
	extent_live = hash_create("Umbra compactor extent live counts", 128,
								  &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	MemSet(&segment_ctl, 0, sizeof(segment_ctl));
	segment_ctl.keysize = sizeof(BlockNumber);
	segment_ctl.entrysize = sizeof(MapSegmentLiveEntry);
	segment_ctl.hcxt = CurrentMemoryContext;
	segment_live = hash_create("Umbra compactor live segments", 16,
								  &segment_ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	while (lblkno < logical_eof)
	{
		BlockNumber pblkno;
		BlockNumber nblocks;

		nblocks = UmGetBlockPhysicalRun(reln, forknum, lblkno,
									   logical_eof - lblkno, &pblkno);
		if (nblocks == 0)
			break;
		MapCompactorCountRun(extent_live, pblkno, nblocks, extent_blocks,
								   &highest_extent);
		MapCompactorCountLiveSegments(segment_live, pblkno, nblocks);
		lblkno += nblocks;
		CHECK_FOR_INTERRUPTS();
	}
	MapCompactorDiscoverReclaimableSegments(reln, forknum, logical_eof,
										 segment_live, lblkno == logical_eof);

	lblkno = 0;
	while (lblkno < logical_eof && moves < max_moves)
	{
		BlockNumber pblkno;
		BlockNumber nblocks;
		BlockNumber done = 0;

		nblocks = UmGetBlockPhysicalRun(reln, forknum, lblkno,
									   logical_eof - lblkno, &pblkno);
		if (nblocks == 0)
			break;
		while (done < nblocks && moves < max_moves)
		{
			BlockNumber current_pblkno = pblkno + done;
			BlockNumber extent_no = current_pblkno / extent_blocks;
			BlockNumber extent_offset = current_pblkno % extent_blocks;
			BlockNumber chunk = Min(nblocks - done,
									extent_blocks - extent_offset);

			if (MapCompactorExtentIsSparse(extent_live, extent_no,
										 highest_extent, extent_blocks))
			{
				for (BlockNumber i = 0; i < chunk && moves < max_moves; i++)
				{
					if (MapCompactorRelocateBuffered(reln, forknum,
												 lblkno + done + i,
											 current_pblkno + i, strategy))
						moves++;
					CHECK_FOR_INTERRUPTS();
				}
			}
			done += chunk;
		}
		lblkno += nblocks;
		CHECK_FOR_INTERRUPTS();
	}

	hash_destroy(extent_live);
	hash_destroy(segment_live);
	return moves;
}

int
MapCompactorStep(int max_relations)
{
	static int	scan_start = 0;
	static int	fork_phase = 0;
	MapSuperTag *tags;
	int			count;
	int			scan_origin;
	int			scanned;
	int			visited;
	int			moves = 0;
	BufferAccessStrategy strategy;

	if (!map_compactor_enable || IsBinaryUpgrade || RecoveryInProgress() ||
		max_relations <= 0 ||
		map_compactor_max_moves <= 0)
		return 0;

	count = MapSuperCollectTags(InvalidOid, InvalidOid, &tags);
	if (count == 0)
		return 0;
	strategy = GetAccessStrategy(BAS_BULKREAD);
	scan_start %= count;
	scan_origin = scan_start;
	visited = Min(count, max_relations);

	for (scanned = 0;
		 scanned < visited && moves < map_compactor_max_moves;
		 scanned++)
	{
		RelFileLocatorBackend rlocator =
			tags[(scan_start + scanned) % count].rlocator;
		SMgrRelation reln;
		bool		database_locked = false;

		CHECK_FOR_INTERRUPTS();
		if (RelFileLocatorBackendIsTemp(rlocator))
			continue;
		/*
		 * A global worker has no relation OID.  Take physical lifecycle locks
		 * before opening smgr state so DROP cannot publish its lifecycle WAL or
		 * free the resident root while this transaction emits relocation WAL.
		 * RowExclusiveLock also conflicts with the ShareLock held while CREATE
		 * DATABASE copies a template's MAP and data files.
		 */
		if (OidIsValid(rlocator.locator.dbOid) &&
			!ConditionalLockSharedObject(DatabaseRelationId,
									 rlocator.locator.dbOid, 0,
									 RowExclusiveLock))
			continue;
		database_locked = OidIsValid(rlocator.locator.dbOid);
		if (!ConditionalLockRelationStorage(rlocator.locator,
										AccessShareLock))
		{
			if (database_locked)
				UnlockSharedObject(DatabaseRelationId,
								   rlocator.locator.dbOid, 0,
								   RowExclusiveLock);
			continue;
		}
		AcceptInvalidationMessages();
		reln = smgropen(rlocator.locator, rlocator.backend);
		if (!smgrexists(reln, MAIN_FORKNUM) ||
			smgrexists(reln, INIT_FORKNUM))
			continue;

		for (int fork_offset = 0;
			 fork_offset < lengthof(MapCompactorForks) &&
			 moves < map_compactor_max_moves;
			 fork_offset++)
		{
			ForkNumber forknum = MapCompactorForks[
				(fork_phase + fork_offset) % lengthof(MapCompactorForks)];

			if (!smgrexists(reln, forknum) ||
				!UmExplicitRemapAvailable(reln, forknum))
				continue;
			moves += MapCompactorAnalyzeFork(reln, forknum,
										 map_compactor_max_moves - moves,
										 strategy);
		}
	}

	/* Advance past only the roots actually examined before the move budget. */
	scan_start = (scan_start + scanned) % count;
	/* Rotate fork priority after each complete pass through resident roots. */
	if (scanned > 0 && scan_origin + scanned >= count)
		fork_phase = (fork_phase + 1) % lengthof(MapCompactorForks);
	FreeAccessStrategy(strategy);
	pfree(tags);
	return moves;
}
