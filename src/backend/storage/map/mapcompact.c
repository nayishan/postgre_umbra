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
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "utils/hsearch.h"
#include "utils/inval.h"

typedef struct MapExtentLiveEntry
{
	BlockNumber extent_no;
	uint32		live_blocks;
} MapExtentLiveEntry;

bool		map_compactor_enable = false;
int			map_compactor_extent_blocks = 1024;
int			map_compactor_low_live_percent = 10;
int			map_compactor_max_moves = 16;

static void MapCompactorCountRun(HTAB *extent_live,
								 BlockNumber pblkno, BlockNumber nblocks,
								 BlockNumber extent_blocks,
								 BlockNumber *highest_extent);
static bool MapCompactorExtentIsSparse(HTAB *extent_live,
									  BlockNumber extent_no,
									  BlockNumber highest_extent,
									  BlockNumber extent_blocks);
static bool MapCompactorRelocateResident(SMgrRelation reln,
										BlockNumber lblkno,
										BlockNumber expected_old_pblkno);
static int MapCompactorAnalyzeRelation(SMgrRelation reln, int max_moves);

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
MapCompactorRelocateResident(SMgrRelation reln, BlockNumber lblkno,
								 BlockNumber expected_old_pblkno)
{
	BufferTag	tag;
	uint32		hashcode;
	LWLock	   *partition_lock;
	BlockNumber current_pblkno;
	Buffer		buffer;
	int			buf_id;
	bool		relocated = false;

	/*
	 * Compaction is opportunistic: do not pull cold relation pages into shared
	 * buffers.  A resident buffer is the authoritative source when it is dirty,
	 * so relocation must pin and exclusively lock that buffer before asking the
	 * WAL insertion path to publish a new P.
	 */
	InitBufferTag(&tag, &reln->smgr_rlocator.locator, MAIN_FORKNUM, lblkno);
	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	LWLockRelease(partition_lock);
	if (buf_id < 0)
		return false;

	buffer = buf_id + 1;
	if (!ReadRecentBuffer(reln->smgr_rlocator.locator, MAIN_FORKNUM,
						  lblkno, buffer))
		return false;
	if (!BufferIsPermanent(buffer) || !ConditionalLockBuffer(buffer))
	{
		ReleaseBuffer(buffer);
		return false;
	}

	if (UmGetBlockPhysical(reln, MAIN_FORKNUM, lblkno, &current_pblkno) &&
		current_pblkno == expected_old_pblkno)
		relocated = UmRelocateBufferedBlock(reln, buffer, MAIN_FORKNUM,
										lblkno, expected_old_pblkno);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	return relocated;
}

static int
MapCompactorAnalyzeRelation(SMgrRelation reln, int max_moves)
{
	HASHCTL		ctl;
	HTAB	   *extent_live;
	BlockNumber logical_eof;
	BlockNumber highest_extent = InvalidBlockNumber;
	BlockNumber extent_blocks = (BlockNumber) map_compactor_extent_blocks;
	BlockNumber lblkno = 0;
	int			moves = 0;

	logical_eof = smgrnblocks(reln, MAIN_FORKNUM);
	if (!BlockNumberIsValid(logical_eof) || logical_eof == 0)
		return 0;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BlockNumber);
	ctl.entrysize = sizeof(MapExtentLiveEntry);
	ctl.hcxt = CurrentMemoryContext;
	extent_live = hash_create("Umbra compactor extent live counts", 128,
							  &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	while (lblkno < logical_eof)
	{
		BlockNumber pblkno;
		BlockNumber nblocks;

		nblocks = UmGetBlockPhysicalRun(reln, MAIN_FORKNUM, lblkno,
									   logical_eof - lblkno, &pblkno);
		if (nblocks == 0)
			break;
		MapCompactorCountRun(extent_live, pblkno, nblocks, extent_blocks,
								   &highest_extent);
		lblkno += nblocks;
		CHECK_FOR_INTERRUPTS();
	}

	lblkno = 0;
	while (lblkno < logical_eof && moves < max_moves)
	{
		BlockNumber pblkno;
		BlockNumber nblocks;
		BlockNumber done = 0;

		nblocks = UmGetBlockPhysicalRun(reln, MAIN_FORKNUM, lblkno,
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
					if (MapCompactorRelocateResident(reln, lblkno + done + i,
											 current_pblkno + i))
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
	return moves;
}

int
MapCompactorStep(int max_relations)
{
	static int	scan_start = 0;
	MapSuperTag *tags;
	int			count;
	int			visited;
	int			moves = 0;

	if (!map_compactor_enable || IsBinaryUpgrade || RecoveryInProgress() ||
		max_relations <= 0 ||
		map_compactor_max_moves <= 0)
		return 0;

	count = MapSuperCollectTags(InvalidOid, InvalidOid, &tags);
	if (count == 0)
		return 0;
	scan_start %= count;
	visited = Min(count, max_relations);

	for (int i = 0; i < visited && moves < map_compactor_max_moves; i++)
	{
		RelFileLocatorBackend rlocator =
			tags[(scan_start + i) % count].rlocator;
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
			smgrexists(reln, INIT_FORKNUM) ||
			!UmWalOwnedRemapAvailable(reln, MAIN_FORKNUM))
			continue;
		moves += MapCompactorAnalyzeRelation(reln,
										map_compactor_max_moves - moves);
	}

	scan_start = (scan_start + visited) % count;
	pfree(tags);
	return moves;
}
