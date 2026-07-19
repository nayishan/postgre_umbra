/*-------------------------------------------------------------------------
 *
 * mapreclaim.c
 *	  Checkpoint-aged reclaim of unreferenced Umbra physical segments.
 *
 * Reclaim is deliberately a primary-only space optimization.  MAP and WAL
 * recovery never depend on a segment being removed, so a standby may retain
 * the same unreferenced segment until it is promoted and discovers a new
 * local candidate.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapreclaim.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/map_internal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "utils/dsa.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#define MAP_RECLAIM_DSA_INIT_SIZE (512 * 1024)

typedef enum MapReclaimOutcome
{
	MAP_RECLAIM_DONE,
	MAP_RECLAIM_CANCEL,
	MAP_RECLAIM_REAGE,
	MAP_RECLAIM_RETRY
} MapReclaimOutcome;

typedef struct MapReclaimTask
{
	MapReclaimTag tag;
	XLogRecPtr	generation_lsn;
	uint64		retired_epoch;
	uint64		retirement_sequence;
} MapReclaimTask;

MapReclaimCtl *MapReclaimCtlData = NULL;

static dsa_area *MapReclaimDSA = NULL;
static dshash_table *MapReclaimHash = NULL;
static dshash_table *MapReservationHash = NULL;
static bool MapReservationCallbackRegistered = false;

static const dshash_parameters MapReclaimHashParams = {
	.key_size = sizeof(MapReclaimTag),
	.entry_size = sizeof(MapReclaimDesc),
	.compare_function = dshash_memcmp,
	.hash_function = dshash_memhash,
	.copy_function = dshash_memcpy,
	.tranche_id = LWTRANCHE_MAP_RECLAIM_MAPPING,
};

static const dshash_parameters MapReservationHashParams = {
	.key_size = sizeof(MapReservationTag),
	.entry_size = sizeof(MapReservationDesc),
	.compare_function = dshash_memcmp,
	.hash_function = dshash_memhash,
	.copy_function = dshash_memcpy,
	.tranche_id = LWTRANCHE_MAP_RECLAIM_MAPPING,
};

static Size MapReclaimShmemSize(void);
static void MapReclaimAttach(void);
static void MapReclaimDetach(int code, Datum arg);
static int MapReclaimCollectEligible(uint64 completed_epoch,
									int max_tasks, MapReclaimTask **tasks);
static MapReclaimOutcome MapReclaimProcessTask(const MapReclaimTask *task);
static bool MapReclaimSegmentReferenced(UmbraFileContext *ctx,
										RelFileLocatorBackend rlocator,
										ForkNumber forknum,
										BlockNumber segno);
static bool MapReclaimSegmentReserved(RelFileLocatorBackend rlocator,
									  ForkNumber forknum,
									  BlockNumber segno);
static bool MapReclaimRangeIntersectsSegment(const UmbraMapRange *range,
											 BlockNumber segno);
static bool MapReclaimTaskStillCurrent(const MapReclaimTask *task);
static void MapReclaimRememberSegment(RelFileLocatorBackend rlocator,
									  ForkNumber forknum, BlockNumber segno,
									  XLogRecPtr generation_lsn, bool re_age);
static void MapReclaimFinishTask(const MapReclaimTask *task,
								 MapReclaimOutcome outcome,
								 uint64 completed_epoch);
static void MapReclaimForgetMatching(const RelFileLocatorBackend *rlocator,
									 Oid dbid, Oid spcOid);
static void MapReclaimReleaseReservations(int owner_procno);
static void MapReclaimReservationXactCallback(XactEvent event, void *arg);
static void MapReclaimReservationExitCallback(int code, Datum arg);

/* Establish backend-local handles and callbacks before WAL critical sections. */
void
MapReclaimBackendInit(void)
{
	if (MapReclaimCtlData == NULL)
		return;
	if (MapReclaimHash == NULL)
		MapReclaimAttach();
	if (MyProc != NULL && !MapReservationCallbackRegistered)
	{
		RegisterXactCallback(MapReclaimReservationXactCallback, NULL);
		before_shmem_exit(MapReclaimReservationExitCallback, 0);
		MapReservationCallbackRegistered = true;
	}
}

/*
 * Record a physical block whose last reference may just have been removed.
 * Callers invoke this before publishing the MAP change, so a concurrent
 * checkpointer sees either the old mapping or an epoch updated for that
 * change.  Queue exhaustion only loses a space optimization, never data.
 */
void
MapReclaimRetirePhysicalBlock(RelFileLocatorBackend rlocator,
							  ForkNumber forknum, BlockNumber pblkno,
							  XLogRecPtr generation_lsn)
{
	if (!BlockNumberIsValid(pblkno))
		return;
	MapReclaimRememberSegment(rlocator, forknum,
							 pblkno / ((BlockNumber) RELSEG_SIZE),
							 generation_lsn, true);
}

/*
 * Reconstruct a lost in-memory candidate from a complete MAP scan.  Repeated
 * discovery must not re-age an existing candidate, or a busy compactor could
 * keep it permanently on the wrong side of every checkpoint boundary.
 */
void
MapReclaimDiscoverSegment(RelFileLocatorBackend rlocator,
						  ForkNumber forknum, BlockNumber segno,
						  XLogRecPtr generation_lsn)
{
	MapReclaimRememberSegment(rlocator, forknum, segno, generation_lsn, false);
}

static void
MapReclaimRememberSegment(RelFileLocatorBackend rlocator,
						  ForkNumber forknum, BlockNumber segno,
						  XLogRecPtr generation_lsn, bool re_age)
{
	MapReclaimDesc *desc;
	MapReclaimTag tag = {0};
	bool		found;
	uint64		retired_epoch;
	uint64		retirement_sequence;

	if (MapReclaimHash == NULL || RecoveryInProgress() ||
		RelFileLocatorBackendIsTemp(rlocator) ||
		!ummap_tracks_fork(forknum) ||
		segno > MaxBlockNumber / ((BlockNumber) RELSEG_SIZE) ||
		!XLogRecPtrIsValid(generation_lsn))
		return;

	tag.rlocator = rlocator;
	tag.forknum = forknum;
	tag.segno = segno;
	retired_epoch = pg_atomic_read_u64(
		&MapReclaimCtlData->checkpoint_started);
	retirement_sequence = pg_atomic_fetch_add_u64(
		&MapReclaimCtlData->retirement_sequence, 1) + 1;

	/*
	 * Existing-page remap publication reaches here inside WAL insertion's
	 * critical section.  It cannot grow the DSA, but it must re-age an already
	 * eligible candidate before removing the last MAP reference to old P.
	 * dshash_find() only locks and probes existing shared memory.  A missing
	 * entry remains an optional space leak and is rediscovered by a complete MAP
	 * scan.
	 */
	if (CritSectionCount > 0)
	{
		desc = dshash_find(MapReclaimHash, &tag, true);
		if (desc == NULL)
			return;
		desc->generation_lsn = generation_lsn;
		desc->retired_epoch = retired_epoch;
		desc->retirement_sequence = retirement_sequence;
		dshash_release_lock(MapReclaimHash, desc);
		return;
	}

	desc = dshash_find_or_insert_extended(MapReclaimHash, &tag, &found,
										 DSHASH_INSERT_NO_OOM);
	if (desc == NULL)
		return;
	if (!found || desc->generation_lsn != generation_lsn)
	{
		desc->generation_lsn = generation_lsn;
		desc->retired_epoch = retired_epoch;
		desc->retirement_sequence = retirement_sequence;
	}
	else if (re_age)
	{
		desc->retired_epoch = retired_epoch;
		desc->retirement_sequence = retirement_sequence;
	}
	dshash_release_lock(MapReclaimHash, desc);
}

/*
 * Publish a relation/fork reservation before releasing its extension lock.
 * One marker per backend is sufficient: it remains until transaction end, so
 * reclaim cannot enter the reserve-to-pending gap or race later publication.
 */
bool
MapReclaimRegisterReservation(RelFileLocatorBackend rlocator,
							  ForkNumber forknum)
{
	MapReservationDesc *desc;
	MapReservationTag tag = {0};
	bool		found;

	if (RecoveryInProgress() || IsBootstrapProcessingMode() ||
		RelFileLocatorBackendIsTemp(rlocator))
		return true;
	if (CritSectionCount > 0)
	{
		/* An unexpected unprotected reservation disables optional reclaim. */
		pg_atomic_write_u32(&MapReclaimCtlData->reservation_unreliable, 1);
		return true;
	}
	if (MapReservationHash == NULL || !MapReservationCallbackRegistered ||
		MyProc == NULL ||
		MyProcNumber == INVALID_PROC_NUMBER)
	{
		if (MapReclaimCtlData != NULL)
			pg_atomic_write_u32(
				&MapReclaimCtlData->reservation_unreliable, 1);
		return true;
	}
	tag.rlocator = rlocator;
	tag.forknum = forknum;
	tag.owner_procno = MyProcNumber;
	desc = dshash_find_or_insert_extended(MapReservationHash, &tag, &found,
										 DSHASH_INSERT_NO_OOM);
	if (desc == NULL)
	{
		/* Never turn optional reclaim accounting into a DML failure. */
		pg_atomic_write_u32(&MapReclaimCtlData->reservation_unreliable, 1);
		return true;
	}
	dshash_release_lock(MapReservationHash, desc);
	return true;
}

/* Mark the boundary before checkpoint buffer capture and redo selection. */
void
MapReclaimCheckpointStart(void)
{
	if (MapReclaimCtlData == NULL || RecoveryInProgress())
		return;
	(void) pg_atomic_fetch_add_u64(&MapReclaimCtlData->checkpoint_started, 1);
}

/* Publish the successful epoch; optional file work belongs to the worker. */
void
MapReclaimCheckpointComplete(void)
{
	uint64		completed_epoch;

	if (MapReclaimCtlData == NULL || RecoveryInProgress())
		return;

	completed_epoch = pg_atomic_read_u64(
		&MapReclaimCtlData->checkpoint_started);
	pg_atomic_write_u64(&MapReclaimCtlData->checkpoint_completed,
						completed_epoch);
	MapWakeCompactor();
}

/*
 * Process only work retired before the last successful checkpoint began.  A
 * remap published during that checkpoint records the current started epoch
 * and therefore cannot remove its source segment until a later checkpoint.
 */
int
MapReclaimStep(int max_tasks)
{
	MapReclaimTask *tasks;
	uint64		completed_epoch;
	int			ntasks;

	/* MAP pins and lifecycle locks are owned by the worker's transaction. */
	Assert(IsTransactionState());
	if (MapReclaimCtlData == NULL || MapReclaimHash == NULL ||
		RecoveryInProgress() || max_tasks <= 0)
		return 0;

	completed_epoch = pg_atomic_read_u64(
		&MapReclaimCtlData->checkpoint_completed);
	ntasks = MapReclaimCollectEligible(completed_epoch, max_tasks, &tasks);
	if (ntasks == 0)
		return 0;

	for (int i = 0; i < ntasks; i++)
	{
		MapReclaimOutcome outcome;

		PG_TRY();
		{
			CHECK_FOR_INTERRUPTS();
			outcome = MapReclaimProcessTask(&tasks[i]);
		}
		PG_CATCH();
		{
			uint64		retry_epoch = pg_atomic_read_u64(
				&MapReclaimCtlData->checkpoint_completed);

			/* ERROR must not leave the same task eligible in this epoch. */
			MapReclaimFinishTask(&tasks[i], MAP_RECLAIM_RETRY,
								 retry_epoch);
			PG_RE_THROW();
		}
		PG_END_TRY();
		completed_epoch = pg_atomic_read_u64(
			&MapReclaimCtlData->checkpoint_completed);
		MapReclaimFinishTask(&tasks[i], outcome, completed_epoch);
	}
	pfree(tasks);
	return ntasks;
}

void
MapReclaimForgetRelation(RelFileLocatorBackend rlocator)
{
	MapReclaimForgetMatching(&rlocator, InvalidOid, InvalidOid);
}

void
MapReclaimForgetDatabase(Oid dbid, Oid spcOid)
{
	MapReclaimForgetMatching(NULL, dbid, spcOid);
}

static int
MapReclaimCollectEligible(uint64 completed_epoch, int max_tasks,
						  MapReclaimTask **tasks)
{
	dshash_seq_status status;
	MapReclaimDesc *desc;
	MapReclaimTask *result;
	int			count = 0;

	Assert(tasks != NULL);
	Assert(max_tasks > 0);
	result = palloc_array(MapReclaimTask, max_tasks);
	/*
	 * Claim each task before any relation or file operation that can ERROR.
	 * Leaving it at the completed epoch makes an interrupted attempt ineligible
	 * until a later successful checkpoint.
	 */
	dshash_seq_init(&status, MapReclaimHash, true);
	while ((desc = dshash_seq_next(&status)) != NULL)
	{
		if (desc->retired_epoch >= completed_epoch)
			continue;
		desc->retired_epoch = completed_epoch;
		result[count].tag = desc->tag;
		result[count].generation_lsn = desc->generation_lsn;
		result[count].retired_epoch = completed_epoch;
		result[count].retirement_sequence = desc->retirement_sequence;
		count++;
		if (count == max_tasks)
			break;
	}
	dshash_seq_term(&status);
	if (count == 0)
	{
		pfree(result);
		result = NULL;
	}
	*tasks = result;
	return count;
}

static MapReclaimOutcome
MapReclaimProcessTask(const MapReclaimTask *task)
{
	UmbraFileContext *volatile ctx = NULL;
	LWLock	   *volatile extension_lock = NULL;
	XLogRecPtr	generation_lsn;
	volatile MapReclaimOutcome outcome = MAP_RECLAIM_RETRY;
	volatile bool database_locked = false;
	volatile bool extension_locked = false;
	volatile bool storage_locked = false;

	/*
	 * Never wait behind CREATE, DROP, or compaction; keeping the request retries
	 * it after the next successful checkpoint.  Transaction ownership provides
	 * the final cleanup path if worker shutdown bypasses this local handler.
	 */
	PG_TRY();
	{
		do
		{
			if (OidIsValid(task->tag.rlocator.locator.dbOid))
			{
				if (!ConditionalLockSharedObject(DatabaseRelationId,
										 task->tag.rlocator.locator.dbOid, 0,
										 RowExclusiveLock))
					break;
				database_locked = true;
			}
			if (!ConditionalLockRelationStorage(
						task->tag.rlocator.locator, AccessExclusiveLock))
				break;
			storage_locked = true;
			extension_lock = MapPageExtensionLock(task->tag.rlocator);
			INJECTION_POINT("umbra-reclaim-before-extension-lock", NULL);
			if (!LWLockConditionalAcquire((LWLock *) extension_lock, LW_EXCLUSIVE))
			{
				INJECTION_POINT("umbra-reclaim-extension-lock-busy", NULL);
				break;
			}
			extension_locked = true;

			ctx = umfile_open_temporary(task->tag.rlocator);
			if (!ummap_exists((UmbraFileContext *) ctx))
				outcome = MAP_RECLAIM_CANCEL;
			else
			{
				generation_lsn = ummap_get_generation_lsn(
					(UmbraFileContext *) ctx, task->tag.rlocator);
				if (!XLogRecPtrIsValid(generation_lsn) ||
					generation_lsn != task->generation_lsn)
					outcome = MAP_RECLAIM_CANCEL;
				else if (!umfile_segment_exists((UmbraFileContext *) ctx,
												task->tag.forknum,
												task->tag.segno))
					outcome = MAP_RECLAIM_DONE;
				else
				{
					if (MapReclaimSegmentReserved(task->tag.rlocator,
											  task->tag.forknum,
											  task->tag.segno) ||
						MapReclaimSegmentReferenced((UmbraFileContext *) ctx,
											 task->tag.rlocator,
											 task->tag.forknum,
											 task->tag.segno))
						outcome = MAP_RECLAIM_REAGE;
					else if (!MapReclaimTaskStillCurrent(task))
						outcome = MAP_RECLAIM_RETRY;
					else
					{
						INJECTION_POINT("umbra-reclaim-before-unlink", NULL);
						if (umfile_unlink_segment((UmbraFileContext *) ctx,
												   task->tag.forknum,
												   task->tag.segno))
						{
							elog(DEBUG1,
								 "Umbra reclaimed physical segment %u/%u/%u fork %d segment %u",
								 task->tag.rlocator.locator.spcOid,
								 task->tag.rlocator.locator.dbOid,
								 task->tag.rlocator.locator.relNumber,
								 (int) task->tag.forknum, task->tag.segno);
							outcome = MAP_RECLAIM_DONE;
						}
						else
							outcome = MAP_RECLAIM_RETRY;
					}
				}
			}
		} while (false);
	}
	PG_CATCH();
	{
		if (ctx != NULL)
			umfile_destroy((UmbraFileContext *) ctx);
		/* Transaction abort releases the LWLock and heavyweight locks. */
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ctx != NULL)
		umfile_destroy((UmbraFileContext *) ctx);
	if (extension_locked)
		LWLockRelease((LWLock *) extension_lock);
	if (storage_locked)
		UnlockRelationStorage(task->tag.rlocator.locator,
							  AccessExclusiveLock);
	if (database_locked)
		UnlockSharedObject(DatabaseRelationId,
							 task->tag.rlocator.locator.dbOid, 0,
							 RowExclusiveLock);
	return (MapReclaimOutcome) outcome;
}

static bool
MapReclaimSegmentReferenced(UmbraFileContext *ctx,
							RelFileLocatorBackend rlocator,
							ForkNumber forknum, BlockNumber segno)
{
	BlockNumber logical_eof;
	BlockNumber physical_frontier;
	BlockNumber lblkno = 0;
	uint64		segment_start = (uint64) segno * RELSEG_SIZE;
	uint64		segment_end = segment_start + RELSEG_SIZE;

	ummap_root_read_frontiers(ctx, rlocator, forknum, &logical_eof,
								  &physical_frontier);
	if (!BlockNumberIsValid(logical_eof) ||
		!BlockNumberIsValid(physical_frontier) ||
		segment_start >= physical_frontier)
		return true;

	/*
	 * Never unlink the segment containing the append-only frontier, including a
	 * full segment whose end is exactly the frontier.  Keeping that high-water
	 * sentinel prevents later capacity checks from treating a reclaimed hole as
	 * physical EOF and materializing the hole again.  Another backend can also
	 * retain an open descriptor for the inode.  Reclaim only older segments.
	 */
	if (segment_end >= physical_frontier)
		return true;

	while (lblkno < logical_eof)
	{
		BlockNumber pblkno;
		BlockNumber nblocks;
		uint64		run_start;
		uint64		run_end;

		nblocks = ummap_lookup_run(ctx, rlocator, forknum, lblkno,
								 logical_eof - lblkno, &pblkno);
		if (nblocks == 0)
			return true;
		run_start = pblkno;
		run_end = run_start + nblocks;
		if (run_start < segment_end && segment_start < run_end)
			return true;
		lblkno += nblocks;
	}
	return false;
}

static bool
MapReclaimSegmentReserved(RelFileLocatorBackend rlocator,
						  ForkNumber forknum, BlockNumber segno)
{
	dshash_seq_status status;
	MapReservationDesc *reservation;

	/* A missed marker makes reclaim fail closed until shared memory resets. */
	if (MapReclaimCtlData == NULL ||
		pg_atomic_read_u32(&MapReclaimCtlData->reservation_unreliable) != 0)
		return true;
	if (MapReservationHash != NULL)
	{
		dshash_seq_init(&status, MapReservationHash, false);
		while ((reservation = dshash_seq_next(&status)) != NULL)
		{
			if (reservation->tag.forknum == forknum &&
				RelFileLocatorBackendEquals(reservation->tag.rlocator,
										 rlocator))
			{
				dshash_seq_term(&status);
				return true;
			}
		}
		dshash_seq_term(&status);
	}

	if (!MapPagePoolIsInitialized())
		return false;

	for (int slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
	{
		MapPageDesc *desc = &MapPageDescriptors[slot_id];
		uint64		state;
		bool		reserved = false;

		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_TAG_VALID) == 0)
			continue;
		LWLockAcquire(&desc->content_lock, LW_SHARED);
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_TAG_VALID) != 0 &&
			RelFileLocatorBackendEquals(desc->tag.rlocator, rlocator))
		{
			MapPagePendingRange *ranges[2] =
			{
				&desc->pending_range,
				&desc->replay_range,
			};

			for (int range_no = 0; range_no < lengthof(ranges); range_no++)
			{
				MapPagePendingRange *range = ranges[range_no];

				if (range->valid && range->forknum == forknum &&
					(MapReclaimRangeIntersectsSegment(&range->range, segno) ||
					 (range->reserved_pblkno_valid &&
					  range->reserved_pblkno /
					  ((BlockNumber) RELSEG_SIZE) == segno)))
				{
					reserved = true;
					break;
				}
			}
		}
		LWLockRelease(&desc->content_lock);
		if (reserved)
			return true;
	}
	return false;
}

static bool
MapReclaimRangeIntersectsSegment(const UmbraMapRange *range,
								 BlockNumber segno)
{
	uint64		segment_start = (uint64) segno * RELSEG_SIZE;
	uint64		segment_end = segment_start + RELSEG_SIZE;
	uint64		range_start = range->first_pblkno;
	uint64		range_end = range_start + range->nblocks;

	return range_start < segment_end && segment_start < range_end;
}

static void
MapReclaimFinishTask(const MapReclaimTask *task, MapReclaimOutcome outcome,
					 uint64 completed_epoch)
{
	MapReclaimDesc *desc;

	desc = dshash_find(MapReclaimHash, &task->tag, true);
	if (desc == NULL)
		return;
	if (desc->generation_lsn != task->generation_lsn ||
		desc->retired_epoch != task->retired_epoch ||
		desc->retirement_sequence != task->retirement_sequence)
	{
		dshash_release_lock(MapReclaimHash, desc);
		return;
	}

	if (outcome == MAP_RECLAIM_DONE || outcome == MAP_RECLAIM_CANCEL)
		dshash_delete_entry(MapReclaimHash, desc);
	else
	{
		if (outcome == MAP_RECLAIM_REAGE || outcome == MAP_RECLAIM_RETRY)
			desc->retired_epoch = completed_epoch;
		dshash_release_lock(MapReclaimHash, desc);
	}
}

static bool
MapReclaimTaskStillCurrent(const MapReclaimTask *task)
{
	MapReclaimDesc *desc;
	bool		current;

	desc = dshash_find(MapReclaimHash, &task->tag, false);
	if (desc == NULL)
		return false;
	current = desc->generation_lsn == task->generation_lsn &&
		desc->retired_epoch == task->retired_epoch &&
		desc->retirement_sequence == task->retirement_sequence;
	dshash_release_lock(MapReclaimHash, desc);
	return current;
}

static void
MapReclaimForgetMatching(const RelFileLocatorBackend *rlocator,
						 Oid dbid, Oid spcOid)
{
	dshash_seq_status status;
	MapReclaimDesc *desc;

	if (MapReclaimHash == NULL)
		return;
	dshash_seq_init(&status, MapReclaimHash, true);
	while ((desc = dshash_seq_next(&status)) != NULL)
	{
		if ((rlocator != NULL &&
			 !RelFileLocatorBackendEquals(desc->tag.rlocator, *rlocator)) ||
			(OidIsValid(dbid) &&
			 desc->tag.rlocator.locator.dbOid != dbid) ||
			(OidIsValid(spcOid) &&
			 desc->tag.rlocator.locator.spcOid != spcOid))
			continue;
		dshash_delete_current(&status);
	}
	dshash_seq_term(&status);
}

static void
MapReclaimReleaseReservations(int owner_procno)
{
	dshash_seq_status status;
	MapReservationDesc *desc;

	if (MapReservationHash == NULL || owner_procno == INVALID_PROC_NUMBER)
		return;
	dshash_seq_init(&status, MapReservationHash, true);
	while ((desc = dshash_seq_next(&status)) != NULL)
	{
		if (desc->tag.owner_procno == owner_procno)
			dshash_delete_current(&status);
	}
	dshash_seq_term(&status);
}

static void
MapReclaimReservationXactCallback(XactEvent event, void *arg)
{
	(void) arg;

	/*
	 * A subabort can leave a harmless allocation hole, but retaining its
	 * marker until the top transaction ends avoids per-subtransaction shared
	 * state and remains conservative.  PREPARE is a final ownership handoff.
	 */
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PREPARE || event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PARALLEL_ABORT)
		MapReclaimReleaseReservations(MyProcNumber);
}

/* A backend exit can bypass the normal top-level transaction callback. */
static void
MapReclaimReservationExitCallback(int code, Datum arg)
{
	(void) code;
	(void) arg;

	if (MapReservationHash != NULL && MyProc != NULL)
		MapReclaimReleaseReservations(MyProcNumber);
}

static Size
MapReclaimShmemSize(void)
{
	Assert(dsa_minimum_size() <= MAP_RECLAIM_DSA_INIT_SIZE);
	return add_size(MAXALIGN(sizeof(MapReclaimCtl)),
					MAP_RECLAIM_DSA_INIT_SIZE);
}

void
MapReclaimShmemRequest(void)
{
	ShmemRequestStruct(.name = "Umbra MAP Reclaim Queue",
					   .size = MapReclaimShmemSize(),
					   .ptr = (void **) &MapReclaimCtlData,
		);
}

void
MapReclaimShmemInit(void)
{
	dsa_area   *dsa;
	dshash_table *hash;
	dshash_table *reservation_hash;
	char	   *raw_area;

	raw_area = (char *) MapReclaimCtlData +
		MAXALIGN(sizeof(MapReclaimCtl));
	MapReclaimCtlData->raw_dsa_area = raw_area;
	pg_atomic_init_u64(&MapReclaimCtlData->checkpoint_started, 0);
	pg_atomic_init_u64(&MapReclaimCtlData->checkpoint_completed, 0);
	pg_atomic_init_u64(&MapReclaimCtlData->retirement_sequence, 0);
	pg_atomic_init_u32(&MapReclaimCtlData->reservation_unreliable, 0);

	dsa = dsa_create_in_place(raw_area, MAP_RECLAIM_DSA_INIT_SIZE,
								  LWTRANCHE_MAP_RECLAIM_MAPPING, NULL);
	dsa_pin(dsa);
	/*
	 * Keep this optional queue inside startup-owned shared memory.  NO_OOM then
	 * drops retirement work or disables reclaim instead of adding a DSM segment;
	 * neither hash is allowed to insert while inside a critical section.
	 */
	dsa_set_size_limit(dsa, MAP_RECLAIM_DSA_INIT_SIZE);
	hash = dshash_create(dsa, &MapReclaimHashParams, NULL);
	MapReclaimCtlData->hash_handle = dshash_get_hash_table_handle(hash);
	reservation_hash = dshash_create(dsa, &MapReservationHashParams, NULL);
	MapReclaimCtlData->reservation_hash_handle =
		dshash_get_hash_table_handle(reservation_hash);

	dshash_detach(reservation_hash);
	dshash_detach(hash);
	dsa_detach(dsa);
}

void
MapReclaimShmemAttach(void)
{
	MapReclaimAttach();
}

static void
MapReclaimAttach(void)
{
	MemoryContext oldcontext;

	if (MapReclaimHash != NULL)
		return;
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	MapReclaimDSA = dsa_attach_in_place(MapReclaimCtlData->raw_dsa_area,
									   NULL);
	dsa_pin_mapping(MapReclaimDSA);
	if (!IsUnderPostmaster)
		dsa_set_size_limit(MapReclaimDSA, MAP_RECLAIM_DSA_INIT_SIZE);
	MapReclaimHash = dshash_attach(MapReclaimDSA, &MapReclaimHashParams,
									MapReclaimCtlData->hash_handle, NULL);
	MapReservationHash = dshash_attach(
		MapReclaimDSA, &MapReservationHashParams,
		MapReclaimCtlData->reservation_hash_handle, NULL);
	if (IsUnderPostmaster)
		before_shmem_exit(MapReclaimDetach, 0);
	MemoryContextSwitchTo(oldcontext);
}

static void
MapReclaimDetach(int code, Datum arg)
{
	if (MapReservationHash != NULL && MyProc != NULL)
		MapReclaimReleaseReservations(MyProcNumber);
	if (MapReservationHash != NULL)
	{
		dshash_detach(MapReservationHash);
		MapReservationHash = NULL;
	}
	if (MapReclaimHash != NULL)
	{
		dshash_detach(MapReclaimHash);
		MapReclaimHash = NULL;
	}
	if (MapReclaimDSA != NULL)
	{
		dsa_detach(MapReclaimDSA);
		dsa_release_in_place(MapReclaimCtlData->raw_dsa_area);
		MapReclaimDSA = NULL;
	}
}
