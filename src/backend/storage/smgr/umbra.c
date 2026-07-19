/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager.
 *
 * This file owns the smgr implementation boundary for Umbra.  Ordinary
 * relation fork operations first pass through Umbra's access boundary, then
 * use Umbra's md-style physical segment file layer in umfile.c.
 *
 * The access boundary currently uses the private map fork in identity mode:
 * extension paths materialize new map entries as logical block L -> physical
 * block L.  Later MAP/remap work can replace that identity publication while
 * keeping smgr callbacks separate from physical segment I/O.
 *
 * Relation-local private map fork handling lives in ummap.c.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "storage/map.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	/*
	 * State stored in SMgrRelationData.smgr_private.
	 *
	 * Umbra owns this state for the lifetime of the SMgrRelation handle.
	 * umfile.c owns the physical segment descriptors below this context.
	 */
	UmbraFileContext *filectx;
	bool		uses_map;
} UmbraSmgrRelationState;

typedef struct UmbraRedoCreateState
{
	RelFileLocator rlocator;		/* hash key, must be first */
	uint32		created_forks;
} UmbraRedoCreateState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static bool um_fork_uses_map(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_get_logical_nblocks(SMgrRelation reln,
										  ForkNumber forknum);
static bool um_begin_checkpoint_delay(void);
static void um_end_checkpoint_delay(bool delay_started);
static void um_end_mapping_checkpoint_delay_if_idle(void);
static void um_mapping_xact_callback(XactEvent event, void *arg);
static void um_mapping_subxact_callback(SubXactEvent event,
										SubTransactionId mySubid,
										SubTransactionId parentSubid,
										void *arg);
static void um_prepare_firstborn_target(SMgrRelation reln,
										ForkNumber forknum,
										BlockNumber target_lblkno,
										BlockNumber anchor_lblkno);
static void um_ensure_physical_capacity(UmbraFileContext *ctx,
										ForkNumber forknum,
										BlockNumber first_pblkno,
										BlockNumber nblocks, bool skipFsync);
static bool um_redo_create_seen(RelFileLocator rlocator, ForkNumber forknum);
static void um_forget_redo_create(RelFileLocator rlocator);
static void um_forget_redo_create_database(Oid dbid, Oid spcOid);

static bool um_xact_callbacks_registered = false;
static bool um_mapping_delay_held = false;
static HTAB *um_redo_create_hash = NULL;

void
uminit(void)
{
	umfile_init();
	if (!um_xact_callbacks_registered)
	{
		RegisterXactCallback(um_mapping_xact_callback, NULL);
		RegisterSubXactCallback(um_mapping_subxact_callback, NULL);
		um_xact_callbacks_registered = true;
	}
}

void
umopen(SMgrRelation reln)
{
	UmbraSmgrRelationState *state;

	Assert(reln->smgr_private == NULL);

	state = MemoryContextAllocZero(TopMemoryContext,
								   sizeof(UmbraSmgrRelationState));
	state->filectx = umfile_open(reln);
	/* Existing MAP storage is the durable policy marker on reopen. */
	state->uses_map = !SmgrIsTemp(reln) && ummap_exists(state->filectx);
	reln->smgr_private = state;
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL && state->filectx != NULL)
	{
		umfile_close(state->filectx, forknum);
		if (forknum == MAIN_FORKNUM)
			ummap_close(state->filectx);
	}
}

void
umdestroy(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL)
	{
		if (state->filectx != NULL)
			umfile_destroy(state->filectx);
		pfree(state);
		reln->smgr_private = NULL;
	}
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx = um_get_filectx(reln);

	Assert(state != NULL);
	umfile_create(ctx, forknum, isRedo);

	/* Redo of a mapped fork belongs to a permanent relation. */
	if (isRedo && ummap_tracks_fork(forknum))
	{
		state->uses_map = true;
		if (!ummap_exists(ctx))
			ummap_create(ctx, true);
	}
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->uses_map = needs_wal;
	if (needs_wal)
		ummap_create(state->filectx, false);
}

void
umredocreate(SMgrRelation reln, ForkNumber forknum)
{
	UmbraRedoCreateState *entry;
	bool		found;

	Assert(InRecovery);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	if (ummap_has_recovery_scratch_relation(reln->smgr_rlocator))
		elog(PANIC,
			 "Umbra redo CREATE encountered unresolved recovery mapping for relation %u/%u/%u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber);
	if (um_redo_create_hash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileLocator);
		ctl.entrysize = sizeof(UmbraRedoCreateState);
		ctl.hcxt = TopMemoryContext;
		um_redo_create_hash = hash_create("Umbra redo CREATE state", 128,
										   &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	entry = hash_search(um_redo_create_hash,
						&reln->smgr_rlocator.locator, HASH_ENTER, &found);
	if (!found)
		entry->created_forks = 0;
	entry->created_forks |= (uint32) 1 << forknum;
}

void
umcheckpoint(void)
{
	MapCheckpoint();
}

void
umflushdatabasetablespace(Oid dbid, Oid spcOid)
{
	MapFlushDatabaseTablespace(dbid, spcOid);
}

void
uminvalidatedatabase(Oid dbid)
{
	ummap_forget_recovery_scratch_database(dbid, InvalidOid);
	um_forget_redo_create_database(dbid, InvalidOid);
	MapInvalidateDatabase(dbid);
}

void
uminvalidatedatabasetablespace(Oid dbid, Oid spcOid)
{
	ummap_forget_recovery_scratch_database(dbid, spcOid);
	um_forget_redo_create_database(dbid, spcOid);
	MapInvalidateDatabaseTablespace(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	return umfile_exists(um_get_filectx(reln), forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == InvalidForkNumber)
	{
		um_forget_redo_create(rlocator.locator);
		ummap_forget_recovery_scratch(rlocator, MAIN_FORKNUM, 0);
		MapInvalidateRelation(rlocator);
		umfile_unlink(rlocator, forknum, isRedo);
		return;
	}

	if (forknum == MAIN_FORKNUM)
	{
		um_forget_redo_create(rlocator.locator);
		ummap_forget_recovery_scratch(rlocator, MAIN_FORKNUM, 0);
		ummap_unlink(rlocator, isRedo);
	}

	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	UmbraMapRange pending_range;
	BlockNumber	logical_nblocks;
	BlockNumber	physical_nblocks;
	BlockNumber	pblkno;
	bool		pending_wal_ready;
	volatile bool delay_started = false;
	bool		uses_wal;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_extend(ctx, forknum, blocknum, buffer, skipFsync);
		return;
	}
	if (forknum != MAIN_FORKNUM)
	{
		logical_nblocks = um_get_logical_nblocks(reln, forknum);
		if (blocknum < logical_nblocks)
		{
			const void *buffers[1] = {buffer};

			umfile_writev(ctx, forknum, blocknum, buffers, 1, skipFsync);
			return;
		}
		umfile_extend(ctx, forknum, blocknum, buffer, skipFsync);
		ummap_publish_mapping_run(ctx, reln->smgr_rlocator, forknum,
								  logical_nblocks, logical_nblocks,
								  blocknum - logical_nblocks + 1,
								  InvalidXLogRecPtr, skipFsync);
		return;
	}

	/* A WAL-before-extend reservation is not a visible existing page yet. */
	if (ummap_pending_range_for_target(reln->smgr_rlocator, forknum,
									 blocknum, &pending_range,
									 &pending_wal_ready))
	{
		bool		critical_started = false;

		pblkno = pending_range.first_pblkno +
			(blocknum - pending_range.first_lblkno);
		if (pending_wal_ready)
		{
			START_CRIT_SECTION();
			critical_started = true;
		}
		physical_nblocks = umfile_nblocks(ctx, forknum);
		if (pblkno < physical_nblocks)
		{
			const void *buffers[1] = {buffer};

			umfile_writev(ctx, forknum, pblkno, buffers, 1, skipFsync);
		}
		else
			umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
		ummap_mark_range_physical(reln->smgr_rlocator, forknum, blocknum);
		if (critical_started)
			END_CRIT_SECTION();
		UmMappingPublicationDone();
		return;
	}

	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	if (blocknum < logical_nblocks)
	{
		const void *buffers[1] = {buffer};

		pblkno = ummap_lookup_block(ctx, reln->smgr_rlocator, forknum,
								 blocknum);
		umfile_writev(ctx, forknum, pblkno, buffers, 1, skipFsync);
		return;
	}
	if (InRecovery)
		elog(PANIC,
			 "missing Umbra WAL mapping for recovery extension at logical block %u",
			 blocknum);

	uses_wal = !IsBootstrapProcessingMode() &&
		!RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator);
	if (uses_wal && !um_mapping_delay_held)
	{
		delay_started = um_begin_checkpoint_delay();
		um_mapping_delay_held = delay_started;
	}
	PG_TRY();
	{
		BlockNumber nblocks = blocknum - logical_nblocks + 1;

		physical_nblocks = ummap_next_physical_block(ctx,
												 reln->smgr_rlocator, forknum);
		pblkno = physical_nblocks + (blocknum - logical_nblocks);
		umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
		if (!uses_wal)
			ummap_publish_mapping_run(ctx, reln->smgr_rlocator, forknum,
									  logical_nblocks, physical_nblocks,
									  nblocks, InvalidXLogRecPtr, skipFsync);
		else
			ummap_prepare_firstborn_range(ctx, reln->smgr_rlocator,
												 forknum, logical_nblocks,
												 physical_nblocks, nblocks,
												 InvalidBlockNumber, true);
	}
	PG_CATCH();
	{
		if (delay_started && !ummap_has_pending_ranges())
		{
			um_end_checkpoint_delay(true);
			um_mapping_delay_held = false;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	logical_nblocks;
	BlockNumber	first_pblkno;
	BlockNumber	range_nblocks;
	UmbraMapRange pending_range;
	volatile bool delay_started = false;
	bool		pending_wal_ready;
	bool		uses_wal;

	Assert(nblocks > 0);
	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_zeroextend(ctx, forknum, blocknum, nblocks, skipFsync);
		return;
	}
	if (forknum != MAIN_FORKNUM)
	{
		logical_nblocks = um_get_logical_nblocks(reln, forknum);
		if (blocknum < logical_nblocks)
			elog(ERROR,
				 "cannot zero-extend Umbra fork %d from block %u below logical EOF %u",
				 (int) forknum, blocknum, logical_nblocks);
		umfile_zeroextend(ctx, forknum, blocknum, nblocks, skipFsync);
		ummap_publish_mapping_run(ctx, reln->smgr_rlocator, forknum,
								  logical_nblocks, logical_nblocks,
								  blocknum - logical_nblocks + nblocks,
								  InvalidXLogRecPtr, skipFsync);
		return;
	}

	if (InRecovery)
	{
		BlockNumber done = 0;
		BlockNumber scratch_end;
		UmbraMapRange replay_range;
		bool		have_replay_range;
		bool		replay_physical_ready;

		logical_nblocks = um_get_logical_nblocks(reln, forknum);
		scratch_end = blocknum + (BlockNumber) nblocks;
		have_replay_range = ummap_replay_range_for_extension(
			reln->smgr_rlocator, forknum, blocknum, (BlockNumber) nblocks,
			&replay_range, &replay_physical_ready);
		if (have_replay_range && replay_range.first_lblkno < scratch_end)
		{
			/*
			 * A prior recovery attempt may already have persisted a prefix of
			 * this exact range.  That canonical prefix legitimately lies below
			 * the recovered EOF; only the uncovered gap before the range needs
			 * recovery scratch.
			 */
			scratch_end = replay_range.first_lblkno;
		}

		if (blocknum >= logical_nblocks && scratch_end > logical_nblocks)
		{
			BlockNumber recovery_nblocks;

			/*
			 * This path runs after bufmgr has protected every logical block in
			 * the extension chunk with BM_IO_IN_PROGRESS.  A missing exact WAL
			 * range can therefore be represented as non-persistent recovery
			 * scratch until a later DROP or TRUNCATE proves it disposable.
			 */
			if (um_redo_create_seen(reln->smgr_rlocator.locator,
								MAIN_FORKNUM))
				elog(PANIC,
					 "missing Umbra WAL mapping after redo create for relation %u/%u/%u block %u",
					 reln->smgr_rlocator.locator.spcOid,
					 reln->smgr_rlocator.locator.dbOid,
					 reln->smgr_rlocator.locator.relNumber, blocknum);

			recovery_nblocks = scratch_end - logical_nblocks;
			XLogRecordInvalidPage(reln->smgr_rlocator.locator, forknum,
							  logical_nblocks, false);
			first_pblkno = ummap_next_physical_block(ctx,
											 reln->smgr_rlocator, forknum);
			um_ensure_physical_capacity(ctx, forknum, first_pblkno,
									 recovery_nblocks, skipFsync);
			pending_range.first_lblkno = logical_nblocks;
			pending_range.first_pblkno = first_pblkno;
			pending_range.nblocks = recovery_nblocks;
			ummap_prepare_recovery_scratch_range(ctx, reln->smgr_rlocator,
											 forknum, &pending_range);
			reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
		}
		if (have_replay_range && !replay_physical_ready)
		{
			um_ensure_physical_capacity(ctx, forknum,
									replay_range.first_pblkno,
									replay_range.nblocks, skipFsync);
			ummap_mark_replay_range_physical(reln->smgr_rlocator, forknum,
										  &replay_range);
		}

		while (done < (BlockNumber) nblocks)
		{
			BlockNumber pblkno;
			BlockNumber run_blocks;

			run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
										 blocknum + done,
										 (BlockNumber) nblocks - done,
										 &pblkno);
			um_ensure_physical_capacity(ctx, forknum, pblkno, run_blocks,
								 skipFsync);
			done += run_blocks;
		}
		return;
	}

	if (ummap_pending_range_for_target(reln->smgr_rlocator, forknum,
									 blocknum, &pending_range,
									 &pending_wal_ready))
	{
		BlockNumber offset = blocknum - pending_range.first_lblkno;
		bool		critical_started = false;

		if ((uint64) offset + nblocks > pending_range.nblocks)
			elog(ERROR, "zero extension crosses an Umbra pending mapping range");
		if (pending_wal_ready)
		{
			START_CRIT_SECTION();
			critical_started = true;
		}
		um_ensure_physical_capacity(ctx, forknum,
								 pending_range.first_pblkno + offset,
								 (BlockNumber) nblocks, skipFsync);
		ummap_mark_range_physical(reln->smgr_rlocator, forknum, blocknum);
		if (critical_started)
			END_CRIT_SECTION();
		UmMappingPublicationDone();
		return;
	}

	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	if (blocknum < logical_nblocks)
		elog(ERROR,
			 "cannot zero-extend Umbra fork %d from block %u below logical EOF %u",
			 (int) forknum, blocknum, logical_nblocks);
	range_nblocks = blocknum - logical_nblocks + nblocks;
	first_pblkno = ummap_next_physical_block(ctx, reln->smgr_rlocator,
											 forknum);

	uses_wal = !IsBootstrapProcessingMode() &&
		!RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator);
	if (uses_wal && !um_mapping_delay_held)
	{
		delay_started = um_begin_checkpoint_delay();
		um_mapping_delay_held = delay_started;
	}
	PG_TRY();
	{
		um_ensure_physical_capacity(ctx, forknum, first_pblkno, range_nblocks,
								 skipFsync);
		if (!uses_wal)
			ummap_publish_mapping_run(ctx, reln->smgr_rlocator, forknum,
									  logical_nblocks, first_pblkno,
									  range_nblocks, InvalidXLogRecPtr,
									  skipFsync);
		else
			ummap_prepare_firstborn_range(ctx, reln->smgr_rlocator,
												 forknum, logical_nblocks,
												 first_pblkno, range_nblocks,
												 InvalidBlockNumber, true);
	}
	PG_CATCH();
	{
		if (delay_started && !ummap_has_pending_ranges())
		{
			um_end_checkpoint_delay(true);
			um_mapping_delay_held = false;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static bool
um_redo_create_seen(RelFileLocator rlocator, ForkNumber forknum)
{
	UmbraRedoCreateState *entry;

	if (um_redo_create_hash == NULL)
		return false;
	entry = hash_search(um_redo_create_hash, &rlocator, HASH_FIND, NULL);
	return entry != NULL &&
		(entry->created_forks & ((uint32) 1 << forknum)) != 0;
}

static void
um_forget_redo_create(RelFileLocator rlocator)
{
	if (um_redo_create_hash != NULL)
		(void) hash_search(um_redo_create_hash, &rlocator, HASH_REMOVE, NULL);
}

static void
um_forget_redo_create_database(Oid dbid, Oid spcOid)
{
	HASH_SEQ_STATUS status;
	UmbraRedoCreateState *entry;

	if (um_redo_create_hash == NULL)
		return;

	hash_seq_init(&status, um_redo_create_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->rlocator.dbOid != dbid ||
			(OidIsValid(spcOid) && entry->rlocator.spcOid != spcOid))
			continue;
		if (hash_search(um_redo_create_hash, &entry->rlocator,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "Umbra redo CREATE hash table corrupted");
	}
}

void
UmPrepareFirstbornLocator(RelFileLocator rlocator, ForkNumber forknum,
						  BlockNumber lblkno)
{
	SMgrRelation reln;

	if (IsBootstrapProcessingMode() || RelFileLocatorSkippingWAL(rlocator))
		return;
	if (forknum != MAIN_FORKNUM)
		return;
	reln = smgropen(rlocator, INVALID_PROC_NUMBER);
	if (!um_fork_uses_map(reln, forknum))
		return;

	um_prepare_firstborn_target(reln, forknum, lblkno, lblkno);
}

void
UmPrepareFirstbornRangeLocator(RelFileLocator rlocator, ForkNumber forknum,
							   int nblocks, const BlockNumber *lblknos)
{
	SMgrRelation reln;
	BlockNumber logical_nblocks;
	BlockNumber anchor_lblkno = InvalidBlockNumber;

	Assert(nblocks > 0);
	Assert(lblknos != NULL);
	for (int i = 0; i < nblocks; i++)
	{
		if (!BlockNumberIsValid(lblknos[i]) ||
			(i > 0 && lblknos[i] <= lblknos[i - 1]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Umbra first-born block range is not strictly increasing")));
	}
	if (IsBootstrapProcessingMode() || RelFileLocatorSkippingWAL(rlocator))
		return;
	if (forknum != MAIN_FORKNUM)
		return;

	reln = smgropen(rlocator, INVALID_PROC_NUMBER);
	if (!um_fork_uses_map(reln, forknum))
		return;
	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	for (int i = 0; i < nblocks; i++)
	{
		if (lblknos[i] >= logical_nblocks)
		{
			anchor_lblkno = lblknos[i];
			break;
		}
	}
	if (!BlockNumberIsValid(anchor_lblkno))
		return;

	/* One WAL header covers this batch and any zero-filled logical gaps. */
	um_prepare_firstborn_target(reln, forknum, lblknos[nblocks - 1],
								anchor_lblkno);
}

bool
UmLogMappingRange(RelFileLocator rlocator, ForkNumber forknum,
				  BlockNumber startblk, BlockNumber endblk,
				  BlockNumber anchor_lblkno)
{
	SMgrRelation reln;
	UmbraFileContext *ctx;
	BlockNumber pblkno;
	BlockNumber run_blocks;
	bool		delay_started;

	Assert(!InRecovery);
	if (startblk >= endblk || anchor_lblkno < startblk ||
		anchor_lblkno >= endblk)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Umbra WAL mapping range")));
	if (!RelFileLocatorSkippingWAL(rlocator))
		return false;
	if (forknum != MAIN_FORKNUM)
		return false;

	reln = smgropen(rlocator, INVALID_PROC_NUMBER);
	if (!um_fork_uses_map(reln, forknum))
		return false;
	ctx = um_get_filectx(reln);
	delay_started = um_begin_checkpoint_delay();
	PG_TRY();
	{
		run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
									  startblk, endblk - startblk, &pblkno);
		if (run_blocks != endblk - startblk)
			ereport(ERROR,
					(errmsg("Umbra skip-WAL mapping range is not physically contiguous")));
		ummap_prepare_wal_range(ctx, reln->smgr_rlocator, forknum,
								startblk, pblkno, run_blocks,
								anchor_lblkno);
	}
	PG_CATCH();
	{
		um_end_checkpoint_delay(delay_started);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return delay_started;
}

void
UmLogMappingRangeFinish(bool delay_started)
{
	UmMappingPublicationDone();
	um_end_checkpoint_delay(delay_started);
}

void
UmMappingPublicationDone(void)
{
	MemoryContext oldcontext = CurrentMemoryContext;

	if (CritSectionCount > 0)
	{
		um_end_mapping_checkpoint_delay_if_idle();
		return;
	}

	PG_TRY();
	{
		ummap_publish_ready_ranges();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		edata->elevel = PANIC;
		ThrowErrorData(edata);
		pg_unreachable();
	}
	PG_END_TRY();
	um_end_mapping_checkpoint_delay_if_idle();
}

void
UmPrepareReplayMappingRange(SMgrRelation reln, ForkNumber forknum,
								const UmbraMapRange *range, XLogRecPtr lsn)
{
	UmbraFileContext *ctx;
	BlockNumber logical_nblocks;
	bool		physical_ready;

	Assert(InRecovery);
	Assert(range != NULL);
	if (forknum != MAIN_FORKNUM)
		return;
	smgrcreate(reln, forknum, true);
	if (!um_fork_uses_map(reln, forknum))
		return;
	ctx = um_get_filectx(reln);
	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	physical_ready = logical_nblocks >= range->first_lblkno;
	if (physical_ready)
		um_ensure_physical_capacity(ctx, forknum, range->first_pblkno,
									range->nblocks, false);
	ummap_prepare_replay_range(ctx, reln->smgr_rlocator, forknum, range, lsn,
							  physical_ready);
	reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
}

void
UmPublishReplayMappingRanges(void)
{
	ummap_publish_replay_ranges();
}

void
UmCheckRecoveryDependencies(void)
{
	if (ummap_has_replay_ranges())
		elog(PANIC, "Umbra exact replay mapping survived its WAL record");
	if (ummap_has_recovery_scratch())
		elog(PANIC,
			 "WAL ended with unresolved Umbra recovery mappings");
}

void
UmRecoveryEnd(void)
{
	Assert(!ummap_has_replay_ranges());
	Assert(!ummap_has_recovery_scratch());
	ummap_recovery_end();
	if (um_redo_create_hash != NULL)
	{
		hash_destroy(um_redo_create_hash);
		um_redo_create_hash = NULL;
	}
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	BlockNumber	pblkno;
	BlockNumber	remblocks;

	Assert(nblocks > 0);
	if (!um_fork_uses_map(reln, forknum))
		return umfile_prefetch(um_get_filectx(reln), forknum, blocknum,
							   nblocks);

	remblocks = nblocks;
	while (remblocks > 0)
	{
		BlockNumber run_blocks;

		run_blocks = ummap_lookup_run(um_get_filectx(reln),
									  reln->smgr_rlocator, forknum,
									  blocknum, remblocks, &pblkno);
		if (!umfile_prefetch(um_get_filectx(reln), forknum, pblkno,
							 run_blocks))
			return false;

		blocknum += run_blocks;
		remblocks -= run_blocks;
	}

	return true;
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	logical_nblocks;
	BlockNumber	logical_run;
	BlockNumber	map_run;
	BlockNumber	pblkno;
	uint32		file_run;

	if (!um_fork_uses_map(reln, forknum))
		return umfile_maxcombine(ctx, forknum, blocknum);

	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	if (blocknum >= logical_nblocks)
		return 1;

	/* Do not ask the MAP layer to inspect entries beyond the relation EOF. */
	logical_run = Min(RELSEG_SIZE -
					  (blocknum % ((BlockNumber) RELSEG_SIZE)),
					  logical_nblocks - blocknum);
	map_run = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum, blocknum,
							   logical_run, &pblkno);
	file_run = umfile_maxcombine(ctx, forknum, pblkno);

	return Min(map_run, (BlockNumber) file_run);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers,
					 nblocks);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_run(um_get_filectx(reln),
										  reln->smgr_rlocator, forknum,
										  blocknum, nblocks, &pblkno);
		umfile_readv(um_get_filectx(reln), forknum, pblkno, buffers,
					 run_blocks);

		blocknum += run_blocks;
		buffers += run_blocks;
		nblocks -= run_blocks;
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks,
								 false);
		umfile_startreadv_physical(ioh, um_get_filectx(reln), forknum,
								   blocknum, blocknum,
								   buffers, nblocks);
		return;
	}

	run_blocks = ummap_lookup_run(um_get_filectx(reln),
								  reln->smgr_rlocator, forknum, blocknum,
								  nblocks, &pblkno);
	Assert(run_blocks == nblocks);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, run_blocks,
							 false);
	umfile_startreadv_physical(ioh, um_get_filectx(reln), forknum,
								   blocknum, pblkno, buffers, run_blocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers,
					  nblocks, skipFsync);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_write_run(um_get_filectx(reln),
											reln->smgr_rlocator, forknum,
											blocknum, nblocks, &pblkno);
		umfile_writev(um_get_filectx(reln), forknum, pblkno, buffers,
					  run_blocks, skipFsync);

		blocknum += run_blocks;
		buffers += run_blocks;
		nblocks -= run_blocks;
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
				BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_write_run(um_get_filectx(reln),
											reln->smgr_rlocator, forknum,
											blocknum, nblocks, &pblkno);
		umfile_writeback(um_get_filectx(reln), forknum, pblkno, run_blocks);

		blocknum += run_blocks;
		nblocks -= run_blocks;
	}
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	return um_get_logical_nblocks(reln, forknum);
}

void
umpretruncate(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber old_blocks, BlockNumber nblocks,
			  XLogRecPtr truncate_lsn)
{
	if (!um_fork_uses_map(reln, forknum))
		return;

	if (InRecovery)
		ummap_forget_recovery_scratch(reln->smgr_rlocator, forknum, nblocks);
	ummap_truncate(um_get_filectx(reln), reln->smgr_rlocator, forknum,
					 old_blocks, nblocks, truncate_lsn, false);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	if (um_fork_uses_map(reln, forknum) && forknum == MAIN_FORKNUM)
	{
		/* umpretruncate() already cleared the logical MAP; P is append-only. */
		return;
	}

	umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (um_fork_uses_map(reln, forknum))
		MapFlushRelation(ctx, reln->smgr_rlocator);
	umfile_immedsync(ctx, forknum);
	if (um_fork_uses_map(reln, forknum))
		ummap_immedsync_if_exists(ctx);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (um_fork_uses_map(reln, forknum))
		MapFlushRelation(ctx, reln->smgr_rlocator);
	umfile_registersync(ctx, forknum);
	if (um_fork_uses_map(reln, forknum))
		ummap_registersync_if_exists(ctx);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	BlockNumber	pblkno;

	if (um_fork_uses_map(reln, forknum))
		pblkno = ummap_lookup_block(um_get_filectx(reln),
									reln->smgr_rlocator, forknum, blocknum);
	else
		pblkno = blocknum;
	return umfile_fd(um_get_filectx(reln), forknum, pblkno, off);
}

static bool
um_fork_uses_map(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	return state->uses_map && ummap_tracks_fork(forknum);
}

static BlockNumber
um_get_logical_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (um_fork_uses_map(reln, forknum))
		return ummap_nblocks(um_get_filectx(reln), reln->smgr_rlocator,
							 forknum);

	return umfile_nblocks(um_get_filectx(reln), forknum);
}

static UmbraFileContext *
um_get_filectx(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	Assert(state->filectx != NULL);

	return state->filectx;
}

static void
um_prepare_firstborn_target(SMgrRelation reln, ForkNumber forknum,
							BlockNumber target_lblkno,
							BlockNumber anchor_lblkno)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	UmbraMapRange pending_range;
	BlockNumber logical_nblocks;
	BlockNumber first_pblkno;
	BlockNumber nblocks;
	bool		pending_wal_ready;
	volatile bool delay_started = false;

	if (!BlockNumberIsValid(target_lblkno) ||
		!BlockNumberIsValid(anchor_lblkno))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("invalid Umbra first-born logical block")));
	if (ummap_pending_range_for_target(reln->smgr_rlocator, forknum,
									 target_lblkno, &pending_range,
									 &pending_wal_ready))
		return;

	logical_nblocks = um_get_logical_nblocks(reln, forknum);
	if (target_lblkno < logical_nblocks)
		return;
	if (anchor_lblkno < logical_nblocks || anchor_lblkno > target_lblkno)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Umbra WAL anchor is outside its first-born range")));

	if (!um_mapping_delay_held)
	{
		delay_started = um_begin_checkpoint_delay();
		um_mapping_delay_held = delay_started;
	}
	PG_TRY();
	{
		first_pblkno = ummap_next_physical_block(ctx, reln->smgr_rlocator,
												forknum);
		nblocks = target_lblkno - logical_nblocks + 1;
		ummap_prepare_firstborn_range(ctx, reln->smgr_rlocator, forknum,
									logical_nblocks, first_pblkno, nblocks,
									anchor_lblkno, false);
	}
	PG_CATCH();
	{
		if (delay_started && !ummap_has_pending_ranges())
		{
			um_end_checkpoint_delay(true);
			um_mapping_delay_held = false;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static bool
um_begin_checkpoint_delay(void)
{
	/*
	 * Keep checkpoint start out of the interval in which mapping WAL can
	 * already precede the checkpoint redo point but canonical MAP state is not
	 * yet visible to checkpoint writeback.  Publication correctness itself is
	 * owned by the pending-range path; this flag only protects that boundary.
	 */
	if (InRecovery || (MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0)
		return false;

	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	return true;
}

static void
um_end_checkpoint_delay(bool delay_started)
{
	if (delay_started)
		MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
}

static void
um_end_mapping_checkpoint_delay_if_idle(void)
{
	if (um_mapping_delay_held && !ummap_has_pending_ranges())
	{
		um_end_checkpoint_delay(true);
		um_mapping_delay_held = false;
	}
}

static void
um_mapping_xact_callback(XactEvent event, void *arg)
{
	(void) arg;

	if (event == XACT_EVENT_PRE_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT ||
		event == XACT_EVENT_PRE_PREPARE)
	{
		UmMappingPublicationDone();
		if (ummap_has_pending_ranges())
			elog(PANIC, "Umbra mapping reached commit before publication");
		um_end_mapping_checkpoint_delay_if_idle();
		return;
	}

	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
	{
		ummap_abort_pending_ranges(InvalidSubTransactionId);
		if (um_mapping_delay_held)
		{
			um_end_checkpoint_delay(true);
			um_mapping_delay_held = false;
		}
	}
	else if (event == XACT_EVENT_COMMIT ||
			 event == XACT_EVENT_PARALLEL_COMMIT ||
			 event == XACT_EVENT_PREPARE)
	{
		if (ummap_has_pending_ranges())
			elog(PANIC, "Umbra mapping survived transaction completion");
		um_end_mapping_checkpoint_delay_if_idle();
	}
}

static void
um_mapping_subxact_callback(SubXactEvent event,
							SubTransactionId mySubid,
							SubTransactionId parentSubid, void *arg)
{
	(void) arg;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
		ummap_reparent_pending_ranges(mySubid, parentSubid);
	else if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		ummap_abort_pending_ranges(mySubid);
		um_end_mapping_checkpoint_delay_if_idle();
	}
}

static void
um_ensure_physical_capacity(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber first_pblkno, BlockNumber nblocks,
							bool skipFsync)
{
	BlockNumber physical_nblocks;
	uint64		required_nblocks;

	Assert(nblocks > 0);
	required_nblocks = (uint64) first_pblkno + nblocks;
	if (required_nblocks > (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot materialize Umbra physical mapping range")));
	physical_nblocks = umfile_nblocks(ctx, forknum);
	if ((uint64) physical_nblocks >= required_nblocks)
		return;

	/*
	 * Redo may revisit P after a prior recovery already restored its data.
	 * Only extend the physical capacity; never zero or rewrite existing P.
	 */
	while ((uint64) physical_nblocks < required_nblocks)
	{
		uint64		remaining = required_nblocks - physical_nblocks;
		int			chunk = (int) Min(remaining, (uint64) INT_MAX);

		umfile_zeroextend(ctx, forknum, physical_nblocks, chunk, skipFsync);
		physical_nblocks += chunk;
	}
}
