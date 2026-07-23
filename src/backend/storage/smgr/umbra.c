/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager.
 *
 * This file owns the smgr implementation boundary for Umbra.  Ordinary
 * relation fork operations are serviced by Umbra's md-style physical segment
 * file layer in umfile.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/map.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct UmbraSlot0TruncateState
{
	bool		prepared;
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	current_physical;
} UmbraSlot0TruncateState;

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
	bool		map_policy_probe_done;
	bool		main_slot0_active;
	bool		fsm_slot0_active;
	bool		vm_slot0_active;
	UmbraSlot0TruncateState slot0_truncate[MAX_FORKNUM + 1];
	XLogRecPtr	generation_lsn;
} UmbraSmgrRelationState;

typedef struct UmbraRedoGenerationState
{
	RelFileLocator rlocator;		/* hash key, must be first */
	bool		discard_precreate_redo;
} UmbraRedoGenerationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static void um_reset_recovery_storage(SMgrRelation reln);
static UmbraRedoGenerationState *um_get_redo_generation_state(
	RelFileLocator rlocator, bool create);
static void um_forget_redo_generation_state(RelFileLocator rlocator);
static void um_forget_redo_generation_database(Oid dbid, Oid spcOid);
static void um_refresh_slot0_policy(SMgrRelation reln, ForkNumber forknum);
static bool um_main_uses_slot0(SMgrRelation reln, ForkNumber forknum);
static bool um_fork_uses_slot0(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_main_active_pblk(SMgrRelation reln,
							   BlockNumber logical_block);
static BlockNumber um_slot0_active_pblk(SMgrRelation reln,
									 ForkNumber forknum,
									 BlockNumber logical_block);
static BlockNumber um_slot0_range_end(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber blocknum, BlockNumber nblocks);
static void um_get_slot0_frontiers(SMgrRelation reln, ForkNumber forknum,
									   BlockNumber *logical_eof,
									   BlockNumber *physical_capacity);
static void um_require_slot0_range(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber blocknum, BlockNumber nblocks);
static void um_ensure_slot0_capacity(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber physical_capacity,
									  bool skipFsync);
static void um_ensure_aux_slot0_recovery_capacity(SMgrRelation reln,
												 ForkNumber forknum);
static void um_zero_slot0_range(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber first_block, BlockNumber end_block,
								  bool skipFsync);
static void um_publish_slot0_after_data(SMgrRelation reln, ForkNumber forknum,
									BlockNumber logical_eof,
									BlockNumber physical_capacity);
static void um_publish_slot0_before_truncate(SMgrRelation reln,
										  ForkNumber forknum,
										  BlockNumber logical_eof,
										  BlockNumber physical_capacity);
static void um_ensure_main_selector_pages(SMgrRelation reln,
										 BlockNumber first_block,
										 BlockNumber nblocks, bool skipFsync);

static HTAB *um_redo_generation_hash = NULL;

void
uminit(void)
{
	umfile_init();
}

void
umopen(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	/*
	 * smgropen() enters its hash table before invoking this callback.  Make
	 * initialization retryable in case an allocation ERROR is caught by the
	 * caller and leaves that SMgrRelation entry in place.
	 */
	if (state == NULL)
	{
		state = MemoryContextAllocZero(TopMemoryContext,
									   sizeof(UmbraSmgrRelationState));
		reln->smgr_private = state;
	}

	if (state->filectx == NULL)
	{
		state->filectx = umfile_open(reln->smgr_rlocator);
		state->uses_map = !RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
			ummap_exists(state->filectx);
		state->map_policy_probe_done = true;
		state->main_slot0_active = false;
		state->fsm_slot0_active = false;
		state->vm_slot0_active = false;
		state->generation_lsn = InvalidXLogRecPtr;
	}

	/* CREATE redo repairs a deterministic root before normal validation. */
	if (!InRecovery && state->uses_map && IsUnderPostmaster &&
		!IsBootstrapProcessingMode() && !IsInitProcessingMode())
	{
		ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
		state->main_slot0_active =
			ummap_main_slot0_active(state->filectx, reln->smgr_rlocator);
		state->fsm_slot0_active = ummap_aux_slot0_active(state->filectx,
			FSM_FORKNUM, reln->smgr_rlocator);
		state->vm_slot0_active = ummap_aux_slot0_active(state->filectx,
			VISIBILITYMAP_FORKNUM, reln->smgr_rlocator);
		state->generation_lsn = ummap_get_generation_lsn(state->filectx,
														 reln->smgr_rlocator);
	}
	else if (InRecovery && state->uses_map)
	{
		/* A valid root identifies its current generation before CREATE redo. */
		(void) ummap_try_main_slot0_active(state->filectx,
												reln->smgr_rlocator,
												&state->main_slot0_active);
		(void) ummap_try_aux_slot0_active(state->filectx, FSM_FORKNUM,
										   reln->smgr_rlocator,
										   &state->fsm_slot0_active);
		(void) ummap_try_aux_slot0_active(state->filectx,
										   VISIBILITYMAP_FORKNUM,
										   reln->smgr_rlocator,
										   &state->vm_slot0_active);
		(void) ummap_try_get_generation_lsn(state->filectx,
										reln->smgr_rlocator,
										&state->generation_lsn);
	}
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL && state->filectx != NULL)
	{
		if (forknum == MAIN_FORKNUM)
		{
			umfile_close(state->filectx, UMBRA_METADATA_FORKNUM);
			/* A later open must rediscover a copied, replaced, or removed root. */
			state->uses_map = false;
			state->map_policy_probe_done = false;
			state->main_slot0_active = false;
			state->fsm_slot0_active = false;
			state->vm_slot0_active = false;
			state->generation_lsn = InvalidXLogRecPtr;
			for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
				state->slot0_truncate[forkidx].prepared = false;
		}
		umfile_close(state->filectx, forknum);
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
	bool		map_auxiliary;
	bool		fork_existed = false;

	Assert(state != NULL);
	if (!isRedo)
		um_refresh_slot0_policy(reln, forknum);
	/*
	 * Ordinary page redo marks a permanent relation as map-capable before an
	 * authoritative CREATE has established its root.  Only a relation whose
	 * MAIN fork has already entered slot-0 policy may activate an auxiliary
	 * slot-0 fork; bootstrap system relations have no _map fork at all.
	 */
	map_auxiliary = state->main_slot0_active &&
		UmbraAuxiliaryForkUsesSlot0(forknum);
	if (map_auxiliary)
	{
		fork_existed = umfile_exists(ctx, forknum);
		/* umfile_create() requires this fork to have no cached descriptor. */
		umfile_close(ctx, forknum);
	}
	umfile_create(ctx, forknum, isRedo);
	if (isRedo && forknum == MAIN_FORKNUM &&
		!RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
	{
		state->uses_map = true;
		/*
		 * XLogReadBufferExtended() calls smgrcreate() for ordinary page
		 * redo too.  Preserve a slot-0 root already recognized by umopen(),
		 * and probe an existing root only when this handle has not seen it.
		 */
		if (!state->main_slot0_active)
			(void) ummap_try_main_slot0_active(ctx, reln->smgr_rlocator,
											&state->main_slot0_active);
	}
	if (map_auxiliary)
	{
		bool		active = ummap_aux_slot0_active(ctx, forknum,
															  reln->smgr_rlocator);

		/*
		 * Preserve a pre-existing direct fork during normal operation.  Redo,
		 * however, must recover the mapping policy even when the physical fork
		 * survived a crash before its root update became durable.
		 */
		if (!active && (isRedo || !fork_existed))
		{
			ummap_activate_aux_slot0(ctx, forknum, reln->smgr_rlocator);
			active = true;
			reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
		}
		if (forknum == FSM_FORKNUM)
			state->fsm_slot0_active = active;
		else
			state->vm_slot0_active = active;
	}
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->uses_map = needs_wal && IsUnderPostmaster &&
		!IsBootstrapProcessingMode() && !IsInitProcessingMode();
	state->map_policy_probe_done = true;
	state->main_slot0_active = false;
	state->fsm_slot0_active = false;
	state->vm_slot0_active = false;
	state->generation_lsn = InvalidXLogRecPtr;
	if (state->uses_map)
		ummap_create(um_get_filectx(reln), reln->smgr_rlocator, false);
}

void
umsetgeneration(SMgrRelation reln, ForkNumber forknum,
				XLogRecPtr generation_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	if (forknum == MAIN_FORKNUM && state->uses_map)
	{
		ummap_set_generation_lsn(state->filectx, reln->smgr_rlocator,
								 generation_lsn);
		ummap_activate_main_slot0(state->filectx, reln->smgr_rlocator,
								 generation_lsn);
		state->main_slot0_active = true;
		state->generation_lsn = generation_lsn;
	}
}

void
umredocreate(SMgrRelation reln, ForkNumber forknum,
				 XLogRecPtr generation_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;
	UmbraRedoGenerationState *redo_state;
	bool		empty_storage = true;
	bool		root_matches_generation;

	Assert(InRecovery);
	Assert(state != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	if (UmbraAuxiliaryForkUsesSlot0(forknum))
	{
		if (state->main_slot0_active &&
			!RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		{
			ummap_activate_aux_slot0(state->filectx, forknum,
										 reln->smgr_rlocator);
			if (forknum == FSM_FORKNUM)
				state->fsm_slot0_active = true;
			else
				state->vm_slot0_active = true;
		}
		return;
	}
	if (forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return;

	ctx = state->filectx;
	state->uses_map = true;
	state->map_policy_probe_done = true;
	if (!XLogRecPtrIsValid(state->generation_lsn))
	{
		if (ummap_exists(ctx))
			empty_storage = ummap_is_empty(ctx, reln->smgr_rlocator);
		for (int forkidx = 0; empty_storage && forkidx <= MAX_FORKNUM;
			 forkidx++)
		{
			ForkNumber	fork = (ForkNumber) forkidx;

			if (umfile_exists(ctx, fork) &&
				umfile_nblocks(ctx, fork) != 0)
				empty_storage = false;
		}
	}

	/* An authoritative CREATE never inherits another generation's files. */
	if ((XLogRecPtrIsValid(state->generation_lsn) &&
			 state->generation_lsn != generation_lsn) ||
		(!XLogRecPtrIsValid(state->generation_lsn) && !empty_storage))
		um_reset_recovery_storage(reln);

	root_matches_generation = XLogRecPtrIsValid(state->generation_lsn) &&
		state->generation_lsn == generation_lsn;
	if (!root_matches_generation)
	{
		/* CREATE redo is the one path allowed to repair a damaged root. */
		ummap_create(ctx, reln->smgr_rlocator, true);
		ummap_set_generation_lsn(ctx, reln->smgr_rlocator, generation_lsn);
		ummap_activate_main_slot0(ctx, reln->smgr_rlocator, generation_lsn);
		state->main_slot0_active = true;
		state->fsm_slot0_active = false;
		state->vm_slot0_active = false;
		state->generation_lsn = generation_lsn;
	}

	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 true);
	redo_state->discard_precreate_redo = false;
}

void
umprepareredo(SMgrRelation reln, XLogRecPtr replay_lsn)
{
	UmbraRedoGenerationState *redo_state;

	if (!InRecovery)
		return;
	Assert(XLogRecPtrIsValid(replay_lsn));
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	if (redo_state != NULL && redo_state->discard_precreate_redo)
		return;
	if (umredogenerationahead(reln, replay_lsn))
	{
		um_reset_recovery_storage(reln);
		/* Missing old-generation pages now wait for DROP or TRUNCATE WAL. */
		redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 true);
		redo_state->discard_precreate_redo = true;
	}
}

bool
umredogenerationahead(SMgrRelation reln, XLogRecPtr replay_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraRedoGenerationState *redo_state;
	XLogRecPtr	generation_lsn;

	if (!InRecovery || state == NULL || !state->uses_map ||
		!XLogRecPtrIsValid(replay_lsn))
		return false;
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	if (redo_state != NULL && redo_state->discard_precreate_redo)
		return true;
	if (!XLogRecPtrIsValid(state->generation_lsn))
	{
		if (!ummap_try_get_generation_lsn(state->filectx,
										reln->smgr_rlocator, &generation_lsn) ||
			!XLogRecPtrIsValid(generation_lsn))
			return false;
		state->generation_lsn = generation_lsn;
	}
	return replay_lsn < state->generation_lsn;
}

bool
UmRedoDiscardingPrecreateRecords(SMgrRelation reln)
{
	UmbraRedoGenerationState *redo_state;

	if (!InRecovery || reln == NULL)
		return false;
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	return redo_state != NULL && redo_state->discard_precreate_redo;
}

void
umcheckpoint(void)
{
	ummap_checkpoint();
}

bool
UmWalOwnedSlotShiftAvailable(SMgrRelation reln, ForkNumber forknum)
{
	return reln != NULL && !InRecovery &&
		!RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
		um_main_uses_slot0(reln, forknum);
}

bool
UmPrepareSlotShift(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber logical_block, UmbraSlotShift *shift)
{
	MapSlotShift map_shift;

	Assert(shift != NULL);
	MemSet(shift, 0, sizeof(*shift));
	shift->map_slot_id = -1;
	if (!UmWalOwnedSlotShiftAvailable(reln, forknum) ||
		!MapPrepareSlotShift(um_get_filectx(reln), reln->smgr_rlocator,
						 logical_block, &map_shift))
		return false;

	shift->rlocator = map_shift.rlocator;
	shift->logical_block = map_shift.logical_block;
	shift->source_slot = map_shift.source_slot;
	shift->target_slot = map_shift.target_slot;
	shift->map_slot_id = map_shift.map_slot_id;
	shift->prepared = map_shift.prepared;
	return true;
}

void
UmAbortSlotShift(UmbraSlotShift *shift)
{
	MapSlotShift map_shift;

	Assert(shift != NULL);
	if (!shift->prepared)
		return;
	map_shift.rlocator = shift->rlocator;
	map_shift.logical_block = shift->logical_block;
	map_shift.source_slot = shift->source_slot;
	map_shift.target_slot = shift->target_slot;
	map_shift.map_slot_id = shift->map_slot_id;
	map_shift.prepared = shift->prepared;
	MapAbortSlotShift(&map_shift);
	shift->prepared = false;
	shift->map_slot_id = -1;
}

void
UmReleaseSlotShiftOnExit(UmbraSlotShift *shift)
{
	if (shift != NULL && shift->prepared)
		UmAbortSlotShift(shift);
}

void
UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn)
{
	MapSlotShift map_shift;

	Assert(shift != NULL);
	Assert(shift->prepared);
	map_shift.rlocator = shift->rlocator;
	map_shift.logical_block = shift->logical_block;
	map_shift.source_slot = shift->source_slot;
	map_shift.target_slot = shift->target_slot;
	map_shift.map_slot_id = shift->map_slot_id;
	map_shift.prepared = shift->prepared;
	MapPublishSlotShift(&map_shift, lsn);
	shift->prepared = false;
	shift->map_slot_id = -1;
}

bool
UmCheckpointWriteSourceSlot(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_block, uint8 source_slot,
							const void *buffer)
{
	const void *buffers[1] = {buffer};
	BlockNumber	physical_block;

	if (reln == NULL || buffer == NULL ||
		!UmbraMainActiveSlotIsValid(source_slot))
		return false;
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	if (!um_main_uses_slot0(reln, forknum))
		return false;
	if (!UmbraMainActiveSlotPhysicalBlock(logical_block, source_slot,
										 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN source-slot block mapping overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));

	umfile_writev(um_get_filectx(reln), MAIN_FORKNUM, physical_block,
				  buffers, 1, false);
	return true;
}

void
UmCheckpointWritebackSourceSlot(SMgrRelation reln, ForkNumber forknum,
								BlockNumber logical_block, uint8 source_slot)
{
	BlockNumber	physical_block;

	if (reln == NULL || !UmbraMainActiveSlotIsValid(source_slot))
		return;
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	if (!um_main_uses_slot0(reln, forknum))
		return;
	if (!UmbraMainActiveSlotPhysicalBlock(logical_block, source_slot,
										 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN source-slot block mapping overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));

	umfile_writeback(um_get_filectx(reln), MAIN_FORKNUM, physical_block, 1);
}

void
UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
					BlockNumber logical_block, uint8 active_slot)
{
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	if (reln == NULL || !InRecovery ||
		!UmbraMainActiveSlotIsValid(active_slot) ||
		!um_main_uses_slot0(reln, forknum))
		elog(PANIC, "Umbra redo targets an inactive MAIN mapping");
	MapRedoSetActiveSlot(um_get_filectx(reln), reln->smgr_rlocator,
					 logical_block, active_slot);
}

void
UmRedoSlotShift(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, uint8 source_slot,
				uint8 target_slot)
{
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	if (!InRecovery || !um_main_uses_slot0(reln, forknum))
		elog(PANIC, "Umbra slot-shift WAL targets an inactive MAIN mapping");
	MapRedoSlotShift(um_get_filectx(reln), reln->smgr_rlocator,
					 logical_block, source_slot, target_slot);
}

void
umflushdatabasetablespace(Oid dbid, Oid spcOid)
{
	ummap_flush_database_tablespace(dbid, spcOid);
}

void
uminvalidatedatabase(Oid dbid)
{
	um_forget_redo_generation_database(dbid, InvalidOid);
	ummap_invalidate_database(dbid);
}

void
uminvalidatedatabasetablespace(Oid dbid, Oid spcOid)
{
	um_forget_redo_generation_database(dbid, spcOid);
	ummap_invalidate_database_tablespace(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;

	um_refresh_slot0_policy(reln, forknum);
	ctx = um_get_filectx(reln);

	/*
	 * A durable auxiliary root declares a logical fork even if crash recovery
	 * has not recreated its physical file yet.  In particular, truncate redo
	 * must retain that fork in its prepared list so it can lower its frontier.
	 */
	if (InRecovery && UmbraAuxiliaryForkUsesSlot0(forknum) &&
		um_fork_uses_slot0(reln, forknum))
		return true;
	return umfile_exists(ctx, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
	{
		um_forget_redo_generation_state(rlocator.locator);
		ummap_unlink(rlocator, isRedo);
	}
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	physical_block;
	BlockNumber	logical_end;
	const void *buffers[1];

	um_refresh_slot0_policy(reln, forknum);
	if (!um_fork_uses_slot0(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		return;
	}

	um_get_slot0_frontiers(reln, forknum, &logical_eof, &physical_capacity);
	physical_block = um_slot0_active_pblk(reln, forknum, blocknum);
	buffers[0] = buffer;
	if (blocknum < logical_eof)
	{
		umfile_writev(um_get_filectx(reln), forknum, physical_block,
					  buffers, 1, skipFsync);
		return;
	}

	logical_end = um_slot0_range_end(reln, forknum, blocknum, 1);
	if (!UmbraSlot0PhysicalCapacity(logical_end, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot-0 capacity overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));

	ctx = um_get_filectx(reln);
	um_ensure_slot0_capacity(reln, forknum, physical_capacity, skipFsync);
	if (blocknum > logical_eof)
		um_zero_slot0_range(reln, forknum, logical_eof, blocknum, skipFsync);
	umfile_writev(ctx, forknum, physical_block, buffers, 1, skipFsync);
	um_publish_slot0_after_data(reln, forknum, logical_end, physical_capacity);
	if (forknum == MAIN_FORKNUM)
		um_ensure_main_selector_pages(reln, logical_eof,
									 logical_end - logical_eof, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	um_refresh_slot0_policy(reln, forknum);
	if (!um_fork_uses_slot0(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
		return;
	}
	if (nblocks <= 0)
		return;

	logical_end = um_slot0_range_end(reln, forknum, blocknum,
									 (BlockNumber) nblocks);
	um_get_slot0_frontiers(reln, forknum, &logical_eof, &physical_capacity);
	if (logical_end <= logical_eof)
		return;
	if (!UmbraSlot0PhysicalCapacity(logical_end, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot-0 capacity overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));

	um_ensure_slot0_capacity(reln, forknum, physical_capacity, skipFsync);
	/* A regrown page can still occupy retained slot-0 capacity after truncate. */
	um_zero_slot0_range(reln, forknum, logical_eof, logical_end, skipFsync);
	um_publish_slot0_after_data(reln, forknum, logical_end, physical_capacity);
	if (forknum == MAIN_FORKNUM)
		um_ensure_main_selector_pages(reln, logical_eof,
									 logical_end - logical_eof, skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	um_refresh_slot0_policy(reln, forknum);
	if (um_fork_uses_slot0(reln, forknum))
		return true;
	return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	um_refresh_slot0_policy(reln, forknum);
	if (um_fork_uses_slot0(reln, forknum))
		return 1;
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	UmbraFileContext *ctx;

	um_refresh_slot0_policy(reln, forknum);
	if (!um_fork_uses_slot0(reln, forknum))
	{
		umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
		return;
	}
	if (nblocks == 0)
		return;

	if (InRecovery && UmbraAuxiliaryForkUsesSlot0(forknum))
		um_ensure_aux_slot0_recovery_capacity(reln, forknum);
	um_require_slot0_range(reln, forknum, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber physical_block =
			um_slot0_active_pblk(reln, forknum, blocknum + i);

		umfile_readv(ctx, forknum, physical_block, &buffers[i], 1);
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	PgAioTargetData *target;
	BlockNumber	physical_block;

	um_refresh_slot0_policy(reln, forknum);
	if (!um_fork_uses_slot0(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
		pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
		umfile_startreadv(ioh, um_get_filectx(reln), forknum, blocknum, buffers,
						  nblocks);
		return;
	}
	if (nblocks != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Umbra slot-0 AIO requires a single logical block")));

	if (InRecovery && UmbraAuxiliaryForkUsesSlot0(forknum))
		um_ensure_aux_slot0_recovery_capacity(reln, forknum);
	um_require_slot0_range(reln, forknum, blocknum, nblocks);
	physical_block = um_slot0_active_pblk(reln, forknum, blocknum);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	target = pgaio_io_get_target_data(ioh);
	target->smgr.physicalBlockNum = physical_block;
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), forknum, physical_block,
					  buffers,
					  nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	UmbraFileContext *ctx;

	um_refresh_slot0_policy(reln, forknum);
	if (!um_fork_uses_slot0(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
					  skipFsync);
		return;
	}
	if (nblocks == 0)
		return;

	um_require_slot0_range(reln, forknum, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber physical_block =
			um_slot0_active_pblk(reln, forknum, blocknum + i);

		umfile_writev(ctx, forknum, physical_block, &buffers[i], 1,
					  skipFsync);
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	um_refresh_slot0_policy(reln, forknum);
	if (um_fork_uses_slot0(reln, forknum))
		return;
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;

	um_refresh_slot0_policy(reln, forknum);
	if (um_fork_uses_slot0(reln, forknum))
	{
		um_get_slot0_frontiers(reln, forknum, &logical_eof,
							  &physical_capacity);
		return logical_eof;
	}
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraSlot0TruncateState *truncate;
	UmbraFileContext *ctx;

	if (!um_fork_uses_slot0(reln, forknum))
	{
		umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
		return;
	}
	truncate = &state->slot0_truncate[forknum];

	/*
	 * Redo can find a lower root that was persisted just before a crash but a
	 * still-longer slot-0 file.  A prepared physical cleanup is required even
	 * when the logical truncation is already a no-op.
	 */
	if (!truncate->prepared)
		return;

	/* Persist the target root before removing physical tail blocks. */
	um_publish_slot0_before_truncate(reln, forknum, truncate->logical_eof,
										  truncate->physical_capacity);
	ctx = um_get_filectx(reln);
	umfile_truncate(ctx, forknum, truncate->current_physical,
					truncate->physical_capacity);
	truncate->prepared = false;
	(void) old_blocks;
}

void
umpreparetruncate(SMgrRelation reln, ForkNumber *forknum, int nforks,
				  BlockNumber *old_blocks, BlockNumber *nblocks)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;
	BlockNumber	root_logical_eof;
	BlockNumber	root_physical_capacity;
	BlockNumber	target_logical_eof;
	BlockNumber	target_physical_capacity;
	BlockNumber	current_physical;

	Assert(state != NULL);
	Assert(old_blocks != NULL);
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	um_refresh_slot0_policy(reln, FSM_FORKNUM);
	um_refresh_slot0_policy(reln, VISIBILITYMAP_FORKNUM);
	for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
		state->slot0_truncate[forkidx].prepared = false;
	/*
	 * Root publication orders every activated fork, including an auxiliary fork
	 * not selected by this truncate record.  Recovery must recreate any missing
	 * physical fork before root preparation opens and syncs those descriptors.
	 */
	if (InRecovery)
	{
		if (um_fork_uses_slot0(reln, FSM_FORKNUM))
			um_ensure_aux_slot0_recovery_capacity(reln, FSM_FORKNUM);
		if (um_fork_uses_slot0(reln, VISIBILITYMAP_FORKNUM))
			um_ensure_aux_slot0_recovery_capacity(reln,
										VISIBILITYMAP_FORKNUM);
	}
	for (int i = 0; i < nforks; i++)
	{
		UmbraSlot0TruncateState *truncate;

		if (!um_fork_uses_slot0(reln, forknum[i]))
			continue;

		um_get_slot0_frontiers(reln, forknum[i], &root_logical_eof,
								  &root_physical_capacity);
		target_logical_eof = Min(nblocks[i], root_logical_eof);
		if (!UmbraSlot0PhysicalCapacity(target_logical_eof,
									 &target_physical_capacity))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra slot-0 truncate frontier overflow")));

		ctx = um_get_filectx(reln);
		if (forknum[i] == MAIN_FORKNUM)
			ummap_prepare_main_frontiers(ctx, reln->smgr_rlocator);
		else
			ummap_prepare_aux_frontiers(ctx, forknum[i],
									  reln->smgr_rlocator);
		current_physical = umfile_nblocks(ctx, forknum[i]);
		if (current_physical < root_physical_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Umbra slot-0 physical capacity precedes its metadata root")));
		if (target_logical_eof == root_logical_eof &&
			current_physical == target_physical_capacity)
			continue;

		truncate = &state->slot0_truncate[forknum[i]];
		truncate->logical_eof = target_logical_eof;
		truncate->physical_capacity = target_physical_capacity;
		truncate->current_physical = current_physical;
		truncate->prepared = true;
	}
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;

	um_refresh_slot0_policy(reln, forknum);
	ctx = um_get_filectx(reln);
	umfile_immedsync(ctx, forknum);
	if (um_fork_uses_slot0(reln, forknum))
		ummap_immedsync_if_exists(ctx, reln->smgr_rlocator);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;

	um_refresh_slot0_policy(reln, forknum);
	ctx = um_get_filectx(reln);
	umfile_registersync(ctx, forknum);
	if (um_fork_uses_slot0(reln, forknum))
		ummap_registersync_if_exists(ctx, reln->smgr_rlocator);
}

bool
umpreparependingsync(SMgrRelation reln)
{
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	um_refresh_slot0_policy(reln, FSM_FORKNUM);
	um_refresh_slot0_policy(reln, VISIBILITYMAP_FORKNUM);
	return um_fork_uses_slot0(reln, MAIN_FORKNUM) ||
		um_fork_uses_slot0(reln, FSM_FORKNUM) ||
		um_fork_uses_slot0(reln, VISIBILITYMAP_FORKNUM);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/* AIO reopen passes the physical block stored in its target data. */
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

static bool
um_main_uses_slot0(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	return forknum == MAIN_FORKNUM && state != NULL &&
		state->main_slot0_active;
}

/* Refresh one cached policy bit only from paths that may perform metadata I/O. */
static void
um_refresh_slot0_policy(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;
	bool		active = false;

	if (reln == NULL || RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!IsUnderPostmaster || IsBootstrapProcessingMode() ||
		IsInitProcessingMode() ||
		(forknum != MAIN_FORKNUM && forknum != FSM_FORKNUM &&
		 forknum != VISIBILITYMAP_FORKNUM))
		return;
	Assert(CritSectionCount == 0);
	state = reln->smgr_private;
	if (state == NULL || state->filectx == NULL)
		return;
	if (!state->uses_map)
	{
		if (state->map_policy_probe_done)
			return;
		state->map_policy_probe_done = true;
		if (!ummap_exists(state->filectx))
			return;
		state->uses_map = true;
	}

	if (forknum == MAIN_FORKNUM)
	{
		if (state->main_slot0_active)
			return;
		if (InRecovery)
		{
			if (!ummap_try_main_slot0_active(state->filectx,
										  reln->smgr_rlocator, &active) || !active)
				return;
		}
		else if (!ummap_main_slot0_active(state->filectx,
									 reln->smgr_rlocator))
			return;
		state->main_slot0_active = true;
		reln->smgr_cached_nblocks[MAIN_FORKNUM] = InvalidBlockNumber;
		return;
	}

	/* Auxiliary mapping never exists before the MAIN root policy. */
	um_refresh_slot0_policy(reln, MAIN_FORKNUM);
	if (!state->main_slot0_active)
		return;
	if (forknum == FSM_FORKNUM)
	{
		if (state->fsm_slot0_active)
			return;
		if (InRecovery)
		{
			if (!ummap_try_aux_slot0_active(state->filectx, FSM_FORKNUM,
										 reln->smgr_rlocator, &active) || !active)
				return;
		}
		else if (!ummap_aux_slot0_active(state->filectx, FSM_FORKNUM,
										reln->smgr_rlocator))
			return;
		state->fsm_slot0_active = true;
		reln->smgr_cached_nblocks[FSM_FORKNUM] = InvalidBlockNumber;
		return;
	}

	if (state->vm_slot0_active)
		return;
	if (InRecovery)
	{
		if (!ummap_try_aux_slot0_active(state->filectx, VISIBILITYMAP_FORKNUM,
									 reln->smgr_rlocator, &active) || !active)
			return;
	}
	else if (!ummap_aux_slot0_active(state->filectx, VISIBILITYMAP_FORKNUM,
									reln->smgr_rlocator))
		return;
	state->vm_slot0_active = true;
	reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = InvalidBlockNumber;
}

static bool
um_fork_uses_slot0(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL)
		return false;
	if (forknum == MAIN_FORKNUM)
		return state->main_slot0_active;
	if (forknum == FSM_FORKNUM)
		return state->fsm_slot0_active;
	if (forknum == VISIBILITYMAP_FORKNUM)
		return state->vm_slot0_active;
	return false;
}

static BlockNumber
um_main_active_pblk(SMgrRelation reln, BlockNumber logical_block)
{
	BlockNumber	physical_block;
	uint8		active_slot;

	active_slot = MapGetActiveSlot(um_get_filectx(reln), reln->smgr_rlocator,
							   logical_block);
	if (!UmbraMainActiveSlotPhysicalBlock(logical_block, active_slot,
										 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN active-slot block mapping overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));
	return physical_block;
}

static BlockNumber
um_slot0_active_pblk(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber logical_block)
{
	BlockNumber	physical_block;

	if (forknum == MAIN_FORKNUM)
		return um_main_active_pblk(reln, logical_block);
	if (!UmbraSlot0PhysicalBlock(logical_block, &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot-0 block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return physical_block;
}

static BlockNumber
um_slot0_range_end(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber blocknum, BlockNumber nblocks)
{
	uint64		end = (uint64) blocknum + nblocks;

	if (end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot-0 logical block range overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return (BlockNumber) end;
}

static void
um_get_slot0_frontiers(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber *logical_eof,
					   BlockNumber *physical_capacity)
{
	if (forknum == MAIN_FORKNUM)
		ummap_get_main_frontiers(um_get_filectx(reln), reln->smgr_rlocator,
								 logical_eof, physical_capacity);
	else
		ummap_get_aux_frontiers(um_get_filectx(reln), forknum,
								  reln->smgr_rlocator, logical_eof,
								  physical_capacity);
}

static void
um_require_slot0_range(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	logical_end = um_slot0_range_end(reln, forknum, blocknum, nblocks);
	um_get_slot0_frontiers(reln, forknum, &logical_eof, &physical_capacity);
	if (logical_end > logical_eof)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not access logical block %u beyond Umbra slot-0 EOF %u for fork %d",
						blocknum, logical_eof, (int) forknum)));
}

static void
um_ensure_slot0_capacity(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber physical_capacity, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	current_physical;

	current_physical = umfile_nblocks(ctx, forknum);
	while (current_physical < physical_capacity)
	{
		uint64		remaining = (uint64) physical_capacity - current_physical;
		int			extend_by = (int) Min(remaining, (uint64) INT_MAX);

		umfile_zeroextend(ctx, forknum, current_physical, extend_by,
						  skipFsync);
		current_physical += (BlockNumber) extend_by;
	}
}

/*
 * Auxiliary redo can find a durable root whose mapped physical fork was lost
 * in the same crash.  RBM_ZERO_ON_ERROR callers still enter AIO through the
 * physical slot-0 address, so make the root's declared capacity readable
 * before dispatching that I/O.
 */
static void
um_ensure_aux_slot0_recovery_capacity(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;

	Assert(InRecovery);
	Assert(UmbraAuxiliaryForkUsesSlot0(forknum));
	/* A truncate record can arrive before any page redo recreates this fork. */
	if (!umfile_exists(ctx, forknum))
		umfile_create(ctx, forknum, true);
	um_get_slot0_frontiers(reln, forknum, &logical_eof, &physical_capacity);
	um_ensure_slot0_capacity(reln, forknum, physical_capacity, true);
}

static void
um_zero_slot0_range(SMgrRelation reln, ForkNumber forknum,
				BlockNumber first_block, BlockNumber end_block, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	PGIOAlignedBlock zero_page = {0};
	const void *buffers[1] = {zero_page.data};

	for (BlockNumber logical_block = first_block;
		 logical_block < end_block;
		 logical_block++)
	{
		BlockNumber physical_block =
			um_slot0_active_pblk(reln, forknum, logical_block);

		umfile_writev(ctx, forknum, physical_block, buffers, 1,
					  skipFsync);
	}
}

/* Publish only after the full three-slot capacity has been materialized. */
static void
um_publish_slot0_after_data(SMgrRelation reln, ForkNumber forknum,
						BlockNumber logical_eof, BlockNumber physical_capacity)
{
	/*
	 * Root writeback flushes its WAL dependency and syncs each activated fork
	 * before the root. The skip-WAL pending-sync path also calls umimmedsync(),
	 * which preserves the same order. Avoid an immediate fsync per extension.
	 */
	if (forknum == MAIN_FORKNUM)
		ummap_set_main_frontiers(um_get_filectx(reln), reln->smgr_rlocator,
								  logical_eof, physical_capacity);
	else
		ummap_set_aux_frontiers(um_get_filectx(reln), forknum,
								 reln->smgr_rlocator, logical_eof,
								 physical_capacity);
}

/* A lower root is safe before the physical tail is removed. */
static void
um_publish_slot0_before_truncate(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber logical_eof,
									  BlockNumber physical_capacity)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (forknum == MAIN_FORKNUM)
		ummap_publish_prepared_main_frontiers(ctx, reln->smgr_rlocator,
											 logical_eof, physical_capacity);
	else
		ummap_publish_prepared_aux_frontiers(ctx, forknum,
										  reln->smgr_rlocator, logical_eof,
										  physical_capacity);
}

/* Extension is the non-critical path that seeds selector pages for WAL shifts. */
static void
um_ensure_main_selector_pages(SMgrRelation reln, BlockNumber first_block,
								  BlockNumber nblocks, bool skipFsync)
{
	if (nblocks == 0 || CritSectionCount != 0 || !IsUnderPostmaster ||
		IsBootstrapProcessingMode() || IsInitProcessingMode())
		return;
	MapEnsureActiveSlotPages(um_get_filectx(reln), reln->smgr_rlocator,
						  first_block, nblocks, skipFsync);
}

/*
 * A second recovery can start before an old generation's DROP while a newer
 * generation already occupies the same locator.  Replace that storage before
 * replay reads an old page through it.
 */
static void
um_reset_recovery_storage(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;
	SMgrRelation rels[1] = {reln};

	Assert(InRecovery);
	Assert(state != NULL);
	ctx = state->filectx;

	/* Drop shared buffers and stale descriptors before locator reuse. */
	smgrdounlinkall(rels, lengthof(rels), true);
	if (!umfile_unlink(reln->smgr_rlocator, InvalidForkNumber, true))
		elog(PANIC,
			 "could not remove old Umbra storage generation for relation %u/%u/%u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber);
	umfile_create(ctx, MAIN_FORKNUM, true);
	ummap_create(ctx, reln->smgr_rlocator, true);

	state->uses_map = true;
	state->map_policy_probe_done = true;
	state->main_slot0_active = false;
	state->fsm_slot0_active = false;
	state->vm_slot0_active = false;
	state->generation_lsn = InvalidXLogRecPtr;
	for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
		reln->smgr_cached_nblocks[forkidx] = InvalidBlockNumber;
	reln->smgr_targblock = InvalidBlockNumber;
}

static UmbraRedoGenerationState *
um_get_redo_generation_state(RelFileLocator rlocator, bool create)
{
	UmbraRedoGenerationState *entry;
	bool		found;

	if (um_redo_generation_hash == NULL)
	{
		HASHCTL		ctl;

		if (!create)
			return NULL;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileLocator);
		ctl.entrysize = sizeof(UmbraRedoGenerationState);
		ctl.hcxt = TopMemoryContext;
		um_redo_generation_hash =
			hash_create("Umbra redo generation state", 128, &ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	entry = hash_search(um_redo_generation_hash, &rlocator,
						create ? HASH_ENTER : HASH_FIND,
						create ? &found : NULL);
	if (create && !found)
		entry->discard_precreate_redo = false;
	return entry;
}

static void
um_forget_redo_generation_state(RelFileLocator rlocator)
{
	if (um_redo_generation_hash != NULL)
		(void) hash_search(um_redo_generation_hash, &rlocator, HASH_REMOVE,
							   NULL);
}

static void
um_forget_redo_generation_database(Oid dbid, Oid spcOid)
{
	HASH_SEQ_STATUS status;
	UmbraRedoGenerationState *entry;

	if (um_redo_generation_hash == NULL)
		return;

	hash_seq_init(&status, um_redo_generation_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->rlocator.dbOid != dbid ||
			(OidIsValid(spcOid) && entry->rlocator.spcOid != spcOid))
			continue;
		if (hash_search(um_redo_generation_hash, &entry->rlocator,
							HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "Umbra redo generation hash table corrupted");
	}
}

static UmbraFileContext *
um_get_filectx(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL || state->filectx == NULL)
	{
		umopen(reln);
		state = reln->smgr_private;
	}

	return state->filectx;
}
