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
#include "utils/memutils.h"

typedef struct UmbraMappedTruncateState
{
	bool		prepared;
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	current_physical;
} UmbraMappedTruncateState;

typedef struct UmbraMappedReadRun
{
	BlockNumber	physical_block;
	BlockNumber	nblocks;
	uint8		active_slot;
	bool		selector_page_present;
} UmbraMappedReadRun;

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
	/* Root-format activation bits, not the current per-page selector values. */
	bool		main_slot0_active;
	bool		fsm_slot0_active;
	bool		vm_slot0_active;
	UmbraMappedTruncateState mapped_truncate[MAX_FORKNUM + 1];
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static void um_refresh_mapping_policy(SMgrRelation reln, ForkNumber forknum);
static bool um_fork_uses_mapped_slots(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_active_pblk(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber logical_block);
static void um_get_mapped_read_run(SMgrRelation reln, ForkNumber forknum,
								   BlockNumber logical_block,
								   BlockNumber max_nblocks,
								   UmbraMappedReadRun *run);
static BlockNumber um_mapped_range_end(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber blocknum, BlockNumber nblocks);
static BlockNumber um_get_mapped_frontier(SMgrRelation reln,
										  ForkNumber forknum);
static BlockNumber um_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber logical_eof);
static bool um_resolve_slot_physical_block(SMgrRelation reln,
										   ForkNumber forknum,
										   BlockNumber logical_block,
										   uint8 slot,
										   BlockNumber *physical_block);
static void um_ensure_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber physical_capacity,
									  bool skipFsync);
static void um_ensure_aux_recovery_capacity(SMgrRelation reln,
											ForkNumber forknum);
static void um_zero_active_range(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber first_block, BlockNumber end_block,
								 bool skipFsync,
								 BlockNumber output_first_block,
								 BlockNumber *physical_blocks);
static void um_publish_mapped_after_data(SMgrRelation reln, ForkNumber forknum,
									BlockNumber logical_eof);
static void um_publish_mapped_before_truncate(SMgrRelation reln,
										  ForkNumber forknum,
										  BlockNumber logical_eof);
static void um_ensure_selector_pages(SMgrRelation reln, ForkNumber forknum,
									BlockNumber first_block,
									BlockNumber nblocks, bool skipFsync);

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
	}

	if (!InRecovery && state->uses_map)
	{
		ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
		state->main_slot0_active =
			ummap_main_slot0_active(state->filectx, reln->smgr_rlocator);
		state->fsm_slot0_active = ummap_aux_slot0_active(state->filectx,
			FSM_FORKNUM, reln->smgr_rlocator);
		state->vm_slot0_active = ummap_aux_slot0_active(state->filectx,
			VISIBILITYMAP_FORKNUM, reln->smgr_rlocator);
	}
	else if (InRecovery && state->uses_map)
	{
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
			for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
				state->mapped_truncate[forkidx].prepared = false;
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
	um_refresh_mapping_policy(reln, forknum);
	/*
	 * Ordinary page redo marks a permanent relation as map-capable before an
	 * authoritative CREATE has established its root.  Only a relation whose
	 * MAIN fork has already entered mapped policy may activate a mapped
	 * auxiliary fork; bootstrap system relations have no _map fork at all.
	 */
	map_auxiliary = state->main_slot0_active &&
		UmbraIsMappedAuxiliaryFork(forknum);
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
		/* Defensive page redo must not reset an existing metadata root. */
		state->uses_map = true;
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
		if (isRedo && active)
			um_ensure_aux_recovery_capacity(reln, forknum);
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
	if (state->uses_map)
		ummap_create(um_get_filectx(reln), reln->smgr_rlocator, false);
}

void
umfinishcreate(SMgrRelation reln, ForkNumber forknum, XLogRecPtr create_lsn)
{
	UmbraSmgrRelationState *state;
	UmbraFileContext *ctx;

	if (forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return;

	ctx = um_get_filectx(reln);
	state = reln->smgr_private;
	Assert(state != NULL);
	if (!InRecovery && !state->uses_map)
		return;

	/*
	 * Normal creation already bootstrapped the root before inserting CREATE
	 * WAL.  Redo must create or repair it here because its physical files can
	 * be absent when the CREATE record is replayed.
	 */
	if (InRecovery)
	{
		state->uses_map = true;
		state->map_policy_probe_done = true;
		ummap_create(ctx, reln->smgr_rlocator, true);
	}
	else
		Assert(state->uses_map);
	ummap_activate_main_slot0(ctx, reln->smgr_rlocator, create_lsn);
	state->main_slot0_active = true;
}

void
umcheckpoint(void)
{
	ummap_checkpoint();
}

bool
UmChooseSlotShift(SMgrRelation reln, ForkNumber forknum,
				  BlockNumber logical_block, uint8 cached_active_slot,
				  bool selector_page_present,
				  UmbraSlotShift *shift)
{
	UmbraSmgrRelationState *state;
	MapSlotShift map_shift;
	uint8		map_source_slot;
	uint8		map_target_slot;

	Assert(shift != NULL);
	MemSet(shift, 0, sizeof(*shift));
	shift->map_slot_id = -1;
	if (reln == NULL || InRecovery ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		IsBootstrapProcessingMode() || IsInitProcessingMode() ||
		!UmbraForkUsesActiveSlots(forknum) ||
		!UmbraActiveSlotIsValid(cached_active_slot))
		return false;

	state = reln->smgr_private;
	if (state == NULL || state->filectx == NULL ||
		!um_fork_uses_mapped_slots(reln, forknum))
		return false;
	if (!selector_page_present)
	{
		if (cached_active_slot != 0)
			elog(ERROR, "Umbra nonzero active slot has no selector page");
	}

	if (!MapPrepareSlotShift(reln->smgr_rlocator, forknum, logical_block,
							 cached_active_slot, &map_shift))
	{
		if (CritSectionCount != 0)
			return false;

		/* Load or materialize the selector before entering WAL publication. */
		MapChooseSlotShift(state->filectx, reln->smgr_rlocator, forknum,
						   logical_block, &map_source_slot, &map_target_slot);
		if (map_source_slot != cached_active_slot ||
			map_target_slot != UmbraNextActiveSlot(cached_active_slot))
			elog(ERROR, "Umbra cached active slot disagrees with its selector");
		if (!MapPrepareSlotShift(reln->smgr_rlocator, forknum, logical_block,
							 cached_active_slot, &map_shift))
			return false;
	}

	shift->rlocator = map_shift.rlocator;
	shift->forknum = map_shift.forknum;
	shift->logical_block = map_shift.logical_block;
	shift->source_slot = map_shift.source_slot;
	shift->target_slot = map_shift.target_slot;
	shift->map_slot_id = map_shift.map_slot_id;
	shift->selected = map_shift.prepared;
	return true;
}

void
UmAbortSlotShift(UmbraSlotShift *shift)
{
	MapSlotShift map_shift;

	Assert(shift != NULL);
	if (!shift->selected)
		return;
	map_shift.rlocator = shift->rlocator;
	map_shift.forknum = shift->forknum;
	map_shift.logical_block = shift->logical_block;
	map_shift.source_slot = shift->source_slot;
	map_shift.target_slot = shift->target_slot;
	map_shift.map_slot_id = shift->map_slot_id;
	map_shift.prepared = shift->selected;
	MapAbortSlotShift(&map_shift);
	shift->selected = false;
	shift->map_slot_id = -1;
}

void
UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn)
{
	MapSlotShift map_shift;

	Assert(shift != NULL);
	Assert(shift->selected);
	map_shift.rlocator = shift->rlocator;
	map_shift.forknum = shift->forknum;
	map_shift.logical_block = shift->logical_block;
	map_shift.source_slot = shift->source_slot;
	map_shift.target_slot = shift->target_slot;
	map_shift.map_slot_id = shift->map_slot_id;
	map_shift.prepared = shift->selected;
	MapPublishSlotShift(&map_shift, lsn);
	shift->selected = false;
	shift->map_slot_id = -1;
}

bool
UmUsesMappedSlots(SMgrRelation reln, ForkNumber forknum)
{
	if (reln == NULL)
		return false;
	um_refresh_mapping_policy(reln, forknum);
	return um_fork_uses_mapped_slots(reln, forknum);
}

bool
UmGetActiveSlot(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, uint8 *active_slot,
				bool *selector_page_present)
{
	if (active_slot != NULL)
		*active_slot = UMBRA_ACTIVE_SLOT_INVALID;
	if (selector_page_present != NULL)
		*selector_page_present = false;
	if (reln == NULL || active_slot == NULL)
		return false;
	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
		return false;
	*active_slot = MapGetActiveSlotWithPresence(um_get_filectx(reln),
												reln->smgr_rlocator, forknum,
												logical_block,
												selector_page_present);
	return true;
}

static bool
um_resolve_slot_physical_block(SMgrRelation reln, ForkNumber forknum,
								BlockNumber logical_block, uint8 slot,
								BlockNumber *physical_block)
{
	if (physical_block != NULL)
		*physical_block = InvalidBlockNumber;
	if (reln == NULL || physical_block == NULL ||
		!UmbraActiveSlotIsValid(slot))
		return false;
	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
		return false;
	if (!UmbraActiveSlotPhysicalBlock(logical_block, slot, physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return true;
}

bool
UmWriteSlot(SMgrRelation reln, ForkNumber forknum,
			BlockNumber logical_block, const void *buffer,
			uint8 slot)
{
	const void *buffers[1] = {buffer};
	BlockNumber physical_block;

	if (buffer == NULL)
		return false;

	/* Match smgrwritev_with_targets() interrupt semantics. */
	HOLD_INTERRUPTS();
	if (!um_resolve_slot_physical_block(reln, forknum, logical_block, slot,
										 &physical_block))
	{
		RESUME_INTERRUPTS();
		return false;
	}

	umfile_writev(um_get_filectx(reln), forknum, physical_block,
				  buffers, 1, false);
	RESUME_INTERRUPTS();
	return true;
}

bool
UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
					BlockNumber logical_block, uint8 active_slot)
{
	UmbraSmgrRelationState *state;
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	physical_block;
	BlockNumber	physical_capacity;
	BlockNumber	required_eof;
	bool		root_active = false;

	if (!InRecovery || reln == NULL ||
		!UmbraForkUsesActiveSlots(forknum) ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!UmbraActiveSlotIsValid(active_slot))
		elog(PANIC, "Umbra redo targets an invalid active-slot mapping");

	um_refresh_mapping_policy(reln, forknum);
	state = reln->smgr_private;
	ctx = um_get_filectx(reln);
	Assert(state != NULL);

	/* Image-free MAIN redo must never create or repair a missing source. */
	if (forknum == MAIN_FORKNUM)
	{
		if (!ummap_try_main_slot0_active(ctx, reln->smgr_rlocator,
										&root_active) || !root_active)
			return false;
		state->main_slot0_active = true;
		state->uses_map = true;
		logical_eof = um_get_mapped_frontier(reln, forknum);
		physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
		if (logical_block >= logical_eof ||
			!umfile_exists(ctx, forknum) ||
			umfile_nblocks(ctx, forknum) < physical_capacity)
			return false;
	}
	else
	{
		/* MAIN root authority is required before auxiliary birth recovery. */
		if (!ummap_try_main_slot0_active(ctx, reln->smgr_rlocator,
										&root_active) || !root_active)
			return false;
		state->main_slot0_active = true;
		state->uses_map = true;

		/*
		 * The block reference proves only the minimum auxiliary frontier.  Do
		 * not materialize capacity until the recorded source slot is known to
		 * have survived the crash, or zero-extension could fabricate it.
		 */
		if (!UmbraActiveSlotPhysicalBlock(logical_block, active_slot,
										   &physical_block) ||
			!umfile_exists(ctx, forknum) ||
			umfile_nblocks(ctx, forknum) <= physical_block)
			return false;

		if (!ummap_try_aux_slot0_active(ctx, forknum, reln->smgr_rlocator,
									   &root_active))
			return false;
		if (!root_active)
			ummap_activate_aux_slot0(ctx, forknum, reln->smgr_rlocator);
		if (forknum == FSM_FORKNUM)
			state->fsm_slot0_active = true;
		else
			state->vm_slot0_active = true;

		logical_eof = um_get_mapped_frontier(reln, forknum);
		required_eof = um_mapped_range_end(reln, forknum, logical_block, 1);
		if (logical_eof < required_eof)
		{
			logical_eof = required_eof;
			physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
			um_ensure_mapped_capacity(reln, forknum, physical_capacity, true);
			um_publish_mapped_after_data(reln, forknum, logical_eof);
		}
		else
		{
			physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
			um_ensure_mapped_capacity(reln, forknum, physical_capacity, true);
		}
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
	}

	MapRedoSetActiveSlot(ctx, reln->smgr_rlocator, forknum, logical_block,
						 active_slot);
	return true;
}

bool
UmRedoSlotShift(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, uint8 source_slot,
				uint8 target_slot, XLogRecPtr shift_lsn)
{
	UmbraSmgrRelationState *state;
	UmbraFileContext *ctx;
	bool		root_active = false;

	if (!InRecovery || reln == NULL ||
		!UmbraForkUsesActiveSlots(forknum) ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!XLogRecPtrIsValid(shift_lsn))
		elog(PANIC, "Umbra slot-shift WAL targets an inactive mapping");
	um_refresh_mapping_policy(reln, forknum);
	state = reln->smgr_private;
	ctx = um_get_filectx(reln);
	Assert(state != NULL);

	/*
	 * A full-page image proves that the logical page can be reconstructed, but
	 * it does not identify the lifecycle that owned a missing metadata root.
	 * Only CREATE redo may rebuild that authority.  A later DROP or covering
	 * TRUNCATE will clear the dependency recorded by the caller.
	 */
	if (!ummap_try_main_slot0_active(ctx, reln->smgr_rlocator,
									&root_active) || !root_active)
		return false;

	state->uses_map = true;
	state->map_policy_probe_done = true;
	state->main_slot0_active = true;
	reln->smgr_cached_nblocks[MAIN_FORKNUM] = InvalidBlockNumber;

	/* With mapping authority established, the FPI may recreate its fork. */
	umfile_create(ctx, forknum, true);
	if (UmbraIsMappedAuxiliaryFork(forknum) &&
		!um_fork_uses_mapped_slots(reln, forknum))
	{
		ummap_activate_aux_slot0(ctx, forknum, reln->smgr_rlocator);
		if (forknum == FSM_FORKNUM)
			state->fsm_slot0_active = true;
		else
			state->vm_slot0_active = true;
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
	}

	MapRedoSlotShift(ctx, reln->smgr_rlocator, forknum,
					 logical_block, source_slot, target_slot);
	return true;
}

void
umflushdatabasetablespace(Oid dbid, Oid spcOid)
{
	ummap_flush_database_tablespace(dbid, spcOid);
}

void
uminvalidatedatabase(Oid dbid)
{
	ummap_invalidate_database(dbid);
}

void
uminvalidatedatabasetablespace(Oid dbid, Oid spcOid)
{
	ummap_invalidate_database_tablespace(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;

	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);

	/*
	 * A durable auxiliary root declares a logical fork even if crash recovery
	 * has not recreated its physical file yet.  In particular, truncate redo
	 * must retain that fork in its prepared list so it can lower its frontier.
	 */
	if (InRecovery && UmbraIsMappedAuxiliaryFork(forknum) &&
		um_fork_uses_mapped_slots(reln, forknum))
		return true;
	return umfile_exists(ctx, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
		ummap_unlink(rlocator, isRedo);
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync, BlockNumber *writeback_target)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	physical_block;
	BlockNumber	logical_end;
	const void *buffers[1];

	if (writeback_target != NULL)
		*writeback_target = InvalidBlockNumber;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		if (writeback_target != NULL)
			*writeback_target = blocknum;
		return;
	}

	logical_eof = um_get_mapped_frontier(reln, forknum);
	Assert(blocknum >= logical_eof);
	physical_block = um_active_pblk(reln, forknum, blocknum);
	buffers[0] = buffer;

	logical_end = um_mapped_range_end(reln, forknum, blocknum, 1);
	physical_capacity = um_mapped_capacity(reln, forknum, logical_end);

	ctx = um_get_filectx(reln);
	um_ensure_mapped_capacity(reln, forknum, physical_capacity, skipFsync);
	if (blocknum > logical_eof)
		um_zero_active_range(reln, forknum, logical_eof, blocknum, skipFsync,
							 InvalidBlockNumber, NULL);
	umfile_writev(ctx, forknum, physical_block, buffers, 1, skipFsync);
	if (writeback_target != NULL)
		*writeback_target = physical_block;
	um_publish_mapped_after_data(reln, forknum, logical_end);
	um_ensure_selector_pages(reln, forknum, logical_eof,
						 logical_end - logical_eof, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync, BlockNumber *physical_blocks)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	if (physical_blocks != NULL)
	{
		for (int i = 0; i < nblocks; i++)
			physical_blocks[i] = InvalidBlockNumber;
	}

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
		if (physical_blocks != NULL)
		{
			for (int i = 0; i < nblocks; i++)
				physical_blocks[i] = blocknum + i;
		}
		return;
	}
	if (nblocks <= 0)
		return;

	logical_end = um_mapped_range_end(reln, forknum, blocknum,
									 (BlockNumber) nblocks);
	logical_eof = um_get_mapped_frontier(reln, forknum);
	if (logical_end <= logical_eof)
		return;
	physical_capacity = um_mapped_capacity(reln, forknum, logical_end);

	um_ensure_mapped_capacity(reln, forknum, physical_capacity, skipFsync);
	/*
	 * Selector state is independent of logical EOF.  Truncate can retain a
	 * selector for slot 1 or 2, so initialize that selected physical page
	 * before publishing the larger EOF.  No selector-reset protocol is needed.
	 */
	um_zero_active_range(reln, forknum, logical_eof, logical_end, skipFsync,
						 blocknum, physical_blocks);
	um_publish_mapped_after_data(reln, forknum, logical_end);
	um_ensure_selector_pages(reln, forknum, logical_eof,
						 logical_end - logical_eof, skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
#ifdef USE_PREFETCH
	UmbraFileContext *ctx;
	int			remaining;
#endif

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
		return umfile_prefetch(um_get_filectx(reln), forknum, blocknum,
							   nblocks);
#ifdef USE_PREFETCH
	if (nblocks <= 0)
		return true;
	if ((uint64) blocknum + (uint64) nblocks >
		(uint64) MaxBlockNumber + 1)
		return false;

	ctx = um_get_filectx(reln);
	remaining = nblocks;
	while (remaining > 0)
	{
		UmbraMappedReadRun run;
		int			nblocks_this_run;

		um_get_mapped_read_run(reln, forknum, blocknum,
							   (BlockNumber) remaining, &run);
		nblocks_this_run = Min(remaining, (int) run.nblocks);
		if (!umfile_prefetch(ctx, forknum, run.physical_block,
							 nblocks_this_run))
			return false;
		blocknum += nblocks_this_run;
		remaining -= nblocks_this_run;
	}
#endif

	return true;
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	UmbraMappedReadRun run;

	um_refresh_mapping_policy(reln, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
	{
		um_get_mapped_read_run(reln, forknum, blocknum,
							   UMBRA_CHUNK_PAIRED_PAGES, &run);
		return run.nblocks;
	}
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	UmbraFileContext *ctx;
	UmbraMappedReadRun run;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
		return;
	}
	if (nblocks == 0)
		return;

	if (InRecovery && UmbraIsMappedAuxiliaryFork(forknum))
		um_ensure_aux_recovery_capacity(reln, forknum);
	ctx = um_get_filectx(reln);

	um_get_mapped_read_run(reln, forknum, blocknum, nblocks, &run);
	if (nblocks > run.nblocks)
		elog(ERROR, "Umbra mapped read crosses a physical run");
	umfile_readv(ctx, forknum, run.physical_block, buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	PgAioTargetData *target;
	UmbraMappedReadRun run;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
		pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
		umfile_startreadv(ioh, um_get_filectx(reln), forknum, blocknum,
						  buffers, nblocks);
		return;
	}
	if (nblocks == 0)
		elog(ERROR, "Umbra active-slot AIO requires at least one logical block");

	if (InRecovery && UmbraIsMappedAuxiliaryFork(forknum))
		um_ensure_aux_recovery_capacity(reln, forknum);
	um_get_mapped_read_run(reln, forknum, blocknum, nblocks, &run);
	if (nblocks > run.nblocks)
		elog(ERROR, "Umbra active-slot AIO crosses a physical run");
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	target = pgaio_io_get_target_data(ioh);
	target->smgr.physicalBlockNum = run.physical_block;
	target->smgr.umbraActiveSlot = run.active_slot;
	target->smgr.umbraSelectorPagePresent = run.selector_page_present;
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), forknum, run.physical_block,
						  buffers,
						  nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync,
		 BlockNumber *physical_blocks)
{
	UmbraFileContext *ctx;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
					  skipFsync);
		if (physical_blocks != NULL)
		{
			for (BlockNumber i = 0; i < nblocks; i++)
				physical_blocks[i] = blocknum + i;
		}
		return;
	}
	if (nblocks == 0)
		return;

	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber physical_block =
			um_active_pblk(reln, forknum, blocknum + i);

		umfile_writev(ctx, forknum, physical_block, &buffers[i], 1,
					  skipFsync);
		if (physical_blocks != NULL)
			physical_blocks[i] = physical_block;
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	um_refresh_mapping_policy(reln, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		return;
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

void
umwritebackphysical(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, BlockNumber nblocks)
{
	um_refresh_mapping_policy(reln, forknum);
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	um_refresh_mapping_policy(reln, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		return um_get_mapped_frontier(reln, forknum);
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraMappedTruncateState *truncate;
	UmbraFileContext *ctx;

	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
		return;
	}
	truncate = &state->mapped_truncate[forknum];

	/*
	 * Redo can find a lower root that was persisted just before a crash but a
	 * still-longer mapped file.  A prepared physical cleanup is required even
	 * when the logical truncation is already a no-op.
	 */
	if (!truncate->prepared)
		return;

	/* Persist the target root before removing physical tail blocks. */
	um_publish_mapped_before_truncate(reln, forknum, truncate->logical_eof);
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
	um_refresh_mapping_policy(reln, MAIN_FORKNUM);
	um_refresh_mapping_policy(reln, FSM_FORKNUM);
	um_refresh_mapping_policy(reln, VISIBILITYMAP_FORKNUM);
	for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
		state->mapped_truncate[forkidx].prepared = false;
	/*
	 * Root publication orders every activated fork, including an auxiliary fork
	 * not selected by this truncate record.  Recovery must recreate any missing
	 * physical fork before root preparation opens and syncs those descriptors.
	 */
	if (InRecovery)
	{
		if (um_fork_uses_mapped_slots(reln, FSM_FORKNUM))
			um_ensure_aux_recovery_capacity(reln, FSM_FORKNUM);
		if (um_fork_uses_mapped_slots(reln, VISIBILITYMAP_FORKNUM))
			um_ensure_aux_recovery_capacity(reln,
										VISIBILITYMAP_FORKNUM);
	}
	for (int i = 0; i < nforks; i++)
	{
		UmbraMappedTruncateState *truncate;

		if (!um_fork_uses_mapped_slots(reln, forknum[i]))
			continue;

		root_logical_eof = um_get_mapped_frontier(reln, forknum[i]);
		root_physical_capacity =
			um_mapped_capacity(reln, forknum[i], root_logical_eof);
		target_logical_eof = Min(nblocks[i], root_logical_eof);
		target_physical_capacity =
			um_mapped_capacity(reln, forknum[i], target_logical_eof);

		ctx = um_get_filectx(reln);
		if (forknum[i] == MAIN_FORKNUM)
			ummap_prepare_main_frontier(ctx, reln->smgr_rlocator);
		else
			ummap_prepare_aux_frontier(ctx, forknum[i],
									 reln->smgr_rlocator);
		current_physical = umfile_nblocks(ctx, forknum[i]);
		if (current_physical < root_physical_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Umbra mapped physical capacity precedes its metadata root")));
		if (target_logical_eof == root_logical_eof &&
			current_physical == target_physical_capacity)
			continue;

		truncate = &state->mapped_truncate[forknum[i]];
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

	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);
	umfile_immedsync(ctx, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		ummap_immedsync_if_exists(ctx, reln->smgr_rlocator);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;

	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);
	umfile_registersync(ctx, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		ummap_registersync_if_exists(ctx, reln->smgr_rlocator);
}

bool
umpreparependingsync(SMgrRelation reln)
{
	um_refresh_mapping_policy(reln, MAIN_FORKNUM);
	return um_fork_uses_mapped_slots(reln, MAIN_FORKNUM);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/* AIO reopen passes the physical block stored in its target data. */
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

/* Refresh one cached policy bit only from paths that may perform metadata I/O. */
static void
um_refresh_mapping_policy(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;
	bool		active = false;

	if (reln == NULL || RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		IsBootstrapProcessingMode() || IsInitProcessingMode() ||
		(forknum != MAIN_FORKNUM && forknum != FSM_FORKNUM &&
		 forknum != VISIBILITYMAP_FORKNUM))
		return;
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

	if ((forknum == FSM_FORKNUM && state->fsm_slot0_active) ||
		(forknum == VISIBILITYMAP_FORKNUM && state->vm_slot0_active))
	{
		Assert(state->main_slot0_active);
		return;
	}

	/* Auxiliary mapping never exists before the MAIN root policy. */
	um_refresh_mapping_policy(reln, MAIN_FORKNUM);
	if (!state->main_slot0_active)
		return;
	if (forknum == FSM_FORKNUM)
	{
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
um_fork_uses_mapped_slots(SMgrRelation reln, ForkNumber forknum)
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
um_active_pblk(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber logical_block)
{
	BlockNumber	physical_block;
	uint8		active_slot;

	active_slot = MapGetActiveSlot(um_get_filectx(reln), reln->smgr_rlocator,
								   forknum, logical_block);
	if (!UmbraActiveSlotPhysicalBlock(logical_block, active_slot,
									 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return physical_block;
}

/* Resolve the first contiguous physical range in one logical chunk. */
static void
um_get_mapped_read_run(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber logical_block, BlockNumber max_nblocks,
					   UmbraMappedReadRun *run)
{
	UmbraFileContext *ctx;
	BlockNumber	run_limit;

	Assert(run != NULL);
	Assert(max_nblocks > 0);
	Assert(um_fork_uses_mapped_slots(reln, forknum));
	ctx = um_get_filectx(reln);
	run->active_slot =
		MapGetActiveSlotWithPresence(ctx, reln->smgr_rlocator, forknum,
								 logical_block, &run->selector_page_present);
	if (!UmbraActiveSlotPhysicalBlock(logical_block, run->active_slot,
									 &run->physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));

	run_limit = UMBRA_CHUNK_PAIRED_PAGES -
		(logical_block % UMBRA_CHUNK_PAIRED_PAGES);
	run_limit = Min(run_limit, max_nblocks);
	run_limit = Min(run_limit,
					umfile_maxcombine(ctx, forknum, run->physical_block));
	run->nblocks = 1;
	for (BlockNumber offset = 1; offset < run_limit; offset++)
	{
		BlockNumber	next_logical_block;
		BlockNumber	next_physical_block;
		uint8		next_active_slot;
		bool		next_selector_page_present;

		if ((uint64) logical_block + offset > (uint64) MaxBlockNumber)
			break;
		next_logical_block = logical_block + offset;
		next_active_slot =
			MapGetActiveSlotWithPresence(ctx, reln->smgr_rlocator, forknum,
									 next_logical_block,
									 &next_selector_page_present);
		if (next_active_slot != run->active_slot ||
			next_selector_page_present != run->selector_page_present)
			break;
		if (!UmbraActiveSlotPhysicalBlock(next_logical_block,
										  next_active_slot,
										  &next_physical_block))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra active-slot block mapping overflow for relation %u/%u/%u fork %d",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							(int) forknum)));
		if ((uint64) next_physical_block !=
			(uint64) run->physical_block + offset)
			break;
		run->nblocks++;
	}
}

static BlockNumber
um_mapped_range_end(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber blocknum, BlockNumber nblocks)
{
	uint64		end = (uint64) blocknum + nblocks;

	if (end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra mapped logical block range overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return (BlockNumber) end;
}

static BlockNumber
um_get_mapped_frontier(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum == MAIN_FORKNUM)
		return ummap_get_main_frontier(um_get_filectx(reln),
									  reln->smgr_rlocator);
	return ummap_get_aux_frontier(um_get_filectx(reln), forknum,
								   reln->smgr_rlocator);
}

static BlockNumber
um_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber logical_eof)
{
	BlockNumber physical_capacity;

	if (!UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra mapped capacity overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return physical_capacity;
}

static void
um_ensure_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
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
 * mapped physical address, so make the root's declared capacity readable
 * before dispatching that I/O.
 */
static void
um_ensure_aux_recovery_capacity(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;

	Assert(InRecovery);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	/* A truncate record can arrive before any page redo recreates this fork. */
	if (!umfile_exists(ctx, forknum))
		umfile_create(ctx, forknum, true);
	logical_eof = um_get_mapped_frontier(reln, forknum);
	physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
	um_ensure_mapped_capacity(reln, forknum, physical_capacity, true);
}

static void
um_zero_active_range(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber first_block, BlockNumber end_block,
					 bool skipFsync, BlockNumber output_first_block,
					 BlockNumber *physical_blocks)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	PGIOAlignedBlock zero_page = {{0}};
	const void *buffers[1] = {zero_page.data};

	for (BlockNumber logical_block = first_block;
		 logical_block < end_block;
		 logical_block++)
	{
		BlockNumber physical_block =
			um_active_pblk(reln, forknum, logical_block);

		umfile_writev(ctx, forknum, physical_block, buffers, 1,
					  skipFsync);
		if (physical_blocks != NULL && logical_block >= output_first_block)
			physical_blocks[logical_block - output_first_block] = physical_block;
	}
}

/* Publish only after the full three-slot capacity has been materialized. */
static void
um_publish_mapped_after_data(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_eof)
{
	/*
	 * Root writeback flushes its WAL dependency and syncs each activated fork
	 * before the root. The skip-WAL pending-sync path also calls umimmedsync(),
	 * which preserves the same order. Avoid an immediate fsync per extension.
	 */
	if (forknum == MAIN_FORKNUM)
		ummap_set_main_frontier(um_get_filectx(reln), reln->smgr_rlocator,
								 logical_eof);
	else
		ummap_set_aux_frontier(um_get_filectx(reln), forknum,
								reln->smgr_rlocator, logical_eof);
}

/* A lower root is safe before the physical tail is removed. */
static void
um_publish_mapped_before_truncate(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber logical_eof)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (forknum == MAIN_FORKNUM)
		ummap_publish_prepared_main_frontier(ctx, reln->smgr_rlocator,
										logical_eof);
	else
		ummap_publish_prepared_aux_frontier(ctx, forknum,
									 reln->smgr_rlocator, logical_eof);
}

/* Extension is the non-critical path that seeds selector pages for WAL shifts. */
static void
um_ensure_selector_pages(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber first_block, BlockNumber nblocks,
						 bool skipFsync)
{
	if (nblocks == 0 || CritSectionCount != 0 || !IsUnderPostmaster ||
		IsBootstrapProcessingMode() || IsInitProcessingMode())
		return;
	MapEnsureActiveSlotPages(um_get_filectx(reln), reln->smgr_rlocator,
							  forknum, first_block, nblocks, skipFsync);
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
