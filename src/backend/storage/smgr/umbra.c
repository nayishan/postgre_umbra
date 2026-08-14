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

typedef enum UmbraMapPolicy
{
	UMBRA_MAP_POLICY_UNKNOWN,
	UMBRA_MAP_POLICY_PLAIN,
	UMBRA_MAP_POLICY_MAPPED
} UmbraMapPolicy;

typedef struct UmbraSmgrRelationState
{
	/*
	 * State stored in SMgrRelationData.smgr_private.
	 *
	 * Umbra owns this state for the lifetime of the SMgrRelation handle.
	 * umfile.c owns the physical segment descriptors below this context.
	 */
	UmbraFileContext *filectx;
	UmbraMapPolicy map_policy;
	bool		map_root_needs_validation;
	UmbraMappedTruncateState mapped_truncate[MAX_FORKNUM + 1];
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static void um_refresh_mapping_policy(SMgrRelation reln, ForkNumber forknum);
static void um_set_map_policy_mapped(SMgrRelation reln);
static bool um_fork_uses_mapped_slots(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_active_pblk(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber logical_block,
								  uint8 *active_slot);
static BlockNumber um_mapped_range_end(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber blocknum, BlockNumber nblocks);
static BlockNumber um_get_mapped_frontier(SMgrRelation reln,
									  ForkNumber forknum);
static BlockNumber um_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber logical_eof);
static void um_require_mapped_range(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber blocknum, BlockNumber nblocks);
static void um_ensure_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber physical_capacity,
									  bool skipFsync);
static void um_ensure_aux_recovery_capacity(SMgrRelation reln,
												 ForkNumber forknum);
static void um_zero_active_range(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber first_block, BlockNumber end_block,
								  bool skipFsync);
static void um_publish_mapped_after_data(SMgrRelation reln, ForkNumber forknum,
									BlockNumber logical_eof);
static void um_publish_mapped_before_truncate(SMgrRelation reln,
										  ForkNumber forknum,
										  BlockNumber logical_eof);
static void um_ensure_selector_pages(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber first_block, BlockNumber nblocks,
								 bool skipFsync);

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
		state->map_policy = UMBRA_MAP_POLICY_UNKNOWN;
		state->map_root_needs_validation = false;
		um_refresh_mapping_policy(reln, MAIN_FORKNUM);
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
			/* Reopen the root, but do not change an established layout. */
			if (state->map_policy == UMBRA_MAP_POLICY_MAPPED)
				state->map_root_needs_validation = true;
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

	Assert(state != NULL);
	/*
	 * Page redo can reach this generic path to recreate a missing MAIN fork.
	 * Its locator and isRedo flag do not identify persistence, and catalog
	 * lookup is not safe during recovery.  It may validate an existing root,
	 * but only XLOG_SMGR_CREATE redo may create one through
	 * smgrfinishcreate().
	 */
	um_refresh_mapping_policy(reln, forknum);
	umfile_create(ctx, forknum, isRedo);
	if (isRedo && forknum == MAIN_FORKNUM &&
		!RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
		state->map_policy == UMBRA_MAP_POLICY_UNKNOWN &&
		ummap_try_validate(ctx))
		um_set_map_policy_mapped(reln);
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->map_policy = UMBRA_MAP_POLICY_PLAIN;
	state->map_root_needs_validation = false;
	/*
	 * needs_wal identifies permanent storage.  Bootstrap and init must create
	 * its root too, because it fixes the layout normal backends will later use.
	 */
	if (needs_wal)
	{
		ummap_create(um_get_filectx(reln), reln->smgr_rlocator, false);
		um_set_map_policy_mapped(reln);
	}
}

void
umfinishcreate(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;
	UmbraFileContext *ctx;

	if (forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return;

	Assert(InRecovery);
	ctx = um_get_filectx(reln);
	state = reln->smgr_private;
	Assert(state != NULL);

	/*
	 * Only authoritative CREATE redo may create or repair a missing root.
	 * Defensive page redo can create the MAIN file, but must not invent the
	 * relation-wide mapping authority.  Later redo can use the resident root.
	 * Normal checkpoint or relation metadata synchronization makes it durable;
	 * no separate immediate sync is needed here.
	 */
	ummap_create(ctx, reln->smgr_rlocator, true);
	um_set_map_policy_mapped(reln);
}

/* Flush Umbra's private metadata-root cache at the checkpoint boundary. */
void
umcheckpoint(void)
{
	ummap_checkpoint();
}

/*
 * During redo, an UNKNOWN policy for a mapped-capable fork is not permission
 * to use the direct physical layout.  Only CREATE redo may resolve it.
 */
bool
UmRedoMappingPolicyResolved(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;

	if (reln == NULL || !UmbraForkUsesActiveSlots(forknum) ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return true;

	Assert(InRecovery);
	um_refresh_mapping_policy(reln, forknum);
	state = reln->smgr_private;
	return state != NULL && state->filectx != NULL &&
		state->map_policy != UMBRA_MAP_POLICY_UNKNOWN;
}

/*
 * Choose a transition from a buffer's cached slot without consulting MAP.
 * Publication rereads the canonical selector under its own lock.
 */
bool
UmChooseSlotShift(SMgrRelation reln, ForkNumber forknum,
				  BlockNumber logical_block, uint8 cached_active_slot,
				  UmbraSlotShift *shift)
{
	UmbraSmgrRelationState *state;

	Assert(shift != NULL);
	MemSet(shift, 0, sizeof(*shift));
	if (reln == NULL || InRecovery ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		IsBootstrapProcessingMode() || IsInitProcessingMode() ||
		!UmbraActiveSlotIsValid(cached_active_slot) ||
		!um_fork_uses_mapped_slots(reln, forknum))
		return false;

	state = reln->smgr_private;
	if (state == NULL || state->filectx == NULL)
		return false;

	shift->reln = reln;
	shift->forknum = forknum;
	shift->logical_block = logical_block;
	shift->source_slot = cached_active_slot;
	shift->target_slot = UmbraNextActiveSlot(cached_active_slot);
	shift->selected = true;
	return true;
}

void
UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn)
{
	UmbraSmgrRelationState *state;

	Assert(shift != NULL);
	Assert(shift->selected);
	Assert(shift->reln != NULL);
	Assert(XLogRecPtrIsValid(lsn));
	state = shift->reln->smgr_private;
	Assert(state != NULL && state->filectx != NULL);
	MapPublishSlotShift(state->filectx, shift->reln->smgr_rlocator,
						shift->forknum, shift->logical_block,
						shift->source_slot, shift->target_slot, lsn);
	shift->selected = false;
	shift->reln = NULL;
}

bool
UmCheckpointWriteSourceSlot(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_block, uint8 source_slot,
							const void *buffer)
{
	const void *buffers[1] = {buffer};
	BlockNumber	physical_block;

	if (reln == NULL || buffer == NULL ||
		!UmbraActiveSlotIsValid(source_slot))
		return false;
	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
		return false;
	if (!UmbraActiveSlotPhysicalBlock(logical_block, source_slot,
									 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra source-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber, (int) forknum)));

	umfile_writev(um_get_filectx(reln), forknum, physical_block,
				  buffers, 1, false);
	return true;
}

void
UmCheckpointWritebackSourceSlot(SMgrRelation reln, ForkNumber forknum,
								BlockNumber logical_block, uint8 source_slot)
{
	BlockNumber	physical_block;

	if (reln == NULL || !UmbraActiveSlotIsValid(source_slot))
		return;
	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
		return;
	if (!UmbraActiveSlotPhysicalBlock(logical_block, source_slot,
									 &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra source-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber, (int) forknum)));

	umfile_writeback(um_get_filectx(reln), forknum, physical_block, 1);
}

bool
UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
					BlockNumber logical_block, uint8 active_slot)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	physical_block;
	BlockNumber	physical_capacity;
	BlockNumber	required_eof;

	if (!InRecovery || reln == NULL ||
		!UmbraForkUsesActiveSlots(forknum) ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!UmbraActiveSlotIsValid(active_slot))
		elog(PANIC, "Umbra redo targets an invalid active-slot mapping");

	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);

	/* Image-free redo must never create or repair mapping authority. */
	if (!ummap_try_validate(ctx))
		return false;
	um_set_map_policy_mapped(reln);
	if (forknum == MAIN_FORKNUM)
	{
		logical_eof = um_get_mapped_frontier(reln, forknum);
		physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
		if (logical_block >= logical_eof ||
			!umfile_exists(ctx, forknum) ||
			umfile_nblocks(ctx, forknum) < physical_capacity)
			return false;
	}
	else
	{
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
	UmbraFileContext *ctx;

	if (!InRecovery || reln == NULL ||
		!UmbraForkUsesActiveSlots(forknum) ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!XLogRecPtrIsValid(shift_lsn))
		elog(PANIC, "Umbra slot-shift WAL targets an inactive mapping");
	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);

	/*
	 * A full-page image proves that one logical page can be reconstructed, but
	 * it does not identify the lifecycle that owned a missing metadata root.
	 * Only CREATE redo may rebuild that relation-wide mapping authority.  A
	 * later DROP or covering TRUNCATE can clear the dependency recorded by the
	 * caller.
	 */
	if (!ummap_try_validate(ctx))
		return false;
	um_set_map_policy_mapped(reln);

	/* With mapping authority established, the FPI may recreate its fork. */
	umfile_create(ctx, forknum, true);

	MapRedoSlotShift(ctx, reln->smgr_rlocator, forknum,
					 logical_block, source_slot, target_slot);
	return true;
}

/* Flush Umbra's metadata-root cache before copying a database tablespace. */
void
umflushdatabasetablespacecache(Oid dbid, Oid spcOid)
{
	ummap_flush_database_tablespace_cache(dbid, spcOid);
}

/* Discard Umbra's metadata-root cache entries for a database without I/O. */
void
uminvalidatedatabasecache(Oid dbid)
{
	ummap_invalidate_database_cache(dbid);
}

/* Discard one tablespace's metadata-root cache entries without I/O. */
void
uminvalidatedatabasetablespacecache(Oid dbid, Oid spcOid)
{
	ummap_invalidate_database_tablespace_cache(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;

	um_refresh_mapping_policy(reln, forknum);
	ctx = um_get_filectx(reln);

	/*
	 * During recovery, a nonempty mapped auxiliary frontier declares a logical
	 * fork even if its physical file has not been recreated yet.  A zero
	 * frontier does not materialize an otherwise absent fork.
	 */
	if (InRecovery && UmbraIsMappedAuxiliaryFork(forknum) &&
		um_fork_uses_mapped_slots(reln, forknum))
	{
		logical_eof = um_get_mapped_frontier(reln, forknum);
		if (logical_eof != 0)
			return true;
	}
	return umfile_exists(ctx, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	/*
	 * The private root is relation-level state, not an auxiliary-fork file.
	 * Removing MAIN or every fork removes that authority; removing only FSM or
	 * VM leaves it for the relation's remaining forks.
	 */
	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
		ummap_unlink(rlocator, isRedo);
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

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		return;
	}

	logical_eof = um_get_mapped_frontier(reln, forknum);
	if (blocknum < logical_eof)
		ereport(ERROR,
				(errmsg("cannot extend mapped Umbra fork below logical EOF")));
	Assert(blocknum >= logical_eof);
	physical_block = um_active_pblk(reln, forknum, blocknum, NULL);

	logical_end = um_mapped_range_end(reln, forknum, blocknum, 1);
	physical_capacity = um_mapped_capacity(reln, forknum, logical_end);

	ctx = um_get_filectx(reln);
	um_ensure_mapped_capacity(reln, forknum, physical_capacity, skipFsync);
	if (blocknum > logical_eof)
		um_zero_active_range(reln, forknum, logical_eof, blocknum, skipFsync);
	umfile_writev(ctx, forknum, physical_block, &buffer, 1, skipFsync);
	um_publish_mapped_after_data(reln, forknum, logical_end);
	um_ensure_selector_pages(reln, forknum, logical_eof,
						 logical_end - logical_eof, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
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
	/* A regrown page can still occupy retained active-slot capacity after truncate. */
	um_zero_active_range(reln, forknum, logical_eof, logical_end, skipFsync);
	um_publish_mapped_after_data(reln, forknum, logical_end);
	um_ensure_selector_pages(reln, forknum, logical_eof,
						 logical_end - logical_eof, skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	/*
	 * A mapped logical range cannot be passed directly to umfile_prefetch():
	 * active-slot layout can make it physically noncontiguous. This mapping path
	 * leaves prefetch unimplemented; a later patch can translate logical blocks
	 * before submitting physical prefetch advice.
	 */
	um_refresh_mapping_policy(reln, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		return true;
	return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	um_refresh_mapping_policy(reln, forknum);
	if (um_fork_uses_mapped_slots(reln, forknum))
		return 1;
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	UmbraFileContext *ctx;

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
	um_require_mapped_range(reln, forknum, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, blocknum + i, NULL);

		umfile_readv(ctx, forknum, physical_block, &buffers[i], 1);
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	PgAioTargetData *target;
	BlockNumber	physical_block;
	uint8		active_slot = UMBRA_ACTIVE_SLOT_INVALID;

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
		pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
		umfile_startreadv(ioh, um_get_filectx(reln), forknum, blocknum,
						  buffers, nblocks);
		return;
	}
	if (nblocks != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Umbra active-slot AIO requires a single logical block")));

	if (InRecovery && UmbraIsMappedAuxiliaryFork(forknum))
		um_ensure_aux_recovery_capacity(reln, forknum);
	um_require_mapped_range(reln, forknum, blocknum, nblocks);
	physical_block = um_active_pblk(reln, forknum, blocknum, &active_slot);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	target = pgaio_io_get_target_data(ioh);
	target->smgr.physicalBlockNum = physical_block;
	target->smgr.umbraActiveSlot = active_slot;
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

	um_refresh_mapping_policy(reln, forknum);
	if (!um_fork_uses_mapped_slots(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
					  skipFsync);
		return;
	}
	if (nblocks == 0)
		return;

	um_require_mapped_range(reln, forknum, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, blocknum + i, NULL);

		umfile_writev(ctx, forknum, physical_block, &buffers[i], 1,
					  skipFsync);
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

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * A mapped fork reports logical EOF from its metadata root, rather than
	 * the physical file length that mdnblocks() reports.  This avoids opening
	 * physical data segments for a logical size query.
	 */
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
	 * still-longer mapped file. A prepared physical cleanup is required even
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

/*
 * Do the allocation-capable half of mapped truncate before the caller's
 * critical section.  Resolve the root cache and open the metadata and MAIN
 * descriptors now; umtruncate() must use only this prepared state.
 */
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

/*
 * Core calls these per-fork callbacks once for every ordinary PostgreSQL
 * fork.  They intentionally do only that fork's file work.  In particular,
 * do not flush or register the private metadata fork while handling MAIN:
 *
 * - a MAP root is relation-level state, not a MAIN-fork cache entry;
 * - it can publish logical EOF and mapping authority; and
 * - core might still have ordinary fork work to finish for this relation.
 *
 * smgrdosyncall() invokes umsyncrelationmetadata() only after its complete
 * ordinary-fork loop.  Keeping the two responsibilities separate makes that
 * order explicit and prevents an accidental MAP publication from a generic
 * per-fork callback.
 */
void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	um_refresh_mapping_policy(reln, forknum);
	umfile_immedsync(um_get_filectx(reln), forknum);
}

/*
 * Deferred sync registration follows the same rule as immediate sync.  The
 * request carries only the ordinary fork; the later smgrdosyncall() path
 * flushes relation-level metadata after ordinary buffers and forks have been
 * made durable.  Registering _map here would reintroduce a per-fork metadata
 * publication path with no relation-wide ordering guarantee.
 */
void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	um_refresh_mapping_policy(reln, forknum);
	umfile_registersync(um_get_filectx(reln), forknum);
}

/*
 * This is Umbra's normal relation-level metadata boundary.  Its caller,
 * smgrdosyncall(), has already written the relation's ordinary buffers and
 * synchronized all existing ordinary forks.  Only now may Umbra serialize
 * a dirty MAP root and synchronize the metadata fork containing it.
 *
 * Do not add MAIN synchronization or selector-page discovery here.  Those
 * are separate caller-owned phases: ordinary fork durability is completed by
 * core, while checkpoint selector flushing is ordered by ummap_checkpoint().
 * This function publishes only a root that its caller has made safe to
 * publish.
 */
void
umsyncrelationmetadata(SMgrRelation reln)
{
	um_refresh_mapping_policy(reln, MAIN_FORKNUM);
	if (!um_fork_uses_mapped_slots(reln, MAIN_FORKNUM))
		return;

	ummap_sync_relation_metadata(um_get_filectx(reln),
								reln->smgr_rlocator);
}

bool
umforcependingsync(SMgrRelation reln)
{
	um_refresh_mapping_policy(reln, MAIN_FORKNUM);
	/*
	 * This predicate is used only by commit-time pending-sync finalization of a
	 * WAL-skipping relation.  A mapped MAIN root is a prerequisite for mapped
	 * FSM or VM, and generic full-page WAL omits that private authority.  The
	 * MAIN policy alone therefore selects the real file-sync path.
	 */
	return um_fork_uses_mapped_slots(reln, MAIN_FORKNUM);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/* AIO reopen passes the physical block stored in its target data. */
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

/*
 * A mapped layout is established by a valid root or authoritative CREATE
 * redo.  It remains mapped across SMGRRELEASE: a later metadata-root failure
 * must not select direct I/O for the same physical data file.
 */
static void
um_refresh_mapping_policy(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;

	if (reln == NULL ||
		(forknum != MAIN_FORKNUM && forknum != FSM_FORKNUM &&
		 forknum != VISIBILITYMAP_FORKNUM))
		return;
	Assert(CritSectionCount == 0);
	state = reln->smgr_private;
	if (state == NULL || state->filectx == NULL)
		return;
	if (RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
	{
		if (state->map_policy == UMBRA_MAP_POLICY_UNKNOWN)
			state->map_policy = UMBRA_MAP_POLICY_PLAIN;
		return;
	}

	if (state->map_policy == UMBRA_MAP_POLICY_MAPPED)
	{
		if (!state->map_root_needs_validation)
			return;
		if (!ummap_exists(state->filectx) ||
			!ummap_try_validate(state->filectx))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Umbra mapped relation metadata root is missing or corrupted")));
		if (!InRecovery)
			ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
		um_set_map_policy_mapped(reln);
		return;
	}

	if (state->map_policy == UMBRA_MAP_POLICY_PLAIN)
		return;

	Assert(state->map_policy == UMBRA_MAP_POLICY_UNKNOWN);
	if (!ummap_exists(state->filectx))
	{
		/* Recovery can still encounter authoritative CREATE redo later. */
		if (!InRecovery)
			state->map_policy = UMBRA_MAP_POLICY_PLAIN;
		return;
	}
	if (InRecovery)
	{
		/* CREATE redo is allowed to repair this otherwise unclaimed root. */
		if (!ummap_try_validate(state->filectx))
			return;
	}
	else
		ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
	um_set_map_policy_mapped(reln);
}

static void
um_set_map_policy_mapped(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	Assert(state->filectx != NULL);
	Assert(!RelFileLocatorBackendIsTemp(reln->smgr_rlocator));

	state->map_policy = UMBRA_MAP_POLICY_MAPPED;
	state->map_root_needs_validation = false;
	reln->smgr_cached_nblocks[MAIN_FORKNUM] = InvalidBlockNumber;
	reln->smgr_cached_nblocks[FSM_FORKNUM] = InvalidBlockNumber;
	reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = InvalidBlockNumber;
}

static bool
um_fork_uses_mapped_slots(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	return state != NULL && state->map_policy == UMBRA_MAP_POLICY_MAPPED &&
		UmbraForkUsesActiveSlots(forknum);
}

bool
UmGetActiveSlot(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, uint8 *active_slot)
{
	if (active_slot != NULL)
		*active_slot = UMBRA_ACTIVE_SLOT_INVALID;
	if (reln == NULL || active_slot == NULL ||
		!um_fork_uses_mapped_slots(reln, forknum))
		return false;

	*active_slot = MapGetActiveSlot(um_get_filectx(reln), reln->smgr_rlocator,
								  forknum, logical_block);
	return true;
}

static BlockNumber
um_active_pblk(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber logical_block, uint8 *active_slot)
{
	BlockNumber	physical_block;
	uint8		slot;

	if (!UmGetActiveSlot(reln, forknum, logical_block, &slot))
		elog(ERROR, "could not resolve Umbra active slot");
	if (!UmbraActiveSlotPhysicalBlock(logical_block, slot, &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	if (active_slot != NULL)
		*active_slot = slot;
	return physical_block;
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
um_require_mapped_range(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber	logical_eof;
	BlockNumber	logical_end;

	logical_end = um_mapped_range_end(reln, forknum, blocknum, nblocks);
	logical_eof = um_get_mapped_frontier(reln, forknum);
	if (logical_end > logical_eof)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not access logical block %u beyond Umbra mapped EOF %u for fork %d",
						blocknum, logical_eof, (int) forknum)));
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
	logical_eof = um_get_mapped_frontier(reln, forknum);
	if (logical_eof == 0)
		return;
	physical_capacity = um_mapped_capacity(reln, forknum, logical_eof);
	/* A truncate record can arrive before any page redo recreates this fork. */
	if (!umfile_exists(ctx, forknum))
		umfile_create(ctx, forknum, true);
	um_ensure_mapped_capacity(reln, forknum, physical_capacity, true);
}

static void
um_zero_active_range(SMgrRelation reln, ForkNumber forknum,
				BlockNumber first_block, BlockNumber end_block, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	PGIOAlignedBlock zero_page = {0};
	const void *buffers[1] = {zero_page.data};

	for (BlockNumber logical_block = first_block;
		 logical_block < end_block;
		 logical_block++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, logical_block, NULL);

		umfile_writev(ctx, forknum, physical_block, buffers, 1,
					  skipFsync);
	}
}

/* Publish only after the full three-slot capacity has been materialized. */
static void
um_publish_mapped_after_data(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_eof)
{
	/*
	 * The selected data range is materialized before recording this frontier.
	 * Checkpoint and pending-sync callers establish ordinary-fork durability
	 * before publishing the dirty root; avoid an immediate fsync per extension.
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
