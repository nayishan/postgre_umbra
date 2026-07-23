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
	bool		main_slot0_active;
	bool		main_truncate_prepared;
	BlockNumber	main_truncate_logical_eof;
	BlockNumber	main_truncate_physical_capacity;
	BlockNumber	main_truncate_current_physical;
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static bool um_main_uses_slot0(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_main_active_pblk(SMgrRelation reln,
							   BlockNumber logical_block);
static BlockNumber um_main_range_end(SMgrRelation reln,
							  BlockNumber blocknum, BlockNumber nblocks);
static void um_get_main_frontiers(SMgrRelation reln, BlockNumber *logical_eof,
							  BlockNumber *physical_capacity);
static void um_require_main_range(SMgrRelation reln, BlockNumber blocknum,
							 BlockNumber nblocks);
static void um_ensure_main_capacity(SMgrRelation reln,
								 BlockNumber physical_capacity, bool skipFsync);
static void um_zero_main_range(SMgrRelation reln, BlockNumber first_block,
								BlockNumber end_block, bool skipFsync);
static void um_publish_main_after_data(SMgrRelation reln,
								  BlockNumber logical_eof,
								  BlockNumber physical_capacity);
static void um_publish_main_before_truncate(SMgrRelation reln,
									   BlockNumber logical_eof,
									   BlockNumber physical_capacity);

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
		state->main_slot0_active = false;
	}

	if (!InRecovery && state->uses_map)
	{
		ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
		state->main_slot0_active =
			ummap_main_slot0_active(state->filectx, reln->smgr_rlocator);
	}
	else if (InRecovery && state->uses_map)
		(void) ummap_try_main_slot0_active(state->filectx,
										reln->smgr_rlocator,
										&state->main_slot0_active);
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL && state->filectx != NULL)
	{
		if (forknum == MAIN_FORKNUM)
			umfile_close(state->filectx, UMBRA_METADATA_FORKNUM);
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
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->uses_map = needs_wal && IsUnderPostmaster &&
		!IsBootstrapProcessingMode() && !IsInitProcessingMode();
	state->main_slot0_active = false;
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
	return umfile_exists(um_get_filectx(reln), forknum);
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
		 const void *buffer, bool skipFsync)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	physical_block;
	BlockNumber	logical_end;
	const void *buffers[1];

	if (!um_main_uses_slot0(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		return;
	}

	um_get_main_frontiers(reln, &logical_eof, &physical_capacity);
	physical_block = um_main_active_pblk(reln, blocknum);
	buffers[0] = buffer;
	if (blocknum < logical_eof)
	{
		umfile_writev(um_get_filectx(reln), MAIN_FORKNUM, physical_block,
					  buffers, 1, skipFsync);
		return;
	}

	logical_end = um_main_range_end(reln, blocknum, 1);
	if (!UmbraMainSlot0PhysicalCapacity(logical_end, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN slot-0 capacity overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));

	ctx = um_get_filectx(reln);
	um_ensure_main_capacity(reln, physical_capacity, skipFsync);
	if (blocknum > logical_eof)
		um_zero_main_range(reln, logical_eof, blocknum, skipFsync);
	umfile_writev(ctx, MAIN_FORKNUM, physical_block, buffers, 1, skipFsync);
	um_publish_main_after_data(reln, logical_end, physical_capacity);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	if (!um_main_uses_slot0(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
		return;
	}
	if (nblocks <= 0)
		return;

	logical_end = um_main_range_end(reln, blocknum, (BlockNumber) nblocks);
	um_get_main_frontiers(reln, &logical_eof, &physical_capacity);
	if (logical_end <= logical_eof)
		return;
	if (!UmbraMainSlot0PhysicalCapacity(logical_end, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN slot-0 capacity overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));

	um_ensure_main_capacity(reln, physical_capacity, skipFsync);
	/* A regrown page can retain slot-0 capacity after a prior truncate. */
	um_zero_main_range(reln, logical_eof, logical_end, skipFsync);
	um_publish_main_after_data(reln, logical_end, physical_capacity);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	if (um_main_uses_slot0(reln, forknum))
		return true;
	return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	if (um_main_uses_slot0(reln, forknum))
		return 1;
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	UmbraFileContext *ctx;

	if (!um_main_uses_slot0(reln, forknum))
	{
		umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
		return;
	}
	if (nblocks == 0)
		return;

	um_require_main_range(reln, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_main_active_pblk(reln, blocknum + i);

		umfile_readv(ctx, MAIN_FORKNUM, physical_block, &buffers[i], 1);
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	PgAioTargetData *target;
	BlockNumber	physical_block;

	if (!um_main_uses_slot0(reln, forknum))
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
				 errmsg("Umbra slot-0 AIO requires a single logical block")));

	um_require_main_range(reln, blocknum, nblocks);
	physical_block = um_main_active_pblk(reln, blocknum);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	target = pgaio_io_get_target_data(ioh);
	target->smgr.physicalBlockNum = physical_block;
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), MAIN_FORKNUM, physical_block,
					  buffers, nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	UmbraFileContext *ctx;

	if (!um_main_uses_slot0(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
					  skipFsync);
		return;
	}
	if (nblocks == 0)
		return;

	um_require_main_range(reln, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_main_active_pblk(reln, blocknum + i);

		umfile_writev(ctx, MAIN_FORKNUM, physical_block, &buffers[i], 1,
					  skipFsync);
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	if (um_main_uses_slot0(reln, forknum))
		return;
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;

	if (um_main_uses_slot0(reln, forknum))
	{
		um_get_main_frontiers(reln, &logical_eof, &physical_capacity);
		return logical_eof;
	}
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;

	if (!um_main_uses_slot0(reln, forknum))
	{
		umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
		return;
	}

	/* A lower persisted root can still need an old physical tail removed. */
	if (!state->main_truncate_prepared)
		return;

	um_publish_main_before_truncate(reln, state->main_truncate_logical_eof,
									state->main_truncate_physical_capacity);
	ctx = um_get_filectx(reln);
	umfile_truncate(ctx, MAIN_FORKNUM,
					state->main_truncate_current_physical,
					state->main_truncate_physical_capacity);
	state->main_truncate_prepared = false;
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
	state->main_truncate_prepared = false;
	for (int i = 0; i < nforks; i++)
	{
		if (!um_main_uses_slot0(reln, forknum[i]))
			continue;

		um_get_main_frontiers(reln, &root_logical_eof,
							  &root_physical_capacity);
		target_logical_eof = Min(nblocks[i], root_logical_eof);
		if (!UmbraMainSlot0PhysicalCapacity(target_logical_eof,
											 &target_physical_capacity))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra MAIN slot-0 truncate frontier overflow")));

		ctx = um_get_filectx(reln);
		ummap_prepare_main_frontiers(ctx, reln->smgr_rlocator);
		current_physical = umfile_nblocks(ctx, MAIN_FORKNUM);
		if (current_physical < root_physical_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Umbra MAIN physical capacity precedes its metadata root")));
		if (target_logical_eof == root_logical_eof &&
			current_physical == target_physical_capacity)
			continue;

		state->main_truncate_logical_eof = target_logical_eof;
		state->main_truncate_physical_capacity = target_physical_capacity;
		state->main_truncate_current_physical = current_physical;
		state->main_truncate_prepared = true;
		return;
	}
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_immedsync(ctx, forknum);
	if (forknum == MAIN_FORKNUM)
		ummap_immedsync_if_exists(ctx, reln->smgr_rlocator);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_registersync(ctx, forknum);
	if (forknum == MAIN_FORKNUM)
		ummap_registersync_if_exists(ctx, reln->smgr_rlocator);
}

bool
umpreparependingsync(SMgrRelation reln)
{
	return um_main_uses_slot0(reln, MAIN_FORKNUM);
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
um_main_range_end(SMgrRelation reln, BlockNumber blocknum,
				  BlockNumber nblocks)
{
	uint64		end = (uint64) blocknum + nblocks;

	if (end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra MAIN logical block range overflow for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));
	return (BlockNumber) end;
}

static void
um_get_main_frontiers(SMgrRelation reln, BlockNumber *logical_eof,
					  BlockNumber *physical_capacity)
{
	ummap_get_main_frontiers(um_get_filectx(reln), reln->smgr_rlocator,
							 logical_eof, physical_capacity);
}

static void
um_require_main_range(SMgrRelation reln, BlockNumber blocknum,
					  BlockNumber nblocks)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	logical_end;

	logical_end = um_main_range_end(reln, blocknum, nblocks);
	um_get_main_frontiers(reln, &logical_eof, &physical_capacity);
	if (logical_end > logical_eof)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not access logical block %u beyond Umbra MAIN EOF %u",
						blocknum, logical_eof)));
}

static void
um_ensure_main_capacity(SMgrRelation reln, BlockNumber physical_capacity,
						bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	current_physical;

	current_physical = umfile_nblocks(ctx, MAIN_FORKNUM);
	while (current_physical < physical_capacity)
	{
		uint64		remaining = (uint64) physical_capacity - current_physical;
		int			extend_by = (int) Min(remaining, (uint64) INT_MAX);

		umfile_zeroextend(ctx, MAIN_FORKNUM, current_physical, extend_by,
						  skipFsync);
		current_physical += (BlockNumber) extend_by;
	}
}

static void
um_zero_main_range(SMgrRelation reln, BlockNumber first_block,
				   BlockNumber end_block, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	PGIOAlignedBlock zero_page = {0};
	const void *buffers[1] = {zero_page.data};

	for (BlockNumber logical_block = first_block;
		 logical_block < end_block;
		 logical_block++)
	{
		BlockNumber	physical_block =
			um_main_active_pblk(reln, logical_block);

		umfile_writev(ctx, MAIN_FORKNUM, physical_block, buffers, 1,
					  skipFsync);
	}
}

/* Publish only after the full three-slot capacity has been materialized. */
static void
um_publish_main_after_data(SMgrRelation reln, BlockNumber logical_eof,
						  BlockNumber physical_capacity)
{
	ummap_set_main_frontiers(um_get_filectx(reln), reln->smgr_rlocator,
						 logical_eof, physical_capacity);
}

/* A lower root is safe before the physical tail is removed. */
static void
um_publish_main_before_truncate(SMgrRelation reln, BlockNumber logical_eof,
								BlockNumber physical_capacity)
{
	ummap_publish_prepared_main_frontiers(um_get_filectx(reln),
									reln->smgr_rlocator, logical_eof,
									physical_capacity);
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
