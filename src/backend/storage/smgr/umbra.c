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

#include "storage/aio.h"
#include "storage/map.h"
#include "storage/smgr.h"
#include "storage/um_defs.h"
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
	bool		uses_three_buckets;
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static bool um_uses_three_buckets(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_active_pblk(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber logical_block);
static BlockNumber um_three_bucket_nblocks(SMgrRelation reln,
									ForkNumber forknum);
static void um_ensure_three_bucket_capacity(SMgrRelation reln,
										 ForkNumber forknum,
										 BlockNumber physical_capacity,
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
		/* An existing INIT fork identifies an unlogged relation after reopen. */
		state->uses_three_buckets =
			!RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
			!umfile_exists(state->filectx, INIT_FORKNUM);
	}
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL && state->filectx != NULL)
	{
		umfile_close(state->filectx, forknum);
		if (forknum == MAIN_FORKNUM &&
			um_uses_three_buckets(reln, forknum))
			umfile_close(state->filectx, UMBRA_METADATA_FORKNUM);
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

	/* INIT exists only for unlogged relations, which use direct layout. */
	if (forknum == INIT_FORKNUM)
	{
		Assert(state != NULL);
		state->uses_three_buckets = false;
	}
	umfile_create(um_get_filectx(reln), forknum, isRedo);
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->uses_three_buckets = needs_wal;
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
	{
		MapInvalidateRelation(rlocator);
		umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
	}
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	const void *buffers[1] = {buffer};
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;
	BlockNumber	physical_block;

	if (!um_uses_three_buckets(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		return;
	}

	logical_eof = um_three_bucket_nblocks(reln, forknum);
	if (blocknum < logical_eof)
		ereport(ERROR,
				(errmsg("cannot extend Umbra three-bucket fork below logical EOF")));
	if (blocknum == MaxBlockNumber ||
		!UmbraThreeBucketPhysicalCapacity(blocknum + 1, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra three-bucket capacity overflow")));
	um_ensure_three_bucket_capacity(reln, forknum, physical_capacity,
								skipFsync);
	physical_block = um_active_pblk(reln, forknum, blocknum);
	umfile_writev(um_get_filectx(reln), forknum, physical_block, buffers, 1,
				  skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	logical_end;
	BlockNumber	physical_capacity;

	if (!um_uses_three_buckets(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
		return;
	}
	if (nblocks <= 0)
		return;
	if ((uint64) blocknum + (uint64) nblocks >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra three-bucket logical range overflow")));
	logical_end = blocknum + (BlockNumber) nblocks;
	if (!UmbraThreeBucketPhysicalCapacity(logical_end, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra three-bucket capacity overflow")));
	um_ensure_three_bucket_capacity(reln, forknum, physical_capacity,
								skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	if (um_uses_three_buckets(reln, forknum))
		return true;
	return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	if (um_uses_three_buckets(reln, forknum))
		return 1;
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	if (um_uses_three_buckets(reln, forknum))
	{
		for (BlockNumber i = 0; i < nblocks; i++)
		{
			BlockNumber	physical_block =
				um_active_pblk(reln, forknum, blocknum + i);

			umfile_readv(um_get_filectx(reln), forknum, physical_block,
						 buffers + i, 1);
		}
		return;
	}
	umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	BlockNumber	physical_block = blocknum;

	if (um_uses_three_buckets(reln, forknum))
	{
		Assert(nblocks == 1);
		physical_block = um_active_pblk(reln, forknum, blocknum);
	}
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), forknum, physical_block, buffers,
					  nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	if (um_uses_three_buckets(reln, forknum))
	{
		for (BlockNumber i = 0; i < nblocks; i++)
		{
			BlockNumber	physical_block =
				um_active_pblk(reln, forknum, blocknum + i);

			umfile_writev(um_get_filectx(reln), forknum, physical_block,
						  buffers + i, 1, skipFsync);
		}
		return;
	}
	umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
				  skipFsync);
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	if (um_uses_three_buckets(reln, forknum))
	{
		for (BlockNumber i = 0; i < nblocks; i++)
			umfile_writeback(um_get_filectx(reln), forknum,
						 um_active_pblk(reln, forknum, blocknum + i), 1);
		return;
	}
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (um_uses_three_buckets(reln, forknum))
		return um_three_bucket_nblocks(reln, forknum);
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	BlockNumber	old_physical;
	BlockNumber	new_physical;

	if (um_uses_three_buckets(reln, forknum))
	{
		if (!UmbraThreeBucketPhysicalCapacity(old_blocks, &old_physical) ||
			!UmbraThreeBucketPhysicalCapacity(nblocks, &new_physical))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra three-bucket truncation overflow")));
		umfile_truncate(um_get_filectx(reln), forknum, old_physical,
						new_physical);
		return;
	}
	umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	umfile_immedsync(um_get_filectx(reln), forknum);
}

void
umsyncrelationmetadata(SMgrRelation reln)
{
	if (um_uses_three_buckets(reln, MAIN_FORKNUM))
		ummap_sync_relation_metadata(um_get_filectx(reln),
									 reln->smgr_rlocator);
}

void
umcheckpoint(void)
{
	ummap_checkpoint();
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	umfile_registersync(um_get_filectx(reln), forknum);
}

void
umflushdatabasetablespacecache(Oid dbid, Oid spcOid)
{
	MapFlushDatabaseTablespace(dbid, spcOid);
}

void
uminvalidatedatabasecache(Oid dbid)
{
	MapInvalidateDatabase(dbid);
}

void
uminvalidatedatabasetablespacecache(Oid dbid, Oid spcOid)
{
	MapInvalidateDatabaseTablespace(dbid, spcOid);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	if (um_uses_three_buckets(reln, forknum))
		blocknum = um_active_pblk(reln, forknum, blocknum);
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

static bool
um_uses_three_buckets(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state;

	if (reln == NULL || RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return false;
	state = reln->smgr_private;
	return state != NULL && state->uses_three_buckets &&
		UmbraForkUsesThreeBuckets(forknum);
}

static BlockNumber
um_active_pblk(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber logical_block)
{
	BlockNumber	physical_block;
	uint8		active_slot = 0;

	if (um_uses_three_buckets(reln, forknum))
		active_slot = MapGetActiveSlot(um_get_filectx(reln),
								   reln->smgr_rlocator, logical_block);
	if (!UmbraActiveSlotPhysicalBlock(logical_block, active_slot,
								  &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra three-bucket block mapping overflow for fork %d",
						(int) forknum)));
	return physical_block;
}

static BlockNumber
um_three_bucket_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber	physical_nblocks = umfile_nblocks(um_get_filectx(reln),
													  forknum);

	/*
	 * As with mdnblocks(), a concurrent zero-extend can expose a partial
	 * physical tail.  A logical block is visible only after all three slots
	 * have been created.
	 */
	return physical_nblocks / UMBRA_ACTIVE_SLOT_COUNT;
}

static void
um_ensure_three_bucket_capacity(SMgrRelation reln, ForkNumber forknum,
								BlockNumber physical_capacity, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	current_physical = umfile_nblocks(ctx, forknum);

	while (current_physical < physical_capacity)
	{
		uint64		remaining = (uint64) physical_capacity - current_physical;
		int			zero_by = (int) Min(remaining, (uint64) INT_MAX);

		umfile_zeroextend(ctx, forknum, current_physical, zero_by, skipFsync);
		current_physical += (BlockNumber) zero_by;
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
