/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager.
 *
 * This file owns the smgr implementation boundary for Umbra.  Ordinary
 * relation fork operations first pass through Umbra's access boundary, then
 * use Umbra's md-style physical segment file layer in umfile.c.
 *
 * Permanent relations use the private map fork in identity mode: extension
 * paths materialize new map entries as logical block L -> physical block L.
 * Temp and unlogged relations bypass MAP and use direct identity I/O, matching
 * PostgreSQL's existing non-WAL lifecycle.
 *
 * The MAP superblock stores relation-wide logical and physical frontiers,
 * while MAP entries remain authoritative for logical-to-physical translation.
 * Recovery uses the same superblock size information as normal operation.  If
 * redo reaches a block whose identity MAP entry was not persisted, MAP lookup
 * can reconstruct that entry as L -> L because PostgreSQL WAL supplies L and
 * this patch fixes P equal to L.  A future non-identity policy must WAL-log the
 * chosen P instead.
 *
 * Relation-local private map fork handling lives in ummap.c.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static bool um_fork_uses_map(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_get_logical_nblocks(SMgrRelation reln,
										  ForkNumber forknum);

void
uminit(void)
{
	umfile_init();
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
	bool		created;

	Assert(state != NULL);
	created = umfile_create(ctx, forknum, isRedo);

	/* Redo of a mapped fork belongs to a permanent relation. */
	if (isRedo && ummap_tracks_fork(forknum))
	{
		state->uses_map = true;
		if (!ummap_exists(ctx))
			ummap_create(ctx, true);
	}

	if (!isRedo && state->uses_map && forknum != MAIN_FORKNUM &&
		ummap_tracks_fork(forknum))
	{
		if (created && ummap_exists(ctx))
			ummap_init_fork(ctx, reln->smgr_rlocator, forknum, false);
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

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (um_fork_uses_map(reln, forknum))
	{
		/*
		 * A subtransaction abort can unlink a newly-created relation while
		 * its pending-sync entry and SMgrRelation survive until top commit.
		 */
		if (!ummap_exists(ctx))
			return false;

		return ummap_fork_exists(ctx, reln->smgr_rlocator, forknum) &&
			umfile_exists(ctx, forknum);
	}

	return umfile_exists(ctx, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == InvalidForkNumber)
	{
		MapInvalidateRelation(rlocator);
		umfile_unlink(rlocator, forknum, isRedo);
		return;
	}

	if (forknum == MAIN_FORKNUM)
		ummap_unlink(rlocator, isRedo);

	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	lblkno;
	BlockNumber	old_nblocks;
	BlockNumber	pblkno;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_extend(ctx, forknum, blocknum, buffer, skipFsync);
		return;
	}

	old_nblocks = um_get_logical_nblocks(reln, forknum);
	pblkno = blocknum;
	umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);

	/*
	 * smgrextend() permits writing beyond EOF.  The physical extension makes
	 * the intervening sparse blocks read as zeroes, so publish identity MAP
	 * entries for the entire newly-addressable range.
	 */
	lblkno = old_nblocks;
	while (lblkno <= blocknum)
	{
		BlockNumber published_blocks;
		BlockNumber published_pblkno;
		BlockNumber run_blocks;

		run_blocks = ummap_identity_run_limit(forknum, lblkno,
											 blocknum - lblkno + 1, &pblkno);
		published_blocks = ummap_set_identity_run(ctx, reln->smgr_rlocator,
											  forknum, lblkno,
										  run_blocks, &published_pblkno,
											  skipFsync);
		if (published_blocks != run_blocks || published_pblkno != pblkno)
			elog(ERROR,
				 "identity Umbra map run for fork %d block %u published %u blocks at physical block %u",
				 (int) forknum, lblkno, published_blocks, published_pblkno);

		lblkno += run_blocks;
	}
	(void) ummap_set_nblocks(ctx, reln->smgr_rlocator, forknum,
							 blocknum + 1, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	remblocks;

	Assert(nblocks > 0);
	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_zeroextend(ctx, forknum, blocknum, nblocks, skipFsync);
		return;
	}

	remblocks = nblocks;
	while (remblocks > 0)
	{
		BlockNumber published_pblkno;
		BlockNumber published_blocks;
		BlockNumber run_blocks;

		run_blocks = ummap_identity_run_limit(forknum, blocknum, remblocks,
											  &pblkno);
		umfile_zeroextend(ctx, forknum, pblkno, run_blocks, skipFsync);
		published_blocks = ummap_set_identity_run(ctx, reln->smgr_rlocator,
											  forknum,
											  blocknum, run_blocks,
												  &published_pblkno,
												  skipFsync);
		if (published_blocks != run_blocks || published_pblkno != pblkno)
			elog(ERROR,
				 "identity Umbra map run for fork %d block %u published %u blocks at physical block %u",
				 (int) forknum, blocknum, published_blocks, published_pblkno);
		(void) ummap_set_nblocks(ctx, reln->smgr_rlocator, forknum,
								 blocknum + run_blocks, skipFsync);

		blocknum += run_blocks;
		remblocks -= run_blocks;
	}
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	remblocks;

	Assert(nblocks > 0);
	if (!um_fork_uses_map(reln, forknum))
		return umfile_prefetch(ctx, forknum, blocknum, nblocks);

	remblocks = nblocks;
	while (remblocks > 0)
	{
		BlockNumber run_blocks;

		run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
									  blocknum, remblocks, &pblkno);
		if (!umfile_prefetch(ctx, forknum, pblkno, run_blocks))
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
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_readv(ctx, forknum, blocknum, buffers, nblocks);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
									  blocknum, nblocks, &pblkno);
		umfile_readv(ctx, forknum, pblkno, buffers, run_blocks);

		blocknum += run_blocks;
		buffers += run_blocks;
		nblocks -= run_blocks;
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks,
								 false);
		umfile_startreadv_physical(ioh, ctx, forknum, blocknum, blocknum,
								   buffers, nblocks);
		return;
	}

	run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator,
									  forknum, blocknum, nblocks, &pblkno);
	Assert(run_blocks == nblocks);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, run_blocks,
							 false);
	umfile_startreadv_physical(ioh, ctx, forknum,
								   blocknum, pblkno, buffers, run_blocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_writev(ctx, forknum, blocknum, buffers, nblocks, skipFsync);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
									  blocknum, nblocks, &pblkno);
		umfile_writev(ctx, forknum, pblkno, buffers, run_blocks, skipFsync);

		blocknum += run_blocks;
		buffers += run_blocks;
		nblocks -= run_blocks;
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;
	BlockNumber	run_blocks;

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_writeback(ctx, forknum, blocknum, nblocks);
		return;
	}

	while (nblocks > 0)
	{
		run_blocks = ummap_lookup_run(ctx, reln->smgr_rlocator, forknum,
									  blocknum, nblocks, &pblkno);
		umfile_writeback(ctx, forknum, pblkno, run_blocks);

		blocknum += run_blocks;
		nblocks -= run_blocks;
	}
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber nblocks;
	BlockNumber physical_nblocks;

	nblocks = um_get_logical_nblocks(reln, forknum);
	if (um_fork_uses_map(reln, forknum))
	{
		/*
		 * smgrtruncate() runs in a critical section after smgrnblocks() has
		 * returned.  Open every active data and MAP segment here so truncate
		 * does not allocate or open files while it is in that critical section.
		 *
		 * This patch uses identity mappings, so a mapped relation's visible EOF
		 * cannot exceed its physical EOF.  Unlogged and temp relations bypass
		 * this path entirely.
		 */
		physical_nblocks = umfile_nblocks(ctx, forknum);
		if (ummap_exists(ctx))
			(void) umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
		return Min(nblocks, physical_nblocks);
	}

	return nblocks;
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (!um_fork_uses_map(reln, forknum))
	{
		umfile_truncate(ctx, forknum, old_blocks, nblocks);
		return;
	}

	if (nblocks >= old_blocks)
	{
		umfile_truncate(ctx, forknum, old_blocks, nblocks);
		return;
	}

	/*
	 * On shrink, publish the smaller logical/physical frontier before removing
	 * physical storage, so an ERROR after truncation cannot leave MAP metadata
	 * pointing past the materialized file.
	 */
	if (!ummap_set_nblocks(ctx, reln->smgr_rlocator, forknum, nblocks,
							 false))
		return;
	MapFlushRelation(ctx, reln->smgr_rlocator);
	umfile_truncate(ctx, forknum, old_blocks, nblocks);
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
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	pblkno;

	if (um_fork_uses_map(reln, forknum))
		pblkno = ummap_lookup_block(ctx, reln->smgr_rlocator, forknum,
									 blocknum);
	else
		pblkno = blocknum;
	return umfile_fd(ctx, forknum, pblkno, off);
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
	UmbraFileContext *ctx = um_get_filectx(reln);

	if (um_fork_uses_map(reln, forknum))
		return ummap_nblocks(ctx, reln->smgr_rlocator, forknum);

	return umfile_nblocks(ctx, forknum);
}

static UmbraFileContext *
um_get_filectx(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	Assert(state->filectx != NULL);

	return state->filectx;
}
