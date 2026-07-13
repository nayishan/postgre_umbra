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

	old_nblocks = umfile_nblocks(ctx, forknum);
	pblkno = blocknum;
	umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
	if (!ummap_tracks_fork(forknum))
		return;

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
		published_blocks = ummap_set_identity_run(ctx, forknum, lblkno,
											  run_blocks, &published_pblkno,
											  skipFsync);
		if (published_blocks != run_blocks || published_pblkno != pblkno)
			elog(ERROR,
				 "identity Umbra map run for fork %d block %u published %u blocks at physical block %u",
				 (int) forknum, lblkno, published_blocks, published_pblkno);

		lblkno += run_blocks;
	}
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
		published_blocks = ummap_set_identity_run(ctx, forknum,
												  blocknum, run_blocks,
												  &published_pblkno,
												  skipFsync);
		if (published_blocks != run_blocks || published_pblkno != pblkno)
			elog(ERROR,
				 "identity Umbra map run for fork %d block %u published %u blocks at physical block %u",
				 (int) forknum, blocknum, published_blocks, published_pblkno);

		blocknum += run_blocks;
		remblocks -= run_blocks;
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

		run_blocks = ummap_lookup_run(um_get_filectx(reln), forknum,
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

	logical_nblocks = umfile_nblocks(ctx, forknum);
	if (blocknum >= logical_nblocks)
		return 1;

	/* Do not ask the MAP layer to inspect entries beyond the relation EOF. */
	logical_run = Min(RELSEG_SIZE -
					  (blocknum % ((BlockNumber) RELSEG_SIZE)),
					  logical_nblocks - blocknum);
	map_run = ummap_lookup_run(ctx, forknum, blocknum, logical_run, &pblkno);
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
		run_blocks = ummap_lookup_run(um_get_filectx(reln), forknum,
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

	run_blocks = ummap_lookup_run(um_get_filectx(reln), forknum, blocknum,
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
		run_blocks = ummap_lookup_run(um_get_filectx(reln), forknum,
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
		run_blocks = ummap_lookup_run(um_get_filectx(reln), forknum,
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
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_immedsync(ctx, forknum);
	if (um_fork_uses_map(reln, forknum))
		ummap_immedsync_if_exists(ctx);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_registersync(ctx, forknum);
	if (um_fork_uses_map(reln, forknum))
		ummap_registersync_if_exists(ctx);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	BlockNumber	pblkno;

	if (um_fork_uses_map(reln, forknum))
		pblkno = ummap_lookup_block(um_get_filectx(reln), forknum, blocknum);
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
