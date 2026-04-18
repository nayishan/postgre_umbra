/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager skeleton.
 *
 * This file establishes Umbra as a separate smgr implementation from md.c.
 * Data-fork operations remain md-backed here, while relation-local metadata
 * file operations go through umfile.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/md.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/umbra.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	UmbraFileContext *filectx;
} UmbraSmgrRelationState;

static UmbraFileContext *um_relation_filectx(SMgrRelation reln);

bool
UmMetadataExists(SMgrRelation reln)
{
	return umfile_exists(um_relation_filectx(reln),
						 UMBRA_METADATA_FORKNUM,
						 UMFILE_EXISTS_DENSE);
}

bool
UmMetadataOpenOrCreate(SMgrRelation reln, bool isRedo, bool *created)
{
	return umfile_open_or_create(um_relation_filectx(reln),
								 UMBRA_METADATA_FORKNUM,
								 isRedo,
								 created);
}

BlockNumber
UmMetadataNblocks(SMgrRelation reln)
{
	return umfile_nblocks(um_relation_filectx(reln),
						  UMBRA_METADATA_FORKNUM,
						  UMFILE_NBLOCKS_DENSE);
}

void
UmMetadataRead(SMgrRelation reln, BlockNumber blkno, void *buffer)
{
	void	   *buffers[1];

	buffers[0] = buffer;
	umfile_readv(um_relation_filectx(reln), UMBRA_METADATA_FORKNUM, blkno,
				 buffers, 1);
}

void
UmMetadataWrite(SMgrRelation reln, BlockNumber blkno, const void *buffer,
				bool skipFsync)
{
	const void *buffers[1];

	buffers[0] = buffer;
	umfile_writev(um_relation_filectx(reln), UMBRA_METADATA_FORKNUM, blkno,
				  buffers, 1, skipFsync);
}

void
UmMetadataExtend(SMgrRelation reln, BlockNumber blkno, const void *buffer,
				 bool skipFsync)
{
	umfile_extend(um_relation_filectx(reln), UMBRA_METADATA_FORKNUM, blkno,
				  buffer, skipFsync);
}

void
UmMetadataImmediateSync(SMgrRelation reln)
{
	umfile_immedsync(um_relation_filectx(reln), UMBRA_METADATA_FORKNUM);
}

void
UmMetadataUnlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

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
	state->filectx = umfile_ctx_acquire(reln->smgr_rlocator);
	reln->smgr_private = state;

	mdopen(reln);
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	mdclose(reln, forknum);
}

void
umdestroy(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	umfile_ctx_forget(reln->smgr_rlocator);

	if (state != NULL)
	{
		pfree(state);
		reln->smgr_private = NULL;
	}
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	mdcreate(reln, forknum, isRedo);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	return mdexists(reln, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	umfile_ctx_forget(rlocator);

	if (forknum == UMBRA_METADATA_FORKNUM)
	{
		UmMetadataUnlink(rlocator, isRedo);
		return;
	}

	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
		UmMetadataUnlink(rlocator, isRedo);

	mdunlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	mdextend(reln, forknum, blocknum, buffer, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	mdzeroextend(reln, forknum, blocknum, nblocks, skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	return mdprefetch(reln, forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return mdmaxcombine(reln, forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	mdreadv(reln, forknum, blocknum, buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	mdwritev(reln, forknum, blocknum, buffers, nblocks, skipFsync);
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	mdwriteback(reln, forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	return mdnblocks(reln, forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	mdtruncate(reln, forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	mdimmedsync(reln, forknum);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	mdregistersync(reln, forknum);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	return mdfd(reln, forknum, blocknum, off);
}

static UmbraFileContext *
um_relation_filectx(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL)
		return umfile_ctx_acquire(reln->smgr_rlocator);

	if (state->filectx == NULL)
		state->filectx = umfile_ctx_acquire(reln->smgr_rlocator);

	return state->filectx;
}
