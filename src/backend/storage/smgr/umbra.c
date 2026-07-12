/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager skeleton.
 *
 * This file establishes Umbra as a separate smgr implementation from md.c.
 * The initial implementation preserves md semantics by forwarding relation
 * file operations to md.c.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/md.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	/*
	 * State stored in SMgrRelationData.smgr_private.
	 *
	 * Patch 1 keeps Umbra behavior equivalent to md.c, but still exercises the
	 * ownership boundary: Umbra may attach per-relation implementation state
	 * when the SMgrRelation is opened and release it when smgr destroys the
	 * handle.
	 */
	bool		initialized;
} UmbraSmgrRelationState;

void
uminit(void)
{
}

void
umopen(SMgrRelation reln)
{
	UmbraSmgrRelationState *state;

	Assert(reln->smgr_private == NULL);

	state = MemoryContextAllocZero(TopMemoryContext,
								   sizeof(UmbraSmgrRelationState));
	state->initialized = true;
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

	if (state != NULL)
	{
		Assert(state->initialized);
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
