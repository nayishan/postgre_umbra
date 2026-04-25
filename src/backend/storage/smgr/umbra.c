/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager skeleton.
 *
 * This file establishes Umbra as a separate smgr implementation from md.c.
 * maintains identity mapping state (logical block number == physical block
 * number) in the relation-local metadata file while using md.c for data-fork
 * I/O and umfile for metadata-file I/O.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "storage/md.h"
#include "storage/mapsuper.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/umbra.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	UmbraFileContext *filectx;
} UmbraSmgrRelationState;

static bool um_tracks_identity_metadata(ForkNumber forknum);
static UmbraFileContext *um_relation_filectx(SMgrRelation reln);
static void um_identity_update_metadata(SMgrRelation reln, ForkNumber forknum,
										BlockNumber nblocks, bool fork_exists,
										bool skipFsync);

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

	umfile_ctx_release(reln->smgr_rlocator);

	if (state != NULL)
	{
		pfree(state);
		reln->smgr_private = NULL;
	}
}

bool
umisinternalfork(ForkNumber forknum)
{
	return forknum == UMBRA_METADATA_FORKNUM;
}

void
umcreaterelationmetadata(SMgrRelation reln)
{
	bool		created = false;

	if (!UmMetadataOpenOrCreate(reln, false, &created))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create Umbra metadata fork for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));
}

void
umcopyrelationmetadata(SMgrRelation src, SMgrRelation dst, char relpersistence)
{
	BlockNumber src_nblocks;
	BlockNumber dst_nblocks;
	PGIOAlignedBlock pagebuf;
	bool		created = false;

	if (relpersistence != RELPERSISTENCE_PERMANENT)
		return;

	if (!UmMetadataExists(src))
		return;

	if (!UmMetadataOpenOrCreate(dst, false, &created))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create Umbra metadata fork for relation %u/%u/%u",
						dst->smgr_rlocator.locator.spcOid,
						dst->smgr_rlocator.locator.dbOid,
						dst->smgr_rlocator.locator.relNumber)));

	src_nblocks = UmMetadataNblocks(src);
	dst_nblocks = UmMetadataNblocks(dst);

	for (BlockNumber blkno = 0; blkno < src_nblocks; blkno++)
	{
		UmMetadataRead(src, blkno, pagebuf.data);
		if (blkno < dst_nblocks)
			UmMetadataWrite(dst, blkno, pagebuf.data, true);
		else
			UmMetadataExtend(dst, blkno, pagebuf.data, true);
	}

	UmMetadataImmediateSync(dst);
}

void
umsyncrelationmetadata(SMgrRelation reln)
{
	if (!UmMetadataExists(reln))
		return;

	UmMetadataImmediateSync(reln);
}

void
umunlinkrelationmetadata(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_ctx_forget(rlocator);
	UmMetadataUnlink(rlocator, isRedo);
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	mdcreate(reln, forknum, isRedo);

	if (um_tracks_identity_metadata(forknum))
		um_identity_update_metadata(reln, forknum, 0, true, true);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum == UMBRA_METADATA_FORKNUM)
		return UmMetadataExists(reln);

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

	if (um_tracks_identity_metadata(forknum))
		um_identity_update_metadata(reln, forknum, blocknum + 1, true,
									skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	target_nblocks;

	mdzeroextend(reln, forknum, blocknum, nblocks, skipFsync);

	if (um_tracks_identity_metadata(forknum))
	{
		target_nblocks = blocknum + (BlockNumber) nblocks;
		if (target_nblocks < blocknum)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra identity mapping block count overflow")));
		um_identity_update_metadata(reln, forknum, target_nblocks, true,
									skipFsync);
	}
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
	/*
	 * Keep md.c responsible for the physical fork size query. mdtruncate()
	 * relies on a preceding mdnblocks() call to have opened all active
	 * segments.
	 */
	return mdnblocks(reln, forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	mdtruncate(reln, forknum, old_blocks, nblocks);

	if (um_tracks_identity_metadata(forknum))
		um_identity_update_metadata(reln, forknum, nblocks, true, false);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	mdimmedsync(reln, forknum);

	if (um_tracks_identity_metadata(forknum) && UmMetadataExists(reln))
		UmMetadataImmediateSync(reln);
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

static bool
um_tracks_identity_metadata(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM ||
		forknum == FSM_FORKNUM ||
		forknum == VISIBILITYMAP_FORKNUM;
}

static void
um_identity_update_metadata(SMgrRelation reln, ForkNumber forknum,
							BlockNumber nblocks, bool fork_exists,
							bool skipFsync)
{
	MapSuperblock super;

	Assert(reln != NULL);
	Assert(um_tracks_identity_metadata(forknum));

	if (!MapSBlockRead(reln, &super))
		MapSuperblockInit(&super, 0);

	if (!fork_exists && forknum != MAIN_FORKNUM)
	{
		MapSuperblockSetLogicalNblocks(&super, forknum, InvalidBlockNumber);
		MapSuperblockSetNextFreePhysBlock(&super, forknum, InvalidBlockNumber);
		MapSuperblockSetPhysCapacity(&super, forknum, InvalidBlockNumber);
	}
	else
	{
		MapSuperblockSetLogicalNblocks(&super, forknum, nblocks);
		MapSuperblockSetNextFreePhysBlock(&super, forknum, nblocks);
		MapSuperblockSetPhysCapacity(&super, forknum, nblocks);
	}

	MapSuperblockSetLastUpdatedLSN(&super, InvalidXLogRecPtr);
	MapSBlockWrite(reln, &super, skipFsync);
}
