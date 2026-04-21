/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager skeleton.
 *
 * This file establishes Umbra as a separate smgr implementation from md.c. It
 * maintains relation-local metadata and MAP checkpoint/cache state while using
 * md.c for data-fork I/O and umfile for metadata-file I/O.
 *
 * src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "common/relpath.h"
#include "storage/bufmgr.h"
#include "storage/map.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/umbra.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

typedef struct UmbraSmgrRelationState
{
	UmbraFileContext *filectx;
} UmbraSmgrRelationState;

static bool um_tracks_identity_metadata(ForkNumber forknum);
static UmbraFileContext *um_relation_filectx(SMgrRelation reln);
static void um_ensure_redo_metadata(SMgrRelation reln, ForkNumber forknum);
static void um_identity_update_metadata(SMgrRelation reln, ForkNumber forknum,
										BlockNumber nblocks, bool fork_exists);
static void um_refresh_identity_metadata(SMgrRelation reln);
static void um_filetag_path(const FileTag *ftag, char *path);

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
	UmbraFileContext *ctx = um_relation_filectx(reln);

	umfile_ctx_write(ctx, UMBRA_METADATA_FORKNUM, blkno,
					 buffer, BLCKSZ, skipFsync);
	umfile_ctx_register_dirty(ctx, UMBRA_METADATA_FORKNUM, blkno,
							  skipFsync,
							  RelFileLocatorBackendIsTemp(reln->smgr_rlocator));
}

void
UmMetadataWriteSuperblock(RelFileLocatorBackend rlocator, const void *sector,
						  bool skipFsync)
{
	UmbraFileContext *ctx = umfile_ctx_acquire(rlocator);

	/*
	 * Superblock checkpoint flush can run while holding MapSuperEntry->lock,
	 * so it must not recurse through smgr/umopen.
	 */
	umfile_ctx_write(ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
					 sector, MAP_SUPERBLOCK_SIZE, skipFsync);
	umfile_ctx_register_dirty(ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
							  skipFsync,
							  RelFileLocatorBackendIsTemp(rlocator));
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
	MapCheckpointRelation(reln->smgr_rlocator.locator);
	umfile_immedsync(um_relation_filectx(reln), UMBRA_METADATA_FORKNUM);
}

void
UmMetadataUnlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

void
UmInvalidateDatabase(Oid dbid)
{
	FileTag		tag;
	RelFileLocator rlocator;

	MapInvalidateDatabase(dbid);

	rlocator.spcOid = 0;
	rlocator.dbOid = dbid;
	rlocator.relNumber = 0;

	memset(&tag, 0, sizeof(tag));
	tag.handler = SYNC_HANDLER_UMBRA;
	tag.rlocator = rlocator;
	tag.forknum = InvalidForkNumber;
	tag.segno = InvalidBlockNumber;

	RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true);
}

void
uminit(void)
{
	umfile_init();
	MapBackendInit();
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

	if (state != NULL)
	{
		umfile_ctx_forget(reln->smgr_rlocator);
		pfree(state);
		reln->smgr_private = NULL;
	}
}

bool
umisinternalfork(ForkNumber forknum)
{
	return forknum == UMBRA_METADATA_FORKNUM;
}

bool
umcreatedballowswallog(void)
{
	return false;
}

void
umcheckpointdatabasetablespaces(Oid dbid, int ntablespaces,
								const Oid *tablespace_ids)
{
	MapCheckpointDatabaseTablespaces(dbid, ntablespaces, tablespace_ids);
}

void
uminvalidatedatabasetablespaces(Oid dbid, int ntablespaces,
								const Oid *tablespace_ids)
{
	MapInvalidateDatabaseTablespaces(dbid, ntablespaces, tablespace_ids);
}

void
umcreaterelationmetadata(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_relation_filectx(reln);
	bool		created = false;

	/*
	 * smgrcreaterelationmetadata() is used both in normal create and redo
	 * paths, so tolerate an already-existing metadata fork here.
	 */
	if (!UmMetadataOpenOrCreate(reln, true, &created))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create Umbra metadata fork for relation %u/%u/%u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber)));

	elog(DEBUG1, "umbra metadata open/create %u/%u/%u created=%s",
		 reln->smgr_rlocator.locator.spcOid,
		 reln->smgr_rlocator.locator.dbOid,
		 reln->smgr_rlocator.locator.relNumber,
		 created ? "true" : "false");

	if (created)
		MapSBlockInit(ctx, reln->smgr_rlocator.locator, InvalidXLogRecPtr);
	else
		(void) MapSBlockEnsureLoaded(ctx, reln->smgr_rlocator.locator);

	um_refresh_identity_metadata(reln);
}

void
umcopyrelationmetadata(SMgrRelation src, SMgrRelation dst, char relpersistence)
{
	BlockNumber src_nblocks;
	BlockNumber dst_nblocks;
	PGIOAlignedBlock pagebuf;

	if (relpersistence != RELPERSISTENCE_PERMANENT)
		return;

	if (!UmMetadataExists(src))
		return;

	umcreaterelationmetadata(dst);

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
	MapInvalidateRelation(rlocator.locator);
	UmMetadataUnlink(rlocator, isRedo);
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	mdcreate(reln, forknum, isRedo);

	/*
	 * Redo for permanent relation creation reaches smgrcreate() directly, so
	 * make sure the metadata fork exists before later recovery steps touch the
	 * relation again.
	 */
	if (isRedo &&
		forknum == MAIN_FORKNUM &&
		!UmMetadataExists(reln))
		umcreaterelationmetadata(reln);

	if (forknum != MAIN_FORKNUM &&
		um_tracks_identity_metadata(forknum) &&
		UmMetadataExists(reln))
		um_identity_update_metadata(reln, forknum, 0, true);
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
	if (forknum == UMBRA_METADATA_FORKNUM ||
		forknum == MAIN_FORKNUM ||
		forknum == InvalidForkNumber)
	{
		MapInvalidateRelation(rlocator.locator);
	}

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
	um_ensure_redo_metadata(reln, forknum);
	mdextend(reln, forknum, blocknum, buffer, skipFsync);

	if (um_tracks_identity_metadata(forknum) && UmMetadataExists(reln))
		um_identity_update_metadata(reln, forknum, blocknum + 1, true);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	BlockNumber	target_nblocks;

	um_ensure_redo_metadata(reln, forknum);
	mdzeroextend(reln, forknum, blocknum, nblocks, skipFsync);

	if (!um_tracks_identity_metadata(forknum) || !UmMetadataExists(reln))
		return;

	target_nblocks = blocknum + (BlockNumber) nblocks;
	if (target_nblocks < blocknum)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra identity mapping block count overflow")));

	um_identity_update_metadata(reln, forknum, target_nblocks, true);
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
	um_ensure_redo_metadata(reln, forknum);
	mdreadv(reln, forknum, blocknum, buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	um_ensure_redo_metadata(reln, forknum);
	mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	um_ensure_redo_metadata(reln, forknum);
	mdwritev(reln, forknum, blocknum, buffers, nblocks, skipFsync);

	if (InRecovery &&
		um_tracks_identity_metadata(forknum) &&
		UmMetadataExists(reln))
		um_identity_update_metadata(reln, forknum, mdnblocks(reln, forknum),
									true);
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
	 * Keep md.c responsible for physical fork size queries. mdtruncate()
	 * relies on a preceding mdnblocks() call to have opened active segments.
	 */
	return mdnblocks(reln, forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	mdtruncate(reln, forknum, old_blocks, nblocks);

	if (um_tracks_identity_metadata(forknum) && UmMetadataExists(reln))
		um_identity_update_metadata(reln, forknum, nblocks, true);
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

int
umsyncfiletag(const FileTag *ftag, char *path)
{
	File		fd;
	int			ret;
	int			save_errno;

	um_filetag_path(ftag, path);

	fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return -1;

	ret = FileSync(fd, WAIT_EVENT_DATA_FILE_SYNC);
	save_errno = errno;

	FileClose(fd);
	errno = save_errno;
	return ret;
}

int
umunlinkfiletag(const FileTag *ftag, char *path)
{
	um_filetag_path(ftag, path);
	return unlink(path);
}

bool
umfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
	if (ftag->forknum == InvalidForkNumber &&
		ftag->segno == InvalidBlockNumber &&
		ftag->rlocator.spcOid == 0 &&
		ftag->rlocator.relNumber == 0)
		return ftag->rlocator.dbOid == candidate->rlocator.dbOid;

	if (ftag->forknum == InvalidForkNumber &&
		ftag->segno == InvalidBlockNumber)
		return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator);

	if (ftag->segno == InvalidBlockNumber)
		return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator) &&
			ftag->forknum == candidate->forknum;

	return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator) &&
		ftag->forknum == candidate->forknum &&
		ftag->segno == candidate->segno;
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
um_ensure_redo_metadata(SMgrRelation reln, ForkNumber forknum)
{
	Assert(reln != NULL);

	if (!InRecovery ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!um_tracks_identity_metadata(forknum) ||
		UmMetadataExists(reln))
		return;

	/*
	 * Redo can materialize a new data fork via mdwritev()/mdextend() without a
	 * preceding smgrcreate() callback, for example during CREATE DATABASE
	 * WAL-log replay. Ensure metadata exists before MAP state is consulted or
	 * checkpointed for that relation.
	 */
	elog(DEBUG1, "umbra redo ensure metadata %u/%u/%u fork=%d",
		 reln->smgr_rlocator.locator.spcOid,
		 reln->smgr_rlocator.locator.dbOid,
		 reln->smgr_rlocator.locator.relNumber,
		 forknum);
	umcreaterelationmetadata(reln);
}

static void
um_identity_update_metadata(SMgrRelation reln, ForkNumber forknum,
							BlockNumber nblocks, bool fork_exists)
{
	UmbraFileContext *ctx = um_relation_filectx(reln);
	BlockNumber logical_nblocks;

	Assert(reln != NULL);
	Assert(um_tracks_identity_metadata(forknum));
	Assert(UmMetadataExists(reln));

	if (!MapSBlockEnsureLoaded(ctx, reln->smgr_rlocator.locator))
		elog(ERROR, "could not load MAP superblock for relation %u/%u/%u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber);

	if (!fork_exists && forknum != MAIN_FORKNUM)
		logical_nblocks = InvalidBlockNumber;
	else
		logical_nblocks = nblocks;

	MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
							   forknum, logical_nblocks,
							   InvalidXLogRecPtr);

	if (fork_exists || forknum == MAIN_FORKNUM)
	{
		MapSBlockBumpNextFreePhysBlock(ctx, reln->smgr_rlocator.locator,
									   forknum, nblocks,
									   InvalidXLogRecPtr);
		MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
									 forknum, nblocks,
									 InvalidXLogRecPtr);
	}
}

static void
um_refresh_identity_metadata(SMgrRelation reln)
{
	ForkNumber	forknum;

	Assert(UmMetadataExists(reln));

	for (forknum = MAIN_FORKNUM; forknum <= VISIBILITYMAP_FORKNUM; forknum++)
	{
		bool		fork_exists;
		BlockNumber nblocks;

		if (!um_tracks_identity_metadata(forknum))
			continue;

		fork_exists = mdexists(reln, forknum);
		nblocks = fork_exists ? mdnblocks(reln, forknum) : 0;
		um_identity_update_metadata(reln, forknum, nblocks, fork_exists);
	}
}

static void
um_filetag_path(const FileTag *ftag, char *path)
{
	RelPathStr	base;

	if (ftag->forknum == UMBRA_METADATA_FORKNUM)
		base = UmMetadataRelPathPerm(ftag->rlocator);
	else
		base = relpathperm(ftag->rlocator, ftag->forknum);

	if (ftag->segno == 0)
		strlcpy(path, base.str, MAXPGPATH);
	else
		snprintf(path, MAXPGPATH, "%s.%llu",
				 base.str, (unsigned long long) ftag->segno);
}
