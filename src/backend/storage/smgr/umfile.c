/*-------------------------------------------------------------------------
 *
 * umfile.c
 *	  Umbra backend-local file/segment helpers.
 *
 * This layer owns backend-local file contexts keyed by RelFileLocatorBackend
 * and provides physical fork/segment management beneath Umbra metadata and
 * mapping code.
 *
 * src/backend/storage/smgr/umfile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/um_defs.h"
#include "storage/umfile.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/* Behavior flags for segment open helpers. */
#define UM_EXTENSION_FAIL				(1 << 0)
#define UM_EXTENSION_RETURN_NULL		(1 << 1)
#define UM_EXTENSION_CREATE				(1 << 2)
#define UM_EXTENSION_CREATE_RECOVERY	(1 << 3)
#define UM_EXTENSION_DONT_OPEN			(1 << 5)

typedef struct UmCtxRegistryEntry
{
	RelFileLocatorBackend rlocator;
	UmbraFileContext *ctx;
} UmCtxRegistryEntry;

typedef struct UmfdVec
{
	File		umfd_vfd;
	BlockNumber	umfd_segno;
} UmfdVec;

struct UmbraFileContext
{
	RelFileLocatorBackend rlocator;
	int			num_open_segs[UMBRA_FORK_SLOTS];
	UmfdVec    *seg_fds[UMBRA_FORK_SLOTS];
	uint32		refcount;
};

static MemoryContext UmFileCxt = NULL;
static HTAB *UmFileContextHash = NULL;

static void umfile_ctx_registry_init(void);
static UmbraFileContext *umfile_ctx_create(RelFileLocatorBackend rlocator);
static void umfile_ctx_destroy(UmbraFileContext *ctx);
static void umfile_close_open_segments(UmbraFileContext *ctx,
									   ForkNumber forknum);
static bool umfile_create(UmbraFileContext *ctx, ForkNumber forknum,
						  bool isRedo);
static int	umfile_open_flags(void);
static void umfile_fdvec_resize(UmbraFileContext *ctx, ForkNumber forknum,
								int nseg);
static inline UmfdVec *umfile_v_get(UmbraFileContext *ctx,
									ForkNumber forknum, int segindex);
static BlockNumber umfile_nblocks_in_seg(File vfd);
static RelPathStr umfile_segpath(RelFileLocatorBackend rlocator,
								 ForkNumber forknum, BlockNumber segno);
static UmfdVec *umfile_openseg(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator,
							   ForkNumber forknum,
							   BlockNumber segno, int oflags);
static UmfdVec *umfile_openfork(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator,
								ForkNumber forknum, int behavior);
static UmfdVec *umfile_getseg(UmbraFileContext *ctx,
							  RelFileLocatorBackend rlocator,
							  ForkNumber forknum, BlockNumber blkno,
							  bool skipFsync, int behavior);
static bool umfile_fork_has_open_segment(UmbraFileContext *ctx,
										 ForkNumber forknum);
static bool umfile_fork_has_open_segment_on_disk(UmbraFileContext *ctx,
												 RelFileLocatorBackend rlocator,
												 ForkNumber forknum);
static inline bool umfile_seg_entry_is_open(const UmfdVec *seg);
static inline void umfile_seg_entry_reset(UmfdVec *seg);

void
umfile_init(void)
{
	HASHCTL		ctl;

	if (UmFileContextHash != NULL)
		return;

	UmFileCxt = AllocSetContextCreate(TopMemoryContext,
									  "UmFile",
									  ALLOCSET_DEFAULT_SIZES);
	MemoryContextAllowInCriticalSection(UmFileCxt, true);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelFileLocatorBackend);
	ctl.entrysize = sizeof(UmCtxRegistryEntry);
	ctl.hcxt = UmFileCxt;

	UmFileContextHash = hash_create("Umbra file context registry",
									256,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

UmbraFileContext *
umfile_ctx_lookup(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;

	umfile_ctx_registry_init();
	entry = hash_search(UmFileContextHash, &rlocator, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	return entry->ctx;
}

UmbraFileContext *
umfile_ctx_acquire(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;
	bool		found;

	umfile_ctx_registry_init();
	entry = hash_search(UmFileContextHash, &rlocator, HASH_ENTER, &found);
	if (!found)
		entry->ctx = umfile_ctx_create(rlocator);
	entry->ctx->refcount++;

	return entry->ctx;
}

UmbraFileContext *
umfile_ctx_create_temporary(RelFileLocatorBackend rlocator)
{
	umfile_ctx_registry_init();
	return umfile_ctx_create(rlocator);
}

void
umfile_ctx_destroy_temporary(UmbraFileContext *ctx)
{
	if (ctx == NULL)
		return;

	umfile_ctx_destroy(ctx);
}

void
umfile_ctx_release(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;
	UmbraFileContext *ctx;

	if (UmFileContextHash == NULL)
		return;

	entry = hash_search(UmFileContextHash, &rlocator, HASH_FIND, NULL);
	if (entry == NULL)
		return;

	ctx = entry->ctx;
	Assert(ctx->refcount > 0);
	ctx->refcount--;

	if (ctx->refcount == 0)
	{
		umfile_ctx_destroy(ctx);
		(void) hash_search(UmFileContextHash, &rlocator, HASH_REMOVE, NULL);
	}
}

void
umfile_ctx_forget(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;
	UmbraFileContext *ctx;

	if (UmFileContextHash == NULL)
		return;

	entry = hash_search(UmFileContextHash, &rlocator, HASH_FIND, NULL);
	if (entry == NULL)
		return;

	ctx = entry->ctx;
	for (ForkNumber forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
		umfile_close_open_segments(ctx, forknum);

	if (ctx->refcount == 0)
	{
		umfile_ctx_destroy(ctx);
		(void) hash_search(UmFileContextHash, &rlocator, HASH_REMOVE, NULL);
	}
}

void
umfile_ctx_close_fork(UmbraFileContext *ctx, ForkNumber forknum)
{
	if (ctx == NULL)
		return;

	umfile_close_open_segments(ctx, forknum);
}

bool
umfile_ctx_fork_exists(UmbraFileContext *ctx, ForkNumber forknum,
					   UmFileExistsMode mode)
{
	if (ctx == NULL)
		return false;

	return umfile_exists(ctx, forknum, mode);
}

BlockNumber
umfile_ctx_get_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
					   UmFileNblocksMode mode)
{
	Assert(ctx != NULL);
	return umfile_nblocks(ctx, forknum, mode);
}

void
umfile_ctx_read(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				char *buffer, int nbytes)
{
	UmfdVec    *seg;
	off_t		offset;
	ssize_t		got;

	Assert(ctx != NULL);
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	seg = umfile_getseg(ctx, ctx->rlocator, forknum, blkno,
						false,
						UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY);
	offset = (off_t) BLCKSZ * (blkno % ((BlockNumber) RELSEG_SIZE));
	got = FileRead(seg->umfd_vfd, buffer, nbytes, offset,
				   WAIT_EVENT_DATA_FILE_READ);
	if (got < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read block %u in file \"%s\": %m",
						blkno, FilePathName(seg->umfd_vfd))));
	if (got != nbytes)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read block %u in file \"%s\"",
						blkno, FilePathName(seg->umfd_vfd)),
				 errdetail("Read only %zd of %d bytes.", got, nbytes)));
}

void
umfile_ctx_write(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				 const char *buffer, int nbytes, bool skipFsync)
{
	UmfdVec    *seg;
	BlockNumber	nblocks;
	off_t		offset;
	ssize_t		wrote;

	Assert(ctx != NULL);
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	nblocks = umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);
	if (blkno >= nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot overwrite block %u in relation %u/%u/%u fork %d",
						blkno,
						ctx->rlocator.locator.spcOid,
						ctx->rlocator.locator.dbOid,
						ctx->rlocator.locator.relNumber,
						forknum),
				 errdetail("Current fork size is %u blocks.", nblocks)));

	seg = umfile_getseg(ctx, ctx->rlocator, forknum, blkno,
						skipFsync,
						UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY);
	offset = (off_t) BLCKSZ * (blkno % ((BlockNumber) RELSEG_SIZE));
	wrote = FileWrite(seg->umfd_vfd, buffer, nbytes, offset,
					  WAIT_EVENT_DATA_FILE_WRITE);
	if (wrote < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write block %u in file \"%s\": %m",
						blkno, FilePathName(seg->umfd_vfd))));
	if (wrote != nbytes)
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\"",
						blkno, FilePathName(seg->umfd_vfd)),
				 errdetail("Wrote only %zd of %d bytes.", wrote, nbytes)));

	/*
	 * Sync policy is explicit at this layer: callers use
	 * umfile_registersync()/umfile_immedsync() for durable requests.
	 */
	(void) skipFsync;
}

void
umfile_ctx_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				  const char *buffer)
{
	BlockNumber	nblocks;

	Assert(ctx != NULL);
	Assert(buffer != NULL);

	(void) umfile_open_or_create(ctx, forknum, false, NULL);
	nblocks = umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);
	if (blkno != nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot extend relation %u/%u/%u fork %d at block %u",
						ctx->rlocator.locator.spcOid,
						ctx->rlocator.locator.dbOid,
						ctx->rlocator.locator.relNumber,
						forknum, blkno),
				 errdetail("Expected next block %u.", nblocks)));

	umfile_extend(ctx, forknum, blkno, buffer, true);
}

void
umfile_ctx_unlinkfork(RelFileLocatorBackend rlocator, ForkNumber forknum,
					  bool isRedo)
{
	umfile_unlink(rlocator, forknum, isRedo);
}

bool
umfile_metadata_exists(UmbraFileContext *ctx)
{
	return umfile_exists(ctx, UMBRA_METADATA_FORKNUM, UMFILE_EXISTS_DENSE);
}

bool
umfile_metadata_open_or_create(UmbraFileContext *ctx, bool isRedo, bool *created)
{
	return umfile_open_or_create(ctx, UMBRA_METADATA_FORKNUM, isRedo, created);
}

BlockNumber
umfile_metadata_nblocks(UmbraFileContext *ctx)
{
	return umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM, UMFILE_NBLOCKS_DENSE);
}

void
umfile_metadata_read(UmbraFileContext *ctx, BlockNumber blkno, void *buffer)
{
	void	   *buffers[1];

	buffers[0] = buffer;
	umfile_readv(ctx, UMBRA_METADATA_FORKNUM, blkno, buffers, 1);
}

void
umfile_metadata_write(UmbraFileContext *ctx, BlockNumber blkno, const void *buffer)
{
	const void *buffers[1];

	buffers[0] = buffer;
	umfile_writev(ctx, UMBRA_METADATA_FORKNUM, blkno, buffers, 1, false);
}

void
umfile_metadata_extend(UmbraFileContext *ctx, BlockNumber blkno, const void *buffer)
{
	umfile_extend(ctx, UMBRA_METADATA_FORKNUM, blkno, buffer, false);
}

void
umfile_metadata_immedsync(UmbraFileContext *ctx)
{
	umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

void
umfile_metadata_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

bool
umfile_exists(UmbraFileContext *ctx, ForkNumber forknum, UmFileExistsMode mode)
{
	Assert(ctx != NULL);
	(void) mode;

	if (umfile_fork_has_open_segment(ctx, forknum))
	{
		if (umfile_fork_has_open_segment_on_disk(ctx, ctx->rlocator, forknum))
			return true;

		umfile_close_open_segments(ctx, forknum);
	}

	return (umfile_openfork(ctx, ctx->rlocator, forknum,
							UM_EXTENSION_RETURN_NULL) != NULL);
}

bool
umfile_open_or_create(UmbraFileContext *ctx, ForkNumber forknum,
					  bool isRedo, bool *created)
{
	UmfdVec    *seg;
	bool		was_created;

	Assert(ctx != NULL);

	if (created != NULL)
		*created = false;

	seg = umfile_openfork(ctx, ctx->rlocator, forknum,
						  UM_EXTENSION_RETURN_NULL);
	if (seg != NULL)
		return true;

	was_created = umfile_create(ctx, forknum, isRedo);
	if (created != NULL)
		*created = was_created;

	return true;
}

BlockNumber
umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum, UmFileNblocksMode mode)
{
	UmfdVec    *seg;
	BlockNumber	segno;
	BlockNumber	nblocks;

	Assert(ctx != NULL);
	(void) mode;

	if (umfile_openfork(ctx, ctx->rlocator, forknum,
						UM_EXTENSION_RETURN_NULL) == NULL)
		return 0;

	Assert(ctx->num_open_segs[forknum] > 0);
	segno = ctx->num_open_segs[forknum] - 1;
	seg = umfile_v_get(ctx, forknum, segno);

	for (;;)
	{
		nblocks = umfile_nblocks_in_seg(seg->umfd_vfd);
		if (nblocks > (BlockNumber) RELSEG_SIZE)
			elog(FATAL, "Umbra segment too big");
		if (nblocks < (BlockNumber) RELSEG_SIZE)
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		segno++;
		seg = umfile_openseg(ctx, ctx->rlocator, forknum, segno, 0);
		if (seg == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

void
umfile_readv(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber nblocks)
{
	for (BlockNumber i = 0; i < nblocks; i++)
		umfile_ctx_read(ctx, forknum, blocknum + i, buffers[i], BLCKSZ);
}

void
umfile_writev(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			  const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	for (BlockNumber i = 0; i < nblocks; i++)
		umfile_ctx_write(ctx, forknum, blocknum + i, buffers[i], BLCKSZ,
						 skipFsync);
}

void
umfile_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			  const void *buffer, bool skipFsync)
{
	UmfdVec    *seg;
	off_t		offset;
	ssize_t		wrote;

	Assert(ctx != NULL);
	Assert(buffer != NULL);

	seg = umfile_getseg(ctx, ctx->rlocator, forknum, blocknum,
						skipFsync,
						UM_EXTENSION_FAIL | UM_EXTENSION_CREATE);
	offset = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	wrote = FileWrite(seg->umfd_vfd, buffer, BLCKSZ, offset,
					  WAIT_EVENT_DATA_FILE_EXTEND);
	if (wrote < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not extend file \"%s\": %m",
						FilePathName(seg->umfd_vfd))));
	if (wrote != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\" at block %u",
						FilePathName(seg->umfd_vfd), blocknum),
				 errdetail("Wrote only %zd of %d bytes.", wrote, BLCKSZ)));

	(void) skipFsync;
}

void
umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum, int nblocks, bool skipFsync)
{
	Assert(ctx != NULL);
	Assert(nblocks >= 0);

	while (nblocks > 0)
	{
		UmfdVec    *seg;
		BlockNumber nblocks_this_segment;
		off_t		offset;
		int			ret;

		seg = umfile_getseg(ctx, ctx->rlocator, forknum, blocknum,
							skipFsync,
							UM_EXTENSION_FAIL | UM_EXTENSION_CREATE);
		offset = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		nblocks_this_segment =
			Min((BlockNumber) nblocks,
				((BlockNumber) RELSEG_SIZE) -
				(blocknum % ((BlockNumber) RELSEG_SIZE)));

		ret = FileZero(seg->umfd_vfd,
					   offset,
					   (off_t) BLCKSZ * nblocks_this_segment,
					   WAIT_EVENT_DATA_FILE_EXTEND);
		if (ret < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not zero-extend file \"%s\": %m",
							FilePathName(seg->umfd_vfd))));

		nblocks -= nblocks_this_segment;
		blocknum += nblocks_this_segment;
	}
}

void
umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
				BlockNumber old_blocks, BlockNumber nblocks)
{
	int			curopensegs;

	Assert(ctx != NULL);

	if (nblocks > old_blocks)
	{
		if (InRecovery)
			return;

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot truncate relation %u/%u/%u fork %d to %u blocks: current size is only %u blocks",
						ctx->rlocator.locator.spcOid,
						ctx->rlocator.locator.dbOid,
						ctx->rlocator.locator.relNumber,
						forknum,
						nblocks,
						old_blocks)));
	}

	if (nblocks == old_blocks)
		return;

	/*
	 * Bring all dense segments into the local array first, then trim from the
	 * tail.  This keeps the truncate contract local to the file manager.
	 */
	(void) umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);
	curopensegs = ctx->num_open_segs[forknum];

	while (curopensegs > 0)
	{
		UmfdVec    *seg;
		BlockNumber	priorblocks;

		priorblocks = (curopensegs - 1) * ((BlockNumber) RELSEG_SIZE);
		seg = umfile_v_get(ctx, forknum, curopensegs - 1);

		if (priorblocks >= nblocks)
		{
			if (FileTruncate(seg->umfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
							FilePathName(seg->umfd_vfd))));

			if (seg != umfile_v_get(ctx, forknum, 0))
			{
				FileClose(seg->umfd_vfd);
				umfile_fdvec_resize(ctx, forknum, curopensegs - 1);
			}
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			BlockNumber	lastsegblocks;

			lastsegblocks = nblocks - priorblocks;
			if (FileTruncate(seg->umfd_vfd,
							 (off_t) lastsegblocks * BLCKSZ,
							 WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
							FilePathName(seg->umfd_vfd),
							nblocks)));
		}
		else
			break;

		curopensegs--;
	}
}

void
umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	Assert(ctx != NULL);

	(void) umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);
	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	while (umfile_openseg(ctx, ctx->rlocator, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *seg = umfile_v_get(ctx, forknum, segno - 1);

		if (FileSync(seg->umfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(seg->umfd_vfd))));

		if (segno > min_inactive_seg)
		{
			FileClose(seg->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, segno - 1);
		}

		segno--;
	}
}

void
umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum)
{
	/*
	 * Registering durability at this boundary is implemented as an immediate
	 * fsync.
	 */
	umfile_immedsync(ctx, forknum);
}

void
umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == InvalidForkNumber)
	{
		for (forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
			umfile_unlink(rlocator, forknum, isRedo);
		return;
	}

	for (BlockNumber segno = 0;; segno++)
	{
		RelPathStr	path;

		path = umfile_segpath(rlocator, forknum, segno);
		if (unlink(path.str) < 0)
		{
			if (FILE_POSSIBLY_DELETED(errno))
			{
				if (segno == 0 && isRedo)
					return;
				break;
			}

			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path.str)));
			break;
		}
	}
}

static void
umfile_ctx_registry_init(void)
{
	if (UmFileContextHash == NULL)
		umfile_init();

	Assert(UmFileContextHash != NULL);
}

static UmbraFileContext *
umfile_ctx_create(RelFileLocatorBackend rlocator)
{
	UmbraFileContext *ctx;

	Assert(UmFileCxt != NULL);

	ctx = MemoryContextAllocZero(UmFileCxt, sizeof(UmbraFileContext));
	ctx->rlocator = rlocator;

	for (ForkNumber forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
	{
		ctx->num_open_segs[forknum] = 0;
		ctx->seg_fds[forknum] = NULL;
	}

	return ctx;
}

static void
umfile_ctx_destroy(UmbraFileContext *ctx)
{
	if (ctx == NULL)
		return;

	for (ForkNumber forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
		umfile_close_open_segments(ctx, forknum);

	pfree(ctx);
}

static void
umfile_close_open_segments(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			nopensegs;

	Assert(ctx != NULL);

	nopensegs = ctx->num_open_segs[forknum];
	while (nopensegs > 0)
	{
		UmfdVec    *seg = umfile_v_get(ctx, forknum, nopensegs - 1);

		if (umfile_seg_entry_is_open(seg))
			FileClose(seg->umfd_vfd);
		umfile_fdvec_resize(ctx, forknum, nopensegs - 1);
		nopensegs--;
	}
}

static bool
umfile_create(UmbraFileContext *ctx, ForkNumber forknum, bool isRedo)
{
	RelPathStr	path;
	File		fd;
	UmfdVec    *seg;
	bool		created = false;

	Assert(ctx != NULL);

	if (isRedo && ctx->num_open_segs[forknum] > 0)
		return false;

	if (ctx->num_open_segs[forknum] > 0)
		umfile_close_open_segments(ctx, forknum);

	TablespaceCreateDbspace(ctx->rlocator.locator.spcOid,
							ctx->rlocator.locator.dbOid,
							isRedo);

	path = umfile_segpath(ctx->rlocator, forknum, 0);
	fd = PathNameOpenFile(path.str, umfile_open_flags() | O_CREAT | O_EXCL);
	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path.str, umfile_open_flags());
		if (fd < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path.str)));
		}
	}
	else
		created = true;

	umfile_fdvec_resize(ctx, forknum, 1);
	seg = umfile_v_get(ctx, forknum, 0);
	seg->umfd_vfd = fd;
	seg->umfd_segno = 0;

	return created;
}

static int
umfile_open_flags(void)
{
	int			flags = O_RDWR | PG_BINARY;

	if (io_direct_flags & IO_DIRECT_DATA)
		flags |= PG_O_DIRECT;

	return flags;
}

static void
umfile_fdvec_resize(UmbraFileContext *ctx, ForkNumber forknum, int nseg)
{
	Assert(ctx != NULL);
	Assert(nseg >= 0);

	if (nseg == 0)
	{
		if (ctx->num_open_segs[forknum] > 0)
			pfree(ctx->seg_fds[forknum]);
		ctx->seg_fds[forknum] = NULL;
		ctx->num_open_segs[forknum] = 0;
		return;
	}

	if (ctx->num_open_segs[forknum] == 0)
	{
		ctx->seg_fds[forknum] =
			MemoryContextAlloc(UmFileCxt, sizeof(UmfdVec) * nseg);
	}
	else if (nseg > ctx->num_open_segs[forknum])
	{
		ctx->seg_fds[forknum] =
			repalloc(ctx->seg_fds[forknum], sizeof(UmfdVec) * nseg);
	}

	ctx->num_open_segs[forknum] = nseg;
}

static inline UmfdVec *
umfile_v_get(UmbraFileContext *ctx, ForkNumber forknum, int segindex)
{
	Assert(ctx != NULL);
	Assert(segindex >= 0);
	Assert(segindex < ctx->num_open_segs[forknum]);
	return &ctx->seg_fds[forknum][segindex];
}

static BlockNumber
umfile_nblocks_in_seg(File vfd)
{
	pgoff_t		size;

	size = FileSize(vfd);
	if (size < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not determine size of file \"%s\": %m",
						FilePathName(vfd))));
	if ((size % BLCKSZ) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("file \"%s\" has partial block contents",
						FilePathName(vfd)),
				 errdetail("File size %lld is not a multiple of %d bytes.",
						   (long long) size, BLCKSZ)));

	return (BlockNumber) (size / BLCKSZ);
}

static RelPathStr
umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum,
			   BlockNumber segno)
{
	RelPathStr	base;
	RelPathStr	fullpath;

	if (forknum == UMBRA_METADATA_FORKNUM)
		base = UmMetadataRelPathBackend(rlocator);
	else
		base = relpath(rlocator, forknum);

	if (segno == 0)
		return base;

	snprintf(fullpath.str, sizeof(fullpath.str), "%s.%u", base.str, segno);
	return fullpath;
}

static UmfdVec *
umfile_openseg(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			   ForkNumber forknum, BlockNumber segno, int oflags)
{
	UmfdVec    *seg;
	RelPathStr	path;
	File		fd;
	int			old_nseg;

	Assert(ctx != NULL);

	old_nseg = ctx->num_open_segs[forknum];
	if (segno < (BlockNumber) old_nseg)
	{
		seg = umfile_v_get(ctx, forknum, (int) segno);
		if (umfile_seg_entry_is_open(seg))
			return seg;
	}

	path = umfile_segpath(rlocator, forknum, segno);
	fd = PathNameOpenFile(path.str, umfile_open_flags() | oflags);
	if (fd < 0)
		return NULL;

	if (segno >= (BlockNumber) old_nseg)
	{
		umfile_fdvec_resize(ctx, forknum, segno + 1);
		for (int i = old_nseg; i < ctx->num_open_segs[forknum]; i++)
			umfile_seg_entry_reset(umfile_v_get(ctx, forknum, i));
	}

	seg = umfile_v_get(ctx, forknum, (int) segno);
	seg->umfd_vfd = fd;
	seg->umfd_segno = segno;

	Assert(umfile_nblocks_in_seg(seg->umfd_vfd) <= (BlockNumber) RELSEG_SIZE);
	return seg;
}

static UmfdVec *
umfile_openfork(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				ForkNumber forknum, int behavior)
{
	RelPathStr	path;
	File		fd;
	UmfdVec    *seg;

	Assert(ctx != NULL);

	if (ctx->num_open_segs[forknum] > 0)
	{
		seg = umfile_v_get(ctx, forknum, 0);
		if (umfile_seg_entry_is_open(seg))
			return seg;
	}

	path = umfile_segpath(rlocator, forknum, 0);
	fd = PathNameOpenFile(path.str, umfile_open_flags());
	if (fd < 0)
	{
		if ((behavior & UM_EXTENSION_RETURN_NULL) &&
			FILE_POSSIBLY_DELETED(errno))
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path.str)));
	}

	if (ctx->num_open_segs[forknum] == 0)
		umfile_fdvec_resize(ctx, forknum, 1);
	seg = umfile_v_get(ctx, forknum, 0);
	seg->umfd_vfd = fd;
	seg->umfd_segno = 0;

	Assert(umfile_nblocks_in_seg(seg->umfd_vfd) <= (BlockNumber) RELSEG_SIZE);
	return seg;
}

static UmfdVec *
umfile_getseg(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			  ForkNumber forknum, BlockNumber blkno,
			  bool skipFsync, int behavior)
{
	UmfdVec    *seg;
	BlockNumber	targetseg;
	BlockNumber	nextsegno;

	Assert(ctx != NULL);
	Assert(behavior &
		   (UM_EXTENSION_FAIL | UM_EXTENSION_CREATE |
			UM_EXTENSION_RETURN_NULL | UM_EXTENSION_DONT_OPEN));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	if (targetseg < (BlockNumber) ctx->num_open_segs[forknum])
	{
		seg = umfile_v_get(ctx, forknum, (int) targetseg);
		if (umfile_seg_entry_is_open(seg))
			return seg;
	}

	if (behavior & UM_EXTENSION_DONT_OPEN)
		return NULL;

	if (ctx->num_open_segs[forknum] > 0)
		seg = umfile_v_get(ctx, forknum, ctx->num_open_segs[forknum] - 1);
	else
	{
		seg = umfile_openfork(ctx, rlocator, forknum, behavior);
		if (seg == NULL)
			return NULL;
	}

	for (nextsegno = ctx->num_open_segs[forknum];
		 nextsegno <= targetseg;
		 nextsegno++)
	{
		BlockNumber	nblocks;
		int			flags = 0;

		Assert(nextsegno == seg->umfd_segno + 1);

		nblocks = umfile_nblocks_in_seg(seg->umfd_vfd);
		if (nblocks > (BlockNumber) RELSEG_SIZE)
			elog(FATAL, "Umbra segment too big");

		if ((behavior & UM_EXTENSION_CREATE) ||
			(InRecovery && (behavior & UM_EXTENSION_CREATE_RECOVERY)))
		{
			if (nblocks < (BlockNumber) RELSEG_SIZE)
			{
				char	   *zerobuf;

				zerobuf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE,
										 MCXT_ALLOC_ZERO);
				umfile_extend(ctx, forknum,
							  nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
							  zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (nblocks < (BlockNumber) RELSEG_SIZE)
		{
			if (behavior & UM_EXTENSION_RETURN_NULL)
			{
				errno = ENOENT;
				return NULL;
			}

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							umfile_segpath(rlocator, forknum, nextsegno).str,
							blkno, nblocks)));
		}

		seg = umfile_openseg(ctx, rlocator, forknum, nextsegno, flags);
		if (seg == NULL)
		{
			if ((behavior & UM_EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							umfile_segpath(rlocator, forknum, nextsegno).str,
							blkno)));
		}
	}

	return seg;
}

static bool
umfile_fork_has_open_segment(UmbraFileContext *ctx, ForkNumber forknum)
{
	Assert(ctx != NULL);

	for (int i = 0; i < ctx->num_open_segs[forknum]; i++)
	{
		if (umfile_seg_entry_is_open(umfile_v_get(ctx, forknum, i)))
			return true;
	}

	return false;
}

static bool
umfile_fork_has_open_segment_on_disk(UmbraFileContext *ctx,
									 RelFileLocatorBackend rlocator,
									 ForkNumber forknum)
{
	bool		have_live = false;

	Assert(ctx != NULL);

	for (int i = 0; i < ctx->num_open_segs[forknum]; i++)
	{
		UmfdVec    *seg = umfile_v_get(ctx, forknum, i);
		RelPathStr	path;

		if (!umfile_seg_entry_is_open(seg))
			continue;

		path = umfile_segpath(rlocator, forknum, seg->umfd_segno);
		if (access(path.str, F_OK) == 0)
		{
			have_live = true;
			continue;
		}

		FileClose(seg->umfd_vfd);
		umfile_seg_entry_reset(seg);
	}

	return have_live;
}

static inline bool
umfile_seg_entry_is_open(const UmfdVec *seg)
{
	return (seg != NULL && seg->umfd_vfd >= 0);
}

static inline void
umfile_seg_entry_reset(UmfdVec *seg)
{
	seg->umfd_vfd = -1;
	seg->umfd_segno = InvalidBlockNumber;
}
