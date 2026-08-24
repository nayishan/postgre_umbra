/*-------------------------------------------------------------------------
 *
 * umfile.c
 *	  This code manages Umbra relation segment files.
 *
 * This layer is an Umbra-owned copy of md.c's md-style segment file
 * management.  It uses the ordinary relation fork paths in this patch, but
 * stores open segment state in UmbraFileContext rather than in md.c's
 * SMgrRelation fields.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/umfile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "common/file_utils.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/aio.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/umfile.h"
#include "storage/relfilelocator.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Relation files use md-compatible RELSEG_SIZE segments: zero or more full
 * segments, one partial segment, and optional zero-length inactive segments.
 * Truncate keeps inactive segments so other backends' open descriptors remain
 * valid if the relation grows again.
 */
StaticAssertDecl(RELSEG_SIZE > 0 && RELSEG_SIZE <= INT_MAX,
				 "RELSEG_SIZE must fit in an integer");

/*
 * num_open_segs describes only cached active segments; another backend may
 * have extended the fork.  The arrays live in UmFileCxt.
 */

typedef struct UmfdVec
{
	File		umfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber umfd_segno;		/* segment number, from 0 */
} UmfdVec;

struct UmbraFileContext
{
	RelFileLocatorBackend rlocator;
	int			num_open_segs[MAX_FORKNUM + 1];
	UmfdVec    *seg_fds[MAX_FORKNUM + 1];
};

static MemoryContext UmFileCxt; /* context for all UmfdVec objects */
static HTAB *UmFileContextHash; /* registry keyed by relation locator */


/* Populate a file tag describing an umfile segment file. */
#define INIT_UMFILE_FILETAG(a,xx_rlocator,xx_forknum,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_UMFILE, \
	(a).rlocator = (xx_rlocator), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)


/*** behavior for umfile_openfork & umfile_getseg ***/
/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/* don't try to open a segment, if not already open */
#define EXTENSION_DONT_OPEN			(1 << 5)


/* Leave room for a dot and the maximum segment number. */
#define SEGMENT_CHARS	OIDCHARS
#define UMFILE_PATH_STR_MAXLEN \
	(\
		REL_PATH_STR_MAXLEN \
		+ sizeof((char)'.') \
		+ SEGMENT_CHARS \
	)
typedef struct UmFilePathStr
{
	char		str[UMFILE_PATH_STR_MAXLEN + 1];
} UmFilePathStr;


/* local routines */
static void umfile_unlinkfork(RelFileLocatorBackend rlocator, ForkNumber forknum,
							  bool isRedo);
static UmfdVec *umfile_openfork(UmbraFileContext *ctx, ForkNumber forknum, int behavior);
static void umfile_register_dirty_segment(UmbraFileContext *ctx, ForkNumber forknum,
										  UmfdVec *seg);
static void umfile_register_unlink_segment(RelFileLocatorBackend rlocator, ForkNumber forknum,
										   BlockNumber segno);
static void umfile_register_forget_request(RelFileLocatorBackend rlocator, ForkNumber forknum,
										   BlockNumber segno);
static void umfile_fdvec_resize(UmbraFileContext *ctx,
								ForkNumber forknum,
								int nseg);
static UmFilePathStr umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum,
									BlockNumber segno);
static UmFilePathStr umfile_segpath_perm(RelFileLocator rlocator,
										 ForkNumber forknum, BlockNumber segno);
static UmfdVec *umfile_openseg(UmbraFileContext *ctx, ForkNumber forknum,
							   BlockNumber segno, int oflags);
static UmfdVec *umfile_getseg(UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber umfile_nblocks_in_seg(UmfdVec *seg);
static inline int umfile_open_flags(void);

static inline int
umfile_open_flags(void)
{
	int			flags = O_RDWR | PG_BINARY;

	if (io_direct_flags & IO_DIRECT_DATA)
		flags |= PG_O_DIRECT;

	return flags;
}

/* Initialize backend-local Umbra file state. */
void
umfile_init(void)
{
	HASHCTL		ctl;

	if (UmFileContextHash != NULL)
		return;

	if (UmFileCxt == NULL)
		UmFileCxt = AllocSetContextCreate(TopMemoryContext,
										  "UmbraFile",
										  ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelFileLocatorBackend);
	ctl.entrysize = sizeof(UmbraFileContext);
	ctl.hcxt = UmFileCxt;

	UmFileContextHash = hash_create("Umbra file contexts", 256, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Test whether a fork's physical file exists. */
bool
umfile_exists(UmbraFileContext *ctx, ForkNumber forknum)
{
	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	/*
	 * Reopen outside recovery so an unlinked cached descriptor is not
	 * trusted.
	 */
	if (!InRecovery)
		umfile_close(ctx, forknum);

	return (umfile_openfork(ctx, forknum, EXTENSION_RETURN_NULL) != NULL);
}

/* Create a fork; redo may reuse an existing file. */
void
umfile_create(UmbraFileContext *ctx, ForkNumber forknum, bool isRedo)
{
	UmfdVec    *v;
	RelPathStr	path;
	File		fd;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	if (isRedo && ctx->num_open_segs[forknum] > 0)
		return;					/* created and opened already... */

	Assert(ctx->num_open_segs[forknum] == 0);

	/* Ensure the database directory exists in this tablespace. */
	TablespaceCreateDbspace(ctx->rlocator.locator.spcOid,
							ctx->rlocator.locator.dbOid,
							isRedo);

	path = relpath(ctx->rlocator, forknum);

	fd = PathNameOpenFile(path.str, umfile_open_flags() | O_CREAT | O_EXCL);

	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path.str, umfile_open_flags());
		if (fd < 0)
		{
			/* be sure to report the error reported by create, not open */
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path.str)));
		}
	}

	umfile_fdvec_resize(ctx, forknum, 1);
	v = &ctx->seg_fds[forknum][0];
	v->umfd_vfd = fd;
	v->umfd_segno = 0;

	if (!RelFileLocatorBackendIsTemp(ctx->rlocator))
		umfile_register_dirty_segment(ctx, forknum, v);
}

/*
 * Unlink one fork or all forks.  A regular MAIN segment zero is truncated and
 * removed after the next checkpoint, preventing unsafe relfilenumber reuse
 * after a skip-WAL write.  Redo, binary upgrade, temporary relations, and
 * other forks remove it immediately.  Additional segments are truncated to
 * release space held by other backends' descriptors, then unlinked.
 * Failures are warnings because unlink commonly runs after transaction end.
 */
void
umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == InvalidForkNumber)
	{
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			umfile_unlinkfork(rlocator, forknum, isRedo);
	}
	else
		umfile_unlinkfork(rlocator, forknum, isRedo);
}

/* Truncate a file to release disk space. */
static int
do_truncate(const char *path)
{
	int			save_errno;
	int			ret;

	ret = pg_truncate(path, 0);

	/* Log a warning here to avoid repetition in callers. */
	if (ret < 0 && errno != ENOENT)
	{
		save_errno = errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not truncate file \"%s\": %m", path)));
		errno = save_errno;
	}

	return ret;
}

static void
umfile_unlinkfork(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	RelPathStr	path;
	int			ret;
	int			save_errno;

	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	path = relpath(rlocator, forknum);

	/* MAIN segment zero may be delayed as described by umfile_unlink(). */
	if (isRedo || IsBinaryUpgrade || forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(rlocator))
	{
		if (!RelFileLocatorBackendIsTemp(rlocator))
		{
			/* Prevent other backends' fds from holding on to the disk space */
			ret = do_truncate(path.str);

			/* Forget any pending sync requests for the first segment */
			save_errno = errno;
			umfile_register_forget_request(rlocator, forknum, 0 /* first seg */ );
			errno = save_errno;
		}
		else
			ret = 0;

		/* Next unlink the file, unless it was already found to be missing */
		if (ret >= 0 || errno != ENOENT)
		{
			ret = unlink(path.str);
			if (ret < 0 && errno != ENOENT)
			{
				save_errno = errno;
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", path.str)));
				errno = save_errno;
			}
		}
	}
	else
	{
		/* Prevent other backends' fds from holding on to the disk space */
		ret = do_truncate(path.str);

		/* Register request to unlink first segment later */
		save_errno = errno;
		umfile_register_unlink_segment(rlocator, forknum, 0 /* first seg */ );
		errno = save_errno;
	}

	/* Delete active and inactive additional segments through the first gap. */
	if (ret >= 0 || errno != ENOENT)
	{
		UmFilePathStr segpath;
		BlockNumber segno;

		for (segno = 1;; segno++)
		{
			segpath = umfile_segpath(rlocator, forknum, segno);

			if (!RelFileLocatorBackendIsTemp(rlocator))
			{
				/* Release space still referenced by another backend. */
				if (do_truncate(segpath.str) < 0 && errno == ENOENT)
					break;

				/* Forget pending sync before unlink. */
				umfile_register_forget_request(rlocator, forknum, segno);
			}

			if (unlink(segpath.str) < 0)
			{
				/* ENOENT is expected after the last segment... */
				if (errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath.str)));
				break;
			}
		}
	}
}

/* Extend a fork by writing one block at or beyond EOF. */
void
umfile_extend(UmbraFileContext *ctx, ForkNumber forknum,
			  BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	pgoff_t		seekpos;
	int			nbytes;
	UmfdVec    *v;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	/* If this build supports direct I/O, the buffer must be I/O aligned. */
	if (PG_O_DIRECT != 0 && PG_IO_ALIGN_SIZE <= BLCKSZ)
		Assert((uintptr_t) buffer == TYPEALIGN(PG_IO_ALIGN_SIZE, buffer));

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= umfile_nblocks(ctx, forknum));
#endif

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.  (Note that this failure should be unreachable
	 * because of upstream checks in bufmgr.c.)
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(ctx->rlocator, forknum).str,
						InvalidBlockNumber)));

	v = umfile_getseg(ctx, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

	if ((nbytes = FileWrite(v->umfd_vfd, buffer, BLCKSZ, seekpos,
							WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->umfd_vfd)),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->umfd_vfd),
						nbytes, BLCKSZ, blocknum),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !RelFileLocatorBackendIsTemp(ctx->rlocator))
		umfile_register_dirty_segment(ctx, forknum, v);

	Assert(umfile_nblocks_in_seg(v) <= ((BlockNumber) RELSEG_SIZE));
}

/*
 * umfile_zeroextend() -- Add new zeroed out blocks to the specified relation.
 *
 * Similar to umfile_extend(), except the relation can be extended by multiple
 * blocks at once and the added blocks will be filled with zeroes.
 */
void
umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum, int nblocks, bool skipFsync)
{
	UmfdVec    *v;
	BlockNumber curblocknum = blocknum;
	int			remblocks = nblocks;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	Assert(nblocks > 0);

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= umfile_nblocks(ctx, forknum));
#endif

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber or larger.
	 */
	if ((uint64) blocknum + nblocks >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(ctx->rlocator, forknum).str,
						InvalidBlockNumber)));

	while (remblocks > 0)
	{
		BlockNumber segstartblock = curblocknum % ((BlockNumber) RELSEG_SIZE);
		pgoff_t		seekpos = (pgoff_t) BLCKSZ * segstartblock;
		int			numblocks;

		if (segstartblock + remblocks > RELSEG_SIZE)
			numblocks = RELSEG_SIZE - segstartblock;
		else
			numblocks = remblocks;

		v = umfile_getseg(ctx, forknum, curblocknum, skipFsync, EXTENSION_CREATE);

		Assert(segstartblock < RELSEG_SIZE);
		Assert(segstartblock + numblocks <= RELSEG_SIZE);

		/*
		 * If available and useful, use posix_fallocate() (via
		 * FileFallocate()) to extend the relation. That's often more
		 * efficient than using write(), as it commonly won't cause the kernel
		 * to allocate page cache space for the extended pages.
		 *
		 * However, we don't use FileFallocate() for small extensions, as it
		 * defeats delayed allocation on some filesystems. Not clear where
		 * that decision should be made though? For now just use a cutoff of
		 * 8, anything between 4 and 8 worked OK in some local testing.
		 */
		if (numblocks > 8 &&
			file_extend_method != FILE_EXTEND_METHOD_WRITE_ZEROS)
		{
			int			ret = 0;

#ifdef HAVE_POSIX_FALLOCATE
			if (file_extend_method == FILE_EXTEND_METHOD_POSIX_FALLOCATE)
			{
				ret = FileFallocate(v->umfd_vfd,
									seekpos, (pgoff_t) BLCKSZ * numblocks,
									WAIT_EVENT_DATA_FILE_EXTEND);
			}
			else
#endif
			{
				elog(ERROR, "unsupported file_extend_method: %d",
					 file_extend_method);
			}
			if (ret != 0)
			{
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not extend file \"%s\" with FileFallocate(): %m",
							   FilePathName(v->umfd_vfd)),
						errhint("Check free disk space."));
			}
		}
		else
		{
			int			ret;

			/* FileZero can extend multiple blocks in one write. */
			ret = FileZero(v->umfd_vfd,
						   seekpos, (pgoff_t) BLCKSZ * numblocks,
						   WAIT_EVENT_DATA_FILE_EXTEND);
			if (ret < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not extend file \"%s\": %m",
							   FilePathName(v->umfd_vfd)),
						errhint("Check free disk space."));
		}

		if (!skipFsync && !RelFileLocatorBackendIsTemp(ctx->rlocator))
			umfile_register_dirty_segment(ctx, forknum, v);

		Assert(umfile_nblocks_in_seg(v) <= ((BlockNumber) RELSEG_SIZE));

		remblocks -= numblocks;
		curblocknum += numblocks;
	}
}

/* Open segment zero, returning NULL only when behavior permits it. */
static UmfdVec *
umfile_openfork(UmbraFileContext *ctx, ForkNumber forknum, int behavior)
{
	UmfdVec    *v;
	RelPathStr	path;
	File		fd;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	/* No work if already open */
	if (ctx->num_open_segs[forknum] > 0)
		return &ctx->seg_fds[forknum][0];

	path = relpath(ctx->rlocator, forknum);

	fd = PathNameOpenFile(path.str, umfile_open_flags());

	if (fd < 0)
	{
		if ((behavior & EXTENSION_RETURN_NULL) &&
			FILE_POSSIBLY_DELETED(errno))
			return NULL;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path.str)));
	}

	umfile_fdvec_resize(ctx, forknum, 1);
	v = &ctx->seg_fds[forknum][0];
	v->umfd_vfd = fd;
	v->umfd_segno = 0;

	Assert(umfile_nblocks_in_seg(v) <= ((BlockNumber) RELSEG_SIZE));

	return v;
}

/* Acquire the registered context for a relation. */
UmbraFileContext *
umfile_open(RelFileLocatorBackend rlocator)
{
	UmbraFileContext *ctx;
	bool		found;

	if (UmFileContextHash == NULL)
		umfile_init();

	ctx = hash_search(UmFileContextHash, &rlocator, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "duplicate Umbra file context");

	ctx->rlocator = rlocator;
	memset(ctx->num_open_segs, 0, sizeof(ctx->num_open_segs));
	memset(ctx->seg_fds, 0, sizeof(ctx->seg_fds));

	return ctx;
}

/* Close one fork's cached segments. */
void
umfile_close(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			nopensegs;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	nopensegs = ctx->num_open_segs[forknum];

	if (nopensegs == 0)
		return;

	while (nopensegs > 0)
	{
		UmfdVec    *v = &ctx->seg_fds[forknum][nopensegs - 1];

		FileClose(v->umfd_vfd);
		umfile_fdvec_resize(ctx, forknum, nopensegs - 1);
		nopensegs--;
	}
}

/* Release a registered Umbra file context. */
void
umfile_destroy(UmbraFileContext *ctx)
{
	ForkNumber	forknum;
	RelFileLocatorBackend rlocator;

	if (ctx == NULL)
		return;

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		umfile_close(ctx, forknum);

	rlocator = ctx->rlocator;
	if (hash_search(UmFileContextHash, &rlocator, HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "Umbra file context registry corrupted");
}

/* Initiate asynchronous read of relation blocks. */
bool
umfile_prefetch(UmbraFileContext *ctx, ForkNumber forknum,
				BlockNumber blocknum, int nblocks)
{
#ifdef USE_PREFETCH

	Assert((io_direct_flags & IO_DIRECT_DATA) == 0);

	if ((uint64) blocknum + nblocks > (uint64) MaxBlockNumber + 1)
		return false;

	while (nblocks > 0)
	{
		pgoff_t		seekpos;
		UmfdVec    *v;
		int			nblocks_this_segment;

		v = umfile_getseg(ctx, forknum, blocknum, false,
						  InRecovery ? EXTENSION_RETURN_NULL : EXTENSION_FAIL);
		if (v == NULL)
			return false;

		seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks,
				RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));

		(void) FilePrefetch(v->umfd_vfd, seekpos, BLCKSZ * nblocks_this_segment,
							WAIT_EVENT_DATA_FILE_PREFETCH);

		blocknum += nblocks_this_segment;
		nblocks -= nblocks_this_segment;
	}
#endif							/* USE_PREFETCH */

	return true;
}

/* Convert buffers to iovecs, merging adjacent buffers. */
static int
buffers_to_iovec(struct iovec *iov, void **buffers, int nblocks)
{
	struct iovec *iovp;
	int			iovcnt;

	Assert(nblocks >= 1);

	/* If this build supports direct I/O, buffers must be I/O aligned. */
	for (int i = 0; i < nblocks; ++i)
	{
		if (PG_O_DIRECT != 0 && PG_IO_ALIGN_SIZE <= BLCKSZ)
			Assert((uintptr_t) buffers[i] ==
				   TYPEALIGN(PG_IO_ALIGN_SIZE, buffers[i]));
	}

	iovp = &iov[0];
	iovp->iov_base = buffers[0];
	iovp->iov_len = BLCKSZ;
	iovcnt = 1;

	for (int i = 1; i < nblocks; ++i)
	{
		void	   *buffer = buffers[i];

		if (((char *) iovp->iov_base + iovp->iov_len) == buffer)
		{
			iovp->iov_len += BLCKSZ;
		}
		else
		{
			iovp++;
			iovp->iov_base = buffer;
			iovp->iov_len = BLCKSZ;
			iovcnt++;
		}
	}

	return iovcnt;
}

/* Return the blocks remaining in this segment. */
uint32
umfile_maxcombine(UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum)
{
	BlockNumber segoff;

	(void) ctx;
	(void) forknum;

	segoff = blocknum % ((BlockNumber) RELSEG_SIZE);

	return RELSEG_SIZE - segoff;
}

/* Read relation blocks. */
void
umfile_readv(UmbraFileContext *ctx, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		int			iovcnt;
		pgoff_t		seekpos;
		int			nbytes;
		UmfdVec    *v;
		BlockNumber nblocks_this_segment;
		size_t		transferred_this_segment;
		size_t		size_this_segment;

		v = umfile_getseg(ctx, forknum, blocknum, false,
						  EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

		seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks,
				RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
		nblocks_this_segment = Min(nblocks_this_segment, lengthof(iov));

		if (nblocks_this_segment != nblocks)
			elog(ERROR, "read crosses segment boundary");

		iovcnt = buffers_to_iovec(iov, buffers, nblocks_this_segment);
		size_this_segment = nblocks_this_segment * BLCKSZ;
		transferred_this_segment = 0;

		/* Retry short reads until the request completes or reaches EOF. */
		for (;;)
		{
			TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
												ctx->rlocator.locator.spcOid,
												ctx->rlocator.locator.dbOid,
												ctx->rlocator.locator.relNumber,
												ctx->rlocator.backend);
			nbytes = FileReadV(v->umfd_vfd, iov, iovcnt, seekpos,
							   WAIT_EVENT_DATA_FILE_READ);
			TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
											   ctx->rlocator.locator.spcOid,
											   ctx->rlocator.locator.dbOid,
											   ctx->rlocator.locator.relNumber,
											   ctx->rlocator.backend,
											   nbytes,
											   size_this_segment - transferred_this_segment);

#ifdef SIMULATE_SHORT_READ
			nbytes = Min(nbytes, 4096);
#endif

			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read blocks %u..%u in file \"%s\": %m",
								blocknum,
								blocknum + nblocks_this_segment - 1,
								FilePathName(v->umfd_vfd))));

			if (nbytes == 0)
			{
				/*
				 * Preserve md's synchronous recovery/zero_damaged_pages
				 * fallback. The assertion records that blocks beyond physical
				 * EOF are not a sound persistent state and are unsupported by
				 * asynchronous reads.
				 */
				if (zero_damaged_pages || InRecovery)
				{
					Assert(false);	/* see comment above */

					for (BlockNumber i = transferred_this_segment / BLCKSZ;
						 i < nblocks_this_segment;
						 ++i)
						memset(buffers[i], 0, BLCKSZ);
					break;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("could not read blocks %u..%u in file \"%s\": read only %zu of %zu bytes",
									blocknum,
									blocknum + nblocks_this_segment - 1,
									FilePathName(v->umfd_vfd),
									transferred_this_segment,
									size_this_segment)));
			}

			transferred_this_segment += nbytes;
			Assert(transferred_this_segment <= size_this_segment);
			if (transferred_this_segment == size_this_segment)
				break;

			seekpos += nbytes;
			iovcnt = compute_remaining_iovec(iov, iov, iovcnt, nbytes);
		}

		nblocks -= nblocks_this_segment;
		buffers += nblocks_this_segment;
		blocknum += nblocks_this_segment;
	}
}

/* Start an asynchronous relation read. */
void
umfile_startreadv(PgAioHandle *ioh,
				  UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	pgoff_t		seekpos;
	UmfdVec    *v;
	BlockNumber nblocks_this_segment;
	struct iovec *iov;
	int			iovcnt;
	int			ret;

	v = umfile_getseg(ctx, forknum, blocknum, false,
					  EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

	nblocks_this_segment =
		Min(nblocks,
			RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));

	if (nblocks_this_segment != nblocks)
		elog(ERROR, "read crossing segment boundary");

	iovcnt = pgaio_io_get_iovec(ioh, &iov);

	Assert(nblocks <= iovcnt);

	iovcnt = buffers_to_iovec(iov, buffers, nblocks_this_segment);

	Assert(iovcnt <= nblocks_this_segment);

	if (!(io_direct_flags & IO_DIRECT_DATA))
		pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);

	ret = FileStartReadV(ioh, v->umfd_vfd, iovcnt, seekpos, WAIT_EVENT_DATA_FILE_READ);
	if (ret != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not start reading blocks %u..%u in file \"%s\": %m",
						blocknum,
						blocknum + nblocks_this_segment - 1,
						FilePathName(v->umfd_vfd))));

	/* The shared md callback classifies hard, zero-length, and partial reads. */
}

/* Write existing relation blocks; extension uses umfile_extend(). */
void
umfile_writev(UmbraFileContext *ctx, ForkNumber forknum,
			  BlockNumber blocknum, const void **buffers,
			  BlockNumber nblocks, bool skipFsync)
{
	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert((uint64) blocknum + (uint64) nblocks <=
		   (uint64) umfile_nblocks(ctx, forknum));
#endif

	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		int			iovcnt;
		pgoff_t		seekpos;
		int			nbytes;
		UmfdVec    *v;
		BlockNumber nblocks_this_segment;
		size_t		transferred_this_segment;
		size_t		size_this_segment;

		v = umfile_getseg(ctx, forknum, blocknum, skipFsync,
						  EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

		seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks,
				RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
		nblocks_this_segment = Min(nblocks_this_segment, lengthof(iov));

		if (nblocks_this_segment != nblocks)
			elog(ERROR, "write crosses segment boundary");

		iovcnt = buffers_to_iovec(iov, (void **) buffers, nblocks_this_segment);
		size_this_segment = nblocks_this_segment * BLCKSZ;
		transferred_this_segment = 0;

		/* Retry short writes; the kernel should eventually report ENOSPC. */
		for (;;)
		{
			TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
												 ctx->rlocator.locator.spcOid,
												 ctx->rlocator.locator.dbOid,
												 ctx->rlocator.locator.relNumber,
												 ctx->rlocator.backend);
			nbytes = FileWriteV(v->umfd_vfd, iov, iovcnt, seekpos,
								WAIT_EVENT_DATA_FILE_WRITE);
			TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
												ctx->rlocator.locator.spcOid,
												ctx->rlocator.locator.dbOid,
												ctx->rlocator.locator.relNumber,
												ctx->rlocator.backend,
												nbytes,
												size_this_segment - transferred_this_segment);

#ifdef SIMULATE_SHORT_WRITE
			nbytes = Min(nbytes, 4096);
#endif

			if (nbytes < 0)
			{
				bool		enospc = errno == ENOSPC;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write blocks %u..%u in file \"%s\": %m",
								blocknum,
								blocknum + nblocks_this_segment - 1,
								FilePathName(v->umfd_vfd)),
						 enospc ? errhint("Check free disk space.") : 0));
			}

			/* One loop should usually be enough. */
			transferred_this_segment += nbytes;
			Assert(transferred_this_segment <= size_this_segment);
			if (transferred_this_segment == size_this_segment)
				break;

			/* Adjust position and iovecs after a short write. */
			seekpos += nbytes;
			iovcnt = compute_remaining_iovec(iov, iov, iovcnt, nbytes);
		}

		if (!skipFsync && !RelFileLocatorBackendIsTemp(ctx->rlocator))
			umfile_register_dirty_segment(ctx, forknum, v);

		nblocks -= nblocks_this_segment;
		buffers += nblocks_this_segment;
		blocknum += nblocks_this_segment;
	}
}

/*
 * umfile_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
umfile_writeback(UmbraFileContext *ctx, ForkNumber forknum,
				 BlockNumber blocknum, BlockNumber nblocks)
{
	Assert((io_direct_flags & IO_DIRECT_DATA) == 0);

	/* Coalesce flushes within each segment. */
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		pgoff_t		seekpos;
		UmfdVec    *v;
		int			segnum_start,
					segnum_end;

		v = umfile_getseg(ctx, forknum, blocknum, true, EXTENSION_DONT_OPEN);

		/* Do not reopen after an smgr-release barrier for an unlinking file. */
		if (!v)
			return;

		/* compute offset inside the current segment */
		segnum_start = blocknum / RELSEG_SIZE;

		/* compute number of desired writes within the current segment */
		segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
		if (segnum_start != segnum_end)
			nflush = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(nflush >= 1);
		Assert(nflush <= nblocks);

		seekpos = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		FileWriteback(v->umfd_vfd, seekpos, (pgoff_t) BLCKSZ * nflush, WAIT_EVENT_DATA_FILE_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}

/* Return fork size and open every active segment. */
BlockNumber
umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum)
{
	UmfdVec    *v;
	BlockNumber nblocks;
	BlockNumber segno;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	umfile_openfork(ctx, forknum, EXTENSION_FAIL);

	/* umfile_openfork has opened the first segment */
	Assert(ctx->num_open_segs[forknum] > 0);

	/*
	 * Start at the last verified full segment; relcache flush handles
	 * truncate.
	 */
	segno = ctx->num_open_segs[forknum] - 1;
	v = &ctx->seg_fds[forknum][segno];

	for (;;)
	{
		nblocks = umfile_nblocks_in_seg(v);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		segno++;

		/* Never recreate a missing segment while measuring size. */
		v = umfile_openseg(ctx, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

/*
 * Truncate without allocation so callers may use it in a critical section.
 * The preceding size lookup must have opened all active segments.
 */
void
umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
				BlockNumber curnblk, BlockNumber nblocks)
{
	BlockNumber priorblocks;
	int			curopensegs;

	Assert(ctx != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);

	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(ctx->rlocator, forknum).str,
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */

	/* Work backward so errors leave a valid descriptor array. */
	curopensegs = ctx->num_open_segs[forknum];
	while (curopensegs > 0)
	{
		UmfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &ctx->seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * Keep an inactive zero-length segment with a valid file
			 * identity.
			 */
			if (FileTruncate(v->umfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->umfd_vfd))));

			if (!RelFileLocatorBackendIsTemp(ctx->rlocator))
				umfile_register_dirty_segment(ctx, forknum, v);

			/* we never drop the 1st segment */
			Assert(v != &ctx->seg_fds[forknum][0]);

			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			/* A segment boundary retains the following zero-length segment. */
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (FileTruncate(v->umfd_vfd, (pgoff_t) lastsegblocks * BLCKSZ,
							 WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->umfd_vfd),
								nblocks)));
			if (!RelFileLocatorBackendIsTemp(ctx->rlocator))
				umfile_register_dirty_segment(ctx, forknum, v);
		}
		else
		{
			/* This and all earlier segments remain active. */
			break;
		}

		curopensegs--;
	}
}

/* Register every active and inactive segment for fsync. */
void
umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	Assert(ctx != NULL);
	Assert(!RelFileLocatorBackendIsTemp(ctx->rlocator));

	/* Open all active segments first. */
	umfile_nblocks(ctx, forknum);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	/* Also register inactive segments, then close those temporary opens. */
	while (umfile_openseg(ctx, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *v = &ctx->seg_fds[forknum][segno - 1];

		umfile_register_dirty_segment(ctx, forknum, v);

		/* Close inactive segments immediately */
		if (segno > min_inactive_seg)
		{
			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, segno - 1);
		}

		segno--;
	}
}

/*
 * Immediately sync issued writes in active and inactive segments.  Syncing
 * inactive segments prevents skip-WAL truncation from reappearing after crash.
 */
void
umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	Assert(ctx != NULL);

	/* Open all active segments first. */
	umfile_nblocks(ctx, forknum);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	/* Also sync inactive segments, then close those temporary opens. */
	while (umfile_openseg(ctx, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *v = &ctx->seg_fds[forknum][segno - 1];

		/* Immediate fsync accounting remains aligned with md.c. */
		if (FileSync(v->umfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->umfd_vfd))));

		/* Close inactive segments immediately */
		if (segno > min_inactive_seg)
		{
			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, segno - 1);
		}

		segno--;
	}
}

int
umfile_fd(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
		  uint32 *off)
{
	UmfdVec    *v;

	Assert(ctx != NULL);

	v = umfile_openfork(ctx, forknum, EXTENSION_FAIL);

	v = umfile_getseg(ctx, forknum, blocknum, false, EXTENSION_FAIL);

	*off = (pgoff_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(*off < (pgoff_t) BLCKSZ * RELSEG_SIZE);

	return FileGetRawDesc(v->umfd_vfd);
}

/* Register a dirty segment, with local fsync fallback if the queue is full. */
static void
umfile_register_dirty_segment(UmbraFileContext *ctx, ForkNumber forknum, UmfdVec *seg)
{
	FileTag		tag;

	INIT_UMFILE_FILETAG(tag, ctx->rlocator.locator, forknum, seg->umfd_segno);

	/* Temp relations should never be fsync'd */
	Assert(!RelFileLocatorBackendIsTemp(ctx->rlocator));

	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */ ))
	{
		instr_time	io_start;

		ereport(DEBUG1,
				(errmsg_internal("could not forward fsync request because request queue is full")));

		io_start = pgstat_prepare_io_time(track_io_timing);

		if (FileSync(seg->umfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(seg->umfd_vfd))));

		/* The fallback has no buffer strategy identity; count it as normal. */
		pgstat_count_io_op_time(IOOBJECT_RELATION, IOCONTEXT_NORMAL,
								IOOP_FSYNC, io_start, 1, 0);
	}
}

/* Schedule a file for deletion after the next checkpoint. */
static void
umfile_register_unlink_segment(RelFileLocatorBackend rlocator, ForkNumber forknum,
							   BlockNumber segno)
{
	FileTag		tag;

	INIT_UMFILE_FILETAG(tag, rlocator.locator, forknum, segno);

	/* Should never be used with temp relations */
	Assert(!RelFileLocatorBackendIsTemp(rlocator));

	RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

/* Forget pending syncs for one segment. */
static void
umfile_register_forget_request(RelFileLocatorBackend rlocator, ForkNumber forknum,
							   BlockNumber segno)
{
	FileTag		tag;

	INIT_UMFILE_FILETAG(tag, rlocator.locator, forknum, segno);

	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

/* Resize a fork's descriptor array. */
static void
umfile_fdvec_resize(UmbraFileContext *ctx,
					ForkNumber forknum,
					int nseg)
{
	if (nseg == 0)
	{
		if (ctx->num_open_segs[forknum] > 0)
		{
			pfree(ctx->seg_fds[forknum]);
			ctx->seg_fds[forknum] = NULL;
		}
	}
	else if (ctx->num_open_segs[forknum] == 0)
	{
		ctx->seg_fds[forknum] =
			MemoryContextAlloc(UmFileCxt, sizeof(UmfdVec) * nseg);
	}
	else if (nseg > ctx->num_open_segs[forknum])
	{
		/* Descriptor growth is rare compared with opening the segment. */
		ctx->seg_fds[forknum] =
			repalloc(ctx->seg_fds[forknum], sizeof(UmfdVec) * nseg);
	}
	else
	{
		/* Do not shrink storage: umfile_truncate() must not allocate. */
	}

	ctx->num_open_segs[forknum] = nseg;
}

/* Return the fixed-size path for a relation segment. */
static UmFilePathStr
umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber segno)
{
	RelPathStr	path;
	UmFilePathStr fullpath;

	path = relpath(rlocator, forknum);

	if (segno > 0)
		sprintf(fullpath.str, "%s.%u", path.str, segno);
	else
		strcpy(fullpath.str, path.str);

	return fullpath;
}

static UmFilePathStr
umfile_segpath_perm(RelFileLocator rlocator, ForkNumber forknum,
					BlockNumber segno)
{
	RelFileLocatorBackend backend_rlocator;

	backend_rlocator.locator = rlocator;
	backend_rlocator.backend = INVALID_PROC_NUMBER;

	return umfile_segpath(backend_rlocator, forknum, segno);
}

/* Open a segment and cache its descriptor. */
static UmfdVec *
umfile_openseg(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber segno,
			   int oflags)
{
	UmfdVec    *v;
	File		fd;
	UmFilePathStr fullpath;

	fullpath = umfile_segpath(ctx->rlocator, forknum, segno);

	/* open the file */
	fd = PathNameOpenFile(fullpath.str, umfile_open_flags() | oflags);

	if (fd < 0)
		return NULL;

	/*
	 * Segments are always opened in order from lowest to highest, so we must
	 * be adding a new one at the end.
	 */
	Assert(segno == ctx->num_open_segs[forknum]);

	umfile_fdvec_resize(ctx, forknum, segno + 1);

	/* fill the entry */
	v = &ctx->seg_fds[forknum][segno];
	v->umfd_vfd = fd;
	v->umfd_segno = segno;

	Assert(umfile_nblocks_in_seg(v) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}

/*
 * umfile_getseg() -- Find the segment of the relation holding the
 *					  specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
static UmfdVec *
umfile_getseg(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
			  bool skipFsync, int behavior)
{
	UmfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	/* some way to handle non-existent segments needs to be specified */
	Assert(behavior &
		   (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL |
			EXTENSION_DONT_OPEN));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < ctx->num_open_segs[forknum])
	{
		v = &ctx->seg_fds[forknum][targetseg];
		return v;
	}

	/* The caller only wants the segment if we already had it open. */
	if (behavior & EXTENSION_DONT_OPEN)
		return NULL;

	/*
	 * The target segment is not yet open. Iterate over all the segments
	 * between the last opened and the target segment. This way missing
	 * segments either raise an error, or get created (according to
	 * 'behavior'). Start with either the last opened, or the first segment if
	 * none was opened before.
	 */
	if (ctx->num_open_segs[forknum] > 0)
		v = &ctx->seg_fds[forknum][ctx->num_open_segs[forknum] - 1];
	else
	{
		v = umfile_openfork(ctx, forknum, behavior);
		if (!v)
			return NULL;		/* if behavior & EXTENSION_RETURN_NULL */
	}

	for (nextsegno = ctx->num_open_segs[forknum];
		 nextsegno <= targetseg; nextsegno++)
	{
		BlockNumber nblocks = umfile_nblocks_in_seg(v);
		int			flags = 0;

		Assert(nextsegno == v->umfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & EXTENSION_CREATE) ||
			(InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Normally we will create new segments only if authorized by the
			 * caller (i.e., we are doing umfile_extend()).  But when doing
			 * WAL recovery, create segments anyway; this allows cases such as
			 * replaying WAL data that has a write into a high-numbered
			 * segment of a relation that was later deleted. We want to go
			 * ahead and create the segments so we can finish out the replay.
			 *
			 * We have to maintain the invariant that segments before the last
			 * active segment are of size RELSEG_SIZE; therefore, if
			 * extending, pad them out with zeroes if needed.  (This only
			 * matters if in recovery, or if the caller is extending the
			 * relation discontiguously, but that can happen in hash indexes.)
			 */
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE,
													 MCXT_ALLOC_ZERO);

				umfile_extend(ctx, forknum,
							  nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
							  zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			/*
			 * When not extending, only open the next segment if the current
			 * one is exactly RELSEG_SIZE.  If not (this branch), either
			 * return NULL or fail.
			 */
			if (behavior & EXTENSION_RETURN_NULL)
			{
				/*
				 * Some callers discern between reasons for umfile_getseg()
				 * returning NULL based on errno. As there's no failing
				 * syscall involved in this case, explicitly set errno to
				 * ENOENT, as that seems the closest interpretation.
				 */
				errno = ENOENT;
				return NULL;
			}

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							umfile_segpath(ctx->rlocator, forknum, nextsegno).str,
							blkno, nblocks)));
		}

		v = umfile_openseg(ctx, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							umfile_segpath(ctx->rlocator, forknum, nextsegno).str,
							blkno)));
		}
	}

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber
umfile_nblocks_in_seg(UmfdVec *seg)
{
	pgoff_t		len;

	len = FileSize(seg->umfd_vfd);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(seg->umfd_vfd))));
	/* note that this calculation will ignore any partial block at EOF */
	return (BlockNumber) (len / BLCKSZ);
}

/*
 * Sync a file to disk, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
umfilesyncfiletag(const FileTag *ftag, char *path)
{
	UmFilePathStr p;
	File		file;
	instr_time	io_start;
	int			result;
	int			save_errno;

	p = umfile_segpath_perm(ftag->rlocator, ftag->forknum, ftag->segno);
	strlcpy(path, p.str, MAXPGPATH);

	file = PathNameOpenFile(path, umfile_open_flags());
	if (file < 0)
		return -1;

	io_start = pgstat_prepare_io_time(track_io_timing);

	/* Sync the file. */
	result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	save_errno = errno;

	FileClose(file);

	pgstat_count_io_op_time(IOOBJECT_RELATION, IOCONTEXT_NORMAL,
							IOOP_FSYNC, io_start, 1, 0);

	errno = save_errno;
	return result;
}

/*
 * Unlink a file, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
umfileunlinkfiletag(const FileTag *ftag, char *path)
{
	UmFilePathStr p;

	/* Compute the path. */
	p = umfile_segpath_perm(ftag->rlocator, ftag->forknum, ftag->segno);
	strlcpy(path, p.str, MAXPGPATH);

	/* Try to unlink the file. */
	return unlink(path);
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
umfilefiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
	/*
	 * For now we only use filter requests as a way to drop all scheduled
	 * callbacks relating to a given database, when dropping the database.
	 * We'll return true for all candidates that have the same database OID as
	 * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
	 */
	return ftag->rlocator.dbOid == candidate->rlocator.dbOid;
}
