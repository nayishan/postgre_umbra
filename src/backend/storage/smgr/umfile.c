/*-------------------------------------------------------------------------
 *
 * umfile.c
 *	  This code manages Umbra relation segment files.
 *
 * This layer is an Umbra-owned copy of md.c's md-style segment file
 * management.  Standard forks use PostgreSQL relation fork paths.  Umbra's
 * private map fork uses a local path convention below this layer and is not
 * exposed as a PostgreSQL fork.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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
#include "storage/ummap.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * The Umbra file manager keeps track of open file descriptors in its own
 * descriptor pool.  This is done to make it easier to support relations that
 * are larger than the operating system's file size limit (often 2GBytes).  In
 * order to do that, we break relations up into "segment" files that are each
 * shorter than the OS file size limit.  The segment size is set by the
 * RELSEG_SIZE configuration constant in pg_config.h.
 *
 * On disk, a relation must consist of consecutively numbered segment files
 * in the pattern
 *	-- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *	-- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *	-- Optionally, any number of inactive segments of size 0 blocks.
 * The full and partial segments are collectively the "active" segments.
 * Inactive segments are those that once contained data but are currently not
 * needed because of an umfile_truncate() operation.  The reason for leaving
 * them present at size zero, rather than unlinking them, is that other
 * backends and/or the checkpointer might be holding open file references to
 * such segments.  If the relation expands again after umfile_truncate(), such
 * that a deactivated segment becomes active again, it is important that such
 * file references still be valid --- else data might get written out to an
 * unlinked old copy of a segment file that will eventually disappear.
 *
 * RELSEG_SIZE must fit into BlockNumber; but since we expose its value
 * as an integer GUC, it actually needs to fit in signed int.  It's worth
 * having a cross-check for this since configure's --with-segsize options
 * could let people select insane values.
 */
StaticAssertDecl(RELSEG_SIZE > 0 && RELSEG_SIZE <= INT_MAX,
				 "RELSEG_SIZE must fit in an integer");

/*
 * File descriptors are stored in the per-fork seg_fds arrays inside
 * UmbraFileContext. The length of these arrays is stored in num_open_segs.
 * Note that a fork's num_open_segs having a specific value does not
 * necessarily mean the relation doesn't have additional segments; we may
 * just not have opened the next segment yet.  (We could not have "all
 * segments are in the array" as an invariant anyway, since another backend
 * could extend the relation while we aren't looking.)  We do not have
 * entries for inactive segments, however; as soon as we find a partial
 * segment, we assume that any subsequent segments are inactive.
 *
 * The entire UmfdVec array is palloc'd in the UmFileCxt memory context.
 */

typedef struct UmfdVec
{
	File		umfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber umfd_segno;		/* segment number, from 0 */
} UmfdVec;

struct UmbraFileContext
{
	RelFileLocatorBackend rlocator;
	int			num_open_segs[UMBRA_NUM_FORKS];
	UmfdVec    *seg_fds[UMBRA_NUM_FORKS];
};

static MemoryContext UmFileCxt;		/* context for all UmfdVec objects */


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


/*
 * Fixed-length string to represent paths to files that need to be built by
 * umfile.
 *
 * The maximum number of segments is MaxBlockNumber / RELSEG_SIZE, where
 * RELSEG_SIZE can be set to 1 (for testing only).
 */
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
static bool umfile_forknum_is_valid(ForkNumber forknum);
static RelPathStr umfile_relpath(RelFileLocatorBackend rlocator,
								 ForkNumber forknum);
static UmFilePathStr umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum,
									BlockNumber segno);
static UmFilePathStr umfile_segpath_perm(RelFileLocator rlocator,
										 ForkNumber forknum, BlockNumber segno);
static UmfdVec *umfile_openseg(UmbraFileContext *ctx, ForkNumber forknum,
							   BlockNumber segno, int oflags);
static UmfdVec *umfile_getseg(UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber umfile_nblocks_in_seg(UmfdVec *seg);
static inline int umfile_open_flags(ForkNumber forknum);

static PgAioResult umfile_readv_complete(PgAioHandle *ioh, PgAioResult prior_result, uint8 cb_data);
static void umfile_readv_report(PgAioResult result, const PgAioTargetData *td, int elevel);

const PgAioHandleCallbacks aio_umfile_readv_cb = {
	.complete_shared = umfile_readv_complete,
	.report = umfile_readv_report,
};


static inline int
umfile_open_flags(ForkNumber forknum)
{
	int			flags = O_RDWR | PG_BINARY;

	/* MAP metadata supports sector-sized superblock I/O. */
	if (forknum != UMBRA_MAP_FORKNUM &&
		(io_direct_flags & IO_DIRECT_DATA))
		flags |= PG_O_DIRECT;

	return flags;
}

/*
 * umfile_init() -- Initialize private state for Umbra file manager.
 */
void
umfile_init(void)
{
	if (UmFileCxt != NULL)
		return;

	UmFileCxt = AllocSetContextCreate(TopMemoryContext,
									  "UmbraFile",
									  ALLOCSET_DEFAULT_SIZES);
}

/*
 * umfile_exists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
umfile_exists(UmbraFileContext *ctx, ForkNumber forknum)
{
	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	/*
	 * Close it first, to ensure that we notice if the fork has been unlinked
	 * since we opened it.  As an optimization, we can skip that in recovery,
	 * which already closes relations when dropping them.
	 */
	if (!InRecovery)
		umfile_close(ctx, forknum);

	return (umfile_openfork(ctx, forknum, EXTENSION_RETURN_NULL) != NULL);
}

/*
 * umfile_create() -- Create a new relation on Umbra storage.
 *
 * If isRedo is true, it's okay for the relation to exist already.  Return
 * true only when this call created the first segment.
 */
bool
umfile_create(UmbraFileContext *ctx, ForkNumber forknum, bool isRedo)
{
	UmfdVec    *v;
	RelPathStr	path;
	File		fd;
	bool		created = true;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	if (isRedo && ctx->num_open_segs[forknum] > 0)
		return false;			/* created and opened already... */

	Assert(ctx->num_open_segs[forknum] == 0);

	/*
	 * We may be using the target table space for the first time in this
	 * database, so create a per-database subdirectory if needed.
	 *
	 * XXX this is a fairly ugly violation of module layering, but this seems
	 * to be the best place to put the check.  Maybe TablespaceCreateDbspace
	 * should be here and not in commands/tablespace.c?  But that would imply
	 * importing a lot of stuff that smgr.c oughtn't know, either.
	 */
	TablespaceCreateDbspace(ctx->rlocator.locator.spcOid,
							ctx->rlocator.locator.dbOid,
							isRedo);

	path = umfile_relpath(ctx->rlocator, forknum);

	fd = PathNameOpenFile(path.str,
						  umfile_open_flags(forknum) | O_CREAT | O_EXCL);

	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
		{
			fd = PathNameOpenFile(path.str, umfile_open_flags(forknum));
			created = false;
		}
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

	return created;
}

/*
 * Read or write a prefix of an existing block.  MAP superblocks use this to
 * keep their physical slot BLCKSZ-sized while transferring one disk sector.
 */
void
umfile_read_bytes(UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum, void *buffer, int nbytes)
{
	pgoff_t		seekpos;
	int			readbytes;
	UmfdVec    *v;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	v = umfile_getseg(ctx, forknum, blocknum, false, EXTENSION_FAIL);
	seekpos = (pgoff_t) BLCKSZ *
		(blocknum % ((BlockNumber) RELSEG_SIZE));

	readbytes = FileRead(v->umfd_vfd, buffer, nbytes, seekpos,
						 WAIT_EVENT_DATA_FILE_READ);
	if (readbytes != nbytes)
	{
		if (readbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							FilePathName(v->umfd_vfd))));
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read file \"%s\": read only %d of %d bytes at block %u",
						FilePathName(v->umfd_vfd), readbytes, nbytes,
						blocknum)));
	}
}

void
umfile_write_bytes(UmbraFileContext *ctx, ForkNumber forknum,
				   BlockNumber blocknum, const void *buffer, int nbytes,
				   bool skipFsync)
{
	pgoff_t		seekpos;
	int			written;
	UmfdVec    *v;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	v = umfile_getseg(ctx, forknum, blocknum, skipFsync, EXTENSION_FAIL);
	seekpos = (pgoff_t) BLCKSZ *
		(blocknum % ((BlockNumber) RELSEG_SIZE));

	written = FileWrite(v->umfd_vfd, buffer, nbytes, seekpos,
						WAIT_EVENT_DATA_FILE_WRITE);
	if (written != nbytes)
	{
		if (written < 0)
		{
			bool		enospc = errno == ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							FilePathName(v->umfd_vfd)),
					 enospc ? errhint("Check free disk space.") : 0));
		}
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->umfd_vfd), written, nbytes,
						blocknum),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !RelFileLocatorBackendIsTemp(ctx->rlocator))
		umfile_register_dirty_segment(ctx, forknum, v);
}

/*
 * umfile_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileLocatorBackend --- by the time this is
 * called, there won't be an SMgrRelation hashtable entry anymore.
 *
 * forknum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenumber
 * from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenumber as
 *	  the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as we do at wal_level=minimal), the contents of
 * the file would be lost forever.  By leaving the empty file until after the
 * next checkpoint, we prevent reassignment of the relfilenumber until it's
 * safe, because relfilenumber assignment skips over any existing file.
 *
 * Additional segments, if any, are truncated and then unlinked.  The reason
 * for truncating is that other backends may still hold open FDs for these at
 * the smgr level, so that the kernel can't remove the file yet.  We want to
 * reclaim the disk space right away despite that.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenumber.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.  Other backends shouldn't have open
 * FDs for them, either.
 *
 * We also don't do it while performing a binary upgrade.  There is no reuse
 * hazard in that case, since after a crash or even a simple ERROR, the
 * upgrade fails and the whole cluster must be recreated from scratch.
 * Furthermore, it is important to remove the files from disk immediately,
 * because we may be about to reuse the same relfilenumber.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenumber from being recycled.  Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: we currently just never warn about ENOENT at all.  We could warn in
 * the main-fork, non-isRedo case, but it doesn't seem worth the trouble.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	/* Now do the per-fork work */
	if (forknum == InvalidForkNumber)
	{
		for (int forkidx = 0; forkidx < UMBRA_NUM_FORKS; forkidx++)
			umfile_unlinkfork(rlocator, (ForkNumber) forkidx, isRedo);
	}
	else
		umfile_unlinkfork(rlocator, forknum, isRedo);
}

/*
 * Truncate a file to release disk space.
 */
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

	Assert(umfile_forknum_is_valid(forknum));

	path = umfile_relpath(rlocator, forknum);

	/*
	 * Truncate and then unlink the first segment, or just register a request
	 * to unlink it later, as described in the comments for umfile_unlink().
	 */
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

	/*
	 * Delete any additional segments.
	 *
	 * Note that because we loop until getting ENOENT, we will correctly
	 * remove all inactive segments as well as active ones.  Ideally we'd
	 * continue the loop until getting exactly that errno, but that risks an
	 * infinite loop if the problem is directory-wide (for instance, if we
	 * suddenly can't read the data directory itself).  We compromise by
	 * continuing after a non-ENOENT truncate error, but stopping after any
	 * unlink error.  If there is indeed a directory-wide problem, additional
	 * unlink attempts wouldn't work anyway.
	 */
	if (ret >= 0 || errno != ENOENT)
	{
		UmFilePathStr segpath;
		BlockNumber segno;

		for (segno = 1;; segno++)
		{
			segpath = umfile_segpath(rlocator, forknum, segno);

			if (!RelFileLocatorBackendIsTemp(rlocator))
			{
				/*
				 * Prevent other backends' fds from holding on to the disk
				 * space.  We're done if we see ENOENT, though.
				 */
				if (do_truncate(segpath.str) < 0 && errno == ENOENT)
					break;

				/*
				 * Forget any pending sync requests for this segment before we
				 * try to unlink.
				 */
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

/*
 * umfile_extend() -- Add a block to the specified relation.
 *
 * The semantics are nearly the same as umfile_writev(): write at the
 * specified position.  However, this is to be used for the case of
 * extending a relation (i.e., blocknum is at or beyond the current
 * EOF).  Note that we assume writing a block beyond current EOF
 * causes intervening file space to become filled with zeroes.
 */
void
umfile_extend(UmbraFileContext *ctx, ForkNumber forknum,
			  BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	pgoff_t		seekpos;
	int			nbytes;
	UmfdVec    *v;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

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
						umfile_relpath(ctx->rlocator, forknum).str,
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
	Assert(umfile_forknum_is_valid(forknum));
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
						umfile_relpath(ctx->rlocator, forknum).str,
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

			/*
			 * Even if we don't want to use fallocate, we can still extend a
			 * bit more efficiently than writing each 8kB block individually.
			 * pg_pwrite_zeros() (via FileZero()) uses pg_pwritev_with_retry()
			 * to avoid multiple writes or needing a zeroed buffer for the
			 * whole length of the extension.
			 */
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

/*
 * umfile_openfork() -- Open one fork of the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as
 * EXTENSION_FAIL; EXTENSION_CREATE means it's OK to extend an
 * existing relation, not to invent one out of whole cloth.
 */
static UmfdVec *
umfile_openfork(UmbraFileContext *ctx, ForkNumber forknum, int behavior)
{
	UmfdVec    *v;
	RelPathStr	path;
	File		fd;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	/* No work if already open */
	if (ctx->num_open_segs[forknum] > 0)
		return &ctx->seg_fds[forknum][0];

	path = umfile_relpath(ctx->rlocator, forknum);

	fd = PathNameOpenFile(path.str, umfile_open_flags(forknum));

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

/*
 * umfile_open_temporary() -- Create an Umbra file context for a locator.
 */
UmbraFileContext *
umfile_open_temporary(RelFileLocatorBackend rlocator)
{
	UmbraFileContext *ctx;

	if (UmFileCxt == NULL)
		umfile_init();

	ctx = MemoryContextAllocZero(UmFileCxt, sizeof(UmbraFileContext));
	ctx->rlocator = rlocator;

	return ctx;
}

/*
 * umfile_open() -- Initialize newly-opened Umbra relation file context.
 */
UmbraFileContext *
umfile_open(SMgrRelation reln)
{
	Assert(reln != NULL);

	return umfile_open_temporary(reln->smgr_rlocator);
}

/*
 * umfile_close() -- Close the specified relation, if it isn't closed already.
 */
void
umfile_close(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			nopensegs;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	nopensegs = ctx->num_open_segs[forknum];

	/* No work if already closed */
	if (nopensegs == 0)
		return;

	/* close segments starting from the end */
	while (nopensegs > 0)
	{
		UmfdVec    *v = &ctx->seg_fds[forknum][nopensegs - 1];

		FileClose(v->umfd_vfd);
		umfile_fdvec_resize(ctx, forknum, nopensegs - 1);
		nopensegs--;
	}
}

/*
 * umfile_destroy() -- Release an Umbra file context.
 */
void
umfile_destroy(UmbraFileContext *ctx)
{
	if (ctx == NULL)
		return;

	for (int forkidx = 0; forkidx < UMBRA_NUM_FORKS; forkidx++)
		umfile_close(ctx, (ForkNumber) forkidx);

	pfree(ctx);
}

/*
 * umfile_prefetch() -- Initiate asynchronous read of the specified blocks of a relation
 */
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

/*
 * Convert an array of buffer address into an array of iovec objects, and
 * return the number that were required.  'iov' must have enough space for up
 * to 'nblocks' elements, but the number used may be less depending on
 * merging.  In the case of a run of fully contiguous buffers, a single iovec
 * will be populated that can be handled as a plain non-vectored I/O.
 */
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

	/* Start the first iovec off with the first buffer. */
	iovp = &iov[0];
	iovp->iov_base = buffers[0];
	iovp->iov_len = BLCKSZ;
	iovcnt = 1;

	/* Try to merge the rest. */
	for (int i = 1; i < nblocks; ++i)
	{
		void	   *buffer = buffers[i];

		if (((char *) iovp->iov_base + iovp->iov_len) == buffer)
		{
			/* Contiguous with the last iovec. */
			iovp->iov_len += BLCKSZ;
		}
		else
		{
			/* Need a new iovec. */
			iovp++;
			iovp->iov_base = buffer;
			iovp->iov_len = BLCKSZ;
			iovcnt++;
		}
	}

	return iovcnt;
}

/*
 * umfile_maxcombine() -- Return the maximum number of total blocks that can be
 *						   combined with an IO starting at blocknum.
 */
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

/*
 * umfile_readv() -- Read the specified blocks from a relation.
 */
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

		/*
		 * Inner loop to continue after a short read.  We'll keep going until
		 * we hit EOF rather than assuming that a short read means we hit the
		 * end.
		 */
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
				 * We are at or past EOF, or we read a partial block at EOF.
				 * Normally this is an error; upper levels should never try to
				 * read a nonexistent block.  However, if zero_damaged_pages
				 * is ON or we are InRecovery, we should instead return zeroes
				 * without complaining.  This allows, for example, the case of
				 * trying to update a block that was later truncated away.
				 *
				 * NB: We think that this codepath is unreachable in recovery
				 * and incomplete with zero_damaged_pages, as missing segments
				 * are not created. Putting blocks into the buffer-pool that
				 * do not exist on disk is rather problematic, as it will not
				 * be found by scans that rely on smgrnblocks(), as they are
				 * beyond EOF. It also can cause weird problems with relation
				 * extension, as relation extension does not expect blocks
				 * beyond EOF to exist.
				 *
				 * Therefore we do not want to copy the logic into
				 * umfile_startreadv_physical(), where it would have to be
				 * more complicated due to potential differences in the
				 * zero_damaged_pages setting between the definer and
				 * completor of IO.
				 *
				 * For PG 18, we are putting an Assert(false) in
				 * umfile_readv() (triggering failures in assertion-enabled
				 * builds, but continuing to work in production builds).
				 * Afterwards we plan to remove this code entirely.
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

			/* One loop should usually be enough. */
			transferred_this_segment += nbytes;
			Assert(transferred_this_segment <= size_this_segment);
			if (transferred_this_segment == size_this_segment)
				break;

			/* Adjust position and vectors after a short read. */
			seekpos += nbytes;
			iovcnt = compute_remaining_iovec(iov, iov, iovcnt, nbytes);
		}

		nblocks -= nblocks_this_segment;
		buffers += nblocks_this_segment;
		blocknum += nblocks_this_segment;
	}
}

/*
 * umfile_startreadv_physical() -- Asynchronous physical read.
 */
void
umfile_startreadv_physical(PgAioHandle *ioh, UmbraFileContext *ctx,
						   ForkNumber forknum, BlockNumber lblkno,
						   BlockNumber pblkno, void **buffers,
						   BlockNumber nblocks)
{
	pgoff_t		seekpos;
	UmfdVec    *v;
	BlockNumber nblocks_this_segment;
	struct iovec *iov;
	int			iovcnt;
	int			ret;

	v = umfile_getseg(ctx, forknum, pblkno, false,
					  EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (pgoff_t) BLCKSZ * (pblkno % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (pgoff_t) BLCKSZ * RELSEG_SIZE);

	nblocks_this_segment =
		Min(nblocks,
			RELSEG_SIZE - (pblkno % ((BlockNumber) RELSEG_SIZE)));

	if (nblocks_this_segment != nblocks)
		elog(ERROR, "read crossing segment boundary");

	iovcnt = pgaio_io_get_iovec(ioh, &iov);

	Assert(nblocks <= iovcnt);

	iovcnt = buffers_to_iovec(iov, buffers, nblocks_this_segment);

	Assert(iovcnt <= nblocks_this_segment);

	if (!(io_direct_flags & IO_DIRECT_DATA))
		pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);

	pgaio_io_register_callbacks(ioh, PGAIO_HCB_UMFILE_READV, 0);

	ret = FileStartReadV(ioh, v->umfd_vfd, iovcnt, seekpos, WAIT_EVENT_DATA_FILE_READ);
	if (ret != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not start reading blocks %u..%u in file \"%s\": %m",
						lblkno,
						lblkno + nblocks_this_segment - 1,
						FilePathName(v->umfd_vfd))));

	/*
	 * The error checks corresponding to the post-read checks in
	 * umfile_readv() are in umfile_readv_complete().
	 *
	 * However we chose, at least for now, to not implement the
	 * zero_damaged_pages logic present in umfile_readv(). As outlined in
	 * umfile_readv() that logic is rather problematic, and we want to get rid
	 * of it. Here equivalent logic would have to be more complicated due to
	 * potential differences in the zero_damaged_pages setting between the
	 * definer and completor of IO.
	 */
}

/*
 * umfile_writev() -- Write the supplied blocks at the appropriate location.
 *
 * This is to be used only for updating already-existing blocks of a
 * relation (ie, those before the current EOF).  To extend a relation,
 * use umfile_extend().
 */
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

		/*
		 * Inner loop to continue after a short write.  If the reason is that
		 * we're out of disk space, a future attempt should get an ENOSPC
		 * error from the kernel.
		 */
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

	/*
	 * Issue flush requests in as few requests as possible; have to split at
	 * segment boundaries though, since those are actually separate files.
	 */
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		pgoff_t		seekpos;
		UmfdVec    *v;
		int			segnum_start,
					segnum_end;

		v = umfile_getseg(ctx, forknum, blocknum, true, EXTENSION_DONT_OPEN);

		/*
		 * We might be flushing buffers of already removed relations, that's
		 * ok, just ignore that case.  If the segment file wasn't open already
		 * (ie from a recent umfile_writev()), then we don't want to re-open
		 * it, to avoid a race with PROCSIGNAL_BARRIER_SMGRRELEASE that might
		 * leave us with a descriptor to a file that is about to be unlinked.
		 */
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

/*
 * umfile_nblocks() -- Get the number of blocks stored in a relation.
 *
 * Important side effect: all active segments of the relation are opened
 * and added to the seg_fds array.  If this routine has not been
 * called, then only segments up to the last one actually touched
 * are present in the array.
 */
BlockNumber
umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum)
{
	UmfdVec    *v;
	BlockNumber nblocks;
	BlockNumber segno;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	umfile_openfork(ctx, forknum, EXTENSION_FAIL);

	/* umfile_openfork has opened the first segment */
	Assert(ctx->num_open_segs[forknum] > 0);

	/*
	 * Start from the last open segments, to avoid redundant seeks.  We have
	 * previously verified that these segments are exactly RELSEG_SIZE long,
	 * and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.  We rely on higher code levels to handle that
	 * scenario by closing and re-opening the umfile fd, which is handled via
	 * relcache flush.  (Since the checkpointer doesn't participate in
	 * relcache flush, it could have segment entries for inactive segments;
	 * that's OK because the checkpointer never needs to compute relation
	 * size.)
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

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		/*
		 * We used to pass O_CREAT here, but that has the disadvantage that it
		 * might create a segment which has vanished through some operating
		 * system misadventure.  In such a case, creating the segment here
		 * undermines umfile_getseg's attempts to notice and report an error
		 * upon access to a missing segment.
		 */
		v = umfile_openseg(ctx, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

/*
 * umfile_truncate() -- Truncate relation to specified number of blocks.
 *
 * Guaranteed not to allocate memory, so it can be used in a critical section.
 * Caller must have called smgrnblocks() to obtain curnblk while holding a
 * sufficient lock to prevent a change in relation size, and not used any smgr
 * functions for this relation or handled interrupts in between.  This makes
 * sure we have opened all active segments, so that truncate loop will get
 * them all!
 *
 * If nblocks > curnblk, the request is ignored when we are InRecovery,
 * otherwise, an error is raised.
 */
void
umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
				BlockNumber curnblk, BlockNumber nblocks)
{
	BlockNumber priorblocks;
	int			curopensegs;

	Assert(ctx != NULL);
	Assert(umfile_forknum_is_valid(forknum));

	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						umfile_relpath(ctx->rlocator, forknum).str,
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */

	/*
	 * Truncate segments, starting at the last one. Starting at the end makes
	 * managing the memory for the fd array easier, should there be errors.
	 */
	curopensegs = ctx->num_open_segs[forknum];
	while (curopensegs > 0)
	{
		UmfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &ctx->seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
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
			/*
			 * This is the last segment we want to keep. Truncate the file to
			 * the right length. NOTE: if nblocks is exactly a multiple K of
			 * RELSEG_SIZE, we will truncate the K+1st segment to 0 length but
			 * keep it. This adheres to the invariant given in the header
			 * comments.
			 */
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
			/*
			 * We still need this segment, so nothing to do for this and any
			 * earlier segment.
			 */
			break;
		}

		curopensegs--;
	}
}

/*
 * umfile_registersync() -- Mark whole relation as needing fsync
 */
void
umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	Assert(ctx != NULL);
	Assert(!RelFileLocatorBackendIsTemp(ctx->rlocator));

	/*
	 * NOTE: umfile_nblocks makes sure we have opened all active segments, so
	 * that the loop below will get them all!
	 */
	umfile_nblocks(ctx, forknum);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	/*
	 * Temporarily open inactive segments, then close them after sync.  There
	 * may be some inactive segments left opened after error, but that is
	 * harmless.  We don't bother to clean them up and take a risk of further
	 * trouble.  The next umfile_close() will soon close them.
	 */
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
 * umfile_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then umfile_truncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	Assert(ctx != NULL);

	/*
	 * NOTE: umfile_nblocks makes sure we have opened all active segments, so
	 * that the loop below will get them all!
	 */
	umfile_nblocks(ctx, forknum);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	/*
	 * Temporarily open inactive segments, then close them after sync.  There
	 * may be some inactive segments left opened after fsync() error, but that
	 * is harmless.  We don't bother to clean them up and take a risk of
	 * further trouble.  The next umfile_close() will soon close them.
	 */
	while (umfile_openseg(ctx, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *v = &ctx->seg_fds[forknum][segno - 1];

		/*
		 * fsyncs done through umfile_immedsync() should be tracked in a
		 * separate IOContext than those done through umfilesyncfiletag() to
		 * differentiate between unavoidable client backend fsyncs (e.g. those
		 * done during index build) and those which ideally would have been
		 * done by the checkpointer. Since other IO operations bypassing the
		 * buffer manager could also be tracked in such an IOContext, wait
		 * until these are also tracked to track immediate fsyncs.
		 */
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

/*
 * umfile_register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * ProcessSyncRequests to process later.  Otherwise, try to pass off the
 * fsync request to the checkpointer process.  If that fails, just do the
 * fsync locally before returning (we hope this will not happen often
 * enough to be a performance problem).
 */
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

		/*
		 * We have no way of knowing if the current IOContext is
		 * IOCONTEXT_NORMAL or IOCONTEXT_[BULKREAD, BULKWRITE, VACUUM] at this
		 * point, so count the fsync as being in the IOCONTEXT_NORMAL
		 * IOContext. This is probably okay, because the number of backend
		 * fsyncs doesn't say anything about the efficacy of the
		 * BufferAccessStrategy. And counting both fsyncs done in
		 * IOCONTEXT_NORMAL and IOCONTEXT_[BULKREAD, BULKWRITE, VACUUM] under
		 * IOCONTEXT_NORMAL is likely clearer when investigating the number of
		 * backend fsyncs.
		 */
		pgstat_count_io_op_time(IOOBJECT_RELATION, IOCONTEXT_NORMAL,
								IOOP_FSYNC, io_start, 1, 0);
	}
}

/*
 * umfile_register_unlink_segment() -- Schedule a file to be deleted after next checkpoint
 */
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

/*
 * umfile_register_forget_request() -- forget any fsyncs for a relation fork's segment
 */
static void
umfile_register_forget_request(RelFileLocatorBackend rlocator, ForkNumber forknum,
							   BlockNumber segno)
{
	FileTag		tag;

	INIT_UMFILE_FILETAG(tag, rlocator.locator, forknum, segno);

	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

/*
 * umfile_fdvec_resize() -- Resize the fork's open segments array
 */
static void
umfile_fdvec_resize(UmbraFileContext *ctx,
					ForkNumber forknum,
					int nseg)
{
	Assert(umfile_forknum_is_valid(forknum));

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
		/*
		 * It doesn't seem worthwhile complicating the code to amortize
		 * repalloc() calls.  Those are far faster than PathNameOpenFile() or
		 * FileClose(), and the memory context internally will sometimes avoid
		 * doing an actual reallocation.
		 */
		ctx->seg_fds[forknum] =
			repalloc(ctx->seg_fds[forknum], sizeof(UmfdVec) * nseg);
	}
	else
	{
		/*
		 * We don't reallocate a smaller array, because we want
		 * umfile_truncate() to be able to promise that it won't allocate
		 * memory, so that it is allowed in a critical section.  This means
		 * that a bit of space in the array is now wasted, until the next time
		 * we add a segment and reallocate.
		 */
	}

	ctx->num_open_segs[forknum] = nseg;
}

static bool
umfile_forknum_is_valid(ForkNumber forknum)
{
	return forknum >= 0 && (int) forknum < UMBRA_NUM_FORKS;
}

static RelPathStr
umfile_relpath(RelFileLocatorBackend rlocator, ForkNumber forknum)
{
	if (forknum == UMBRA_MAP_FORKNUM)
		return ummap_relpath(rlocator);

	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	return relpath(rlocator, forknum);
}

/*
 * Return the filename for the specified segment of the relation. The
 * returned string is palloc'd.
 */
static UmFilePathStr
umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber segno)
{
	RelPathStr	path;
	UmFilePathStr fullpath;

	path = umfile_relpath(rlocator, forknum);

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

/*
 * Open the specified segment of the relation,
 * and make an UmfdVec object for it.  Returns NULL on failure.
 */
static UmfdVec *
umfile_openseg(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber segno,
			   int oflags)
{
	UmfdVec    *v;
	File		fd;
	UmFilePathStr fullpath;

	fullpath = umfile_segpath(ctx->rlocator, forknum, segno);

	/* open the file */
	fd = PathNameOpenFile(fullpath.str,
						  umfile_open_flags(forknum) | oflags);

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

	file = PathNameOpenFile(path, umfile_open_flags(ftag->forknum));
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

/*
 * AIO completion callback for umfile_startreadv_physical().
 */
static PgAioResult
umfile_readv_complete(PgAioHandle *ioh, PgAioResult prior_result, uint8 cb_data)
{
	PgAioTargetData *td = pgaio_io_get_target_data(ioh);
	PgAioResult result = prior_result;

	if (prior_result.result < 0)
	{
		result.status = PGAIO_RS_ERROR;
		result.id = PGAIO_HCB_UMFILE_READV;
		/* For "hard" errors, track the error number in error_data */
		result.error_data = -prior_result.result;
		result.result = 0;

		/*
		 * Immediately log a message about the IO error, but only to the
		 * server log. The reason to do so immediately is that the originator
		 * might not process the query result immediately (because it is busy
		 * doing another part of query processing) or at all (e.g. if it was
		 * cancelled or errored out due to another IO also failing).  The
		 * definer of the IO will emit an ERROR when processing the IO's
		 * results
		 */
		pgaio_result_report(result, td, LOG_SERVER_ONLY);

		return result;
	}

	/*
	 * As explained above smgrstartreadv(), the smgr API operates on the level
	 * of blocks, rather than bytes. Convert.
	 */
	result.result /= BLCKSZ;

	Assert(result.result <= td->smgr.nblocks);

	if (result.result == 0)
	{
		/* consider 0 blocks read a failure */
		result.status = PGAIO_RS_ERROR;
		result.id = PGAIO_HCB_UMFILE_READV;
		result.error_data = 0;

		/* see comment above the "hard error" case */
		pgaio_result_report(result, td, LOG_SERVER_ONLY);

		return result;
	}

	if (result.status != PGAIO_RS_ERROR &&
		result.result < td->smgr.nblocks)
	{
		/* partial reads should be retried at upper level */
		result.status = PGAIO_RS_PARTIAL;
		result.id = PGAIO_HCB_UMFILE_READV;
	}

	return result;
}

/*
 * AIO error reporting callback for umfile_startreadv_physical().
 *
 * Errors are encoded as follows:
 * - PgAioResult.error_data != 0 encodes IO that failed with that errno
 * - PgAioResult.error_data == 0 encodes IO that didn't read all data
 */
static void
umfile_readv_report(PgAioResult result, const PgAioTargetData *td, int elevel)
{
	RelPathStr	path;

	path = relpathbackend(td->smgr.rlocator,
						  td->smgr.is_temp ? MyProcNumber : INVALID_PROC_NUMBER,
						  td->smgr.forkNum);

	if (result.error_data != 0)
	{
		/* for errcode_for_file_access() and %m */
		errno = result.error_data;

		ereport(elevel,
				errcode_for_file_access(),
				errmsg("could not read blocks %u..%u in file \"%s\": %m",
					   td->smgr.blockNum,
					   td->smgr.blockNum + td->smgr.nblocks - 1,
					   path.str));
	}
	else
	{
		/*
		 * NB: This will typically only be output in debug messages, while
		 * retrying a partial IO.
		 */
		ereport(elevel,
				errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("could not read blocks %u..%u in file \"%s\": read only %zu of %zu bytes",
					   td->smgr.blockNum,
					   td->smgr.blockNum + td->smgr.nblocks - 1,
					   path.str,
					   result.result * (size_t) BLCKSZ,
					   td->smgr.nblocks * (size_t) BLCKSZ));
	}
}
