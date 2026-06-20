/*-------------------------------------------------------------------------
 *
 * umfile.c
 *	  Umbra file/segment manager.
 *
 * This layer owns backend-local file contexts keyed by RelFileLocatorBackend
 * and provides low-level physical file/segment handling for Umbra forks.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif

#include "access/xlogutils.h"
#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "commands/tablespace.h"
#include "common/file_utils.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/aio.h"
#include "storage/fd.h"
#include "storage/sync.h"
#include "storage/umfile.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Like md.c, we split relation storage into segments of RELSEG_SIZE blocks.
 */

/* Behavior flags for segment open helpers. */
#define UM_EXTENSION_FAIL				(1 << 0)
#define UM_EXTENSION_RETURN_NULL		(1 << 1)
#define UM_EXTENSION_CREATE				(1 << 2)
#define UM_EXTENSION_CREATE_RECOVERY	(1 << 3)
#define UM_EXTENSION_DONT_OPEN			(1 << 5)

/* local state */
static MemoryContext UmCxt = NULL;
static HTAB *UmCtxRegistry = NULL;

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
	UmfdVec	   *seg_fds[UMBRA_FORK_SLOTS];	/* array [0..num_open_segs) */
};

/* Forward declarations for internal ctx+rlocator core helpers. */
static UmfdVec *umfile_openfork(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
								ForkNumber forknum, int behavior);
static UmfdVec *umfile_openseg(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
							   ForkNumber forknum, BlockNumber segno, int oflags);
static UmfdVec *umfile_getseg(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
							  ForkNumber forknum, BlockNumber blkno,
							  bool skipFsync, int behavior,
							  bool isTempRelation);
static void umfile_register_dirty_seg(RelFileLocatorBackend rlocator,
									  bool isTempRelation,
									  ForkNumber forknum, UmfdVec *seg);
static bool umfile_fork_allows_sparse_segments(ForkNumber forknum);
static BlockNumber umfile_nblocks_sparse(UmbraFileContext *ctx,
										 RelFileLocatorBackend rlocator,
										 ForkNumber forknum);
static BlockNumber umfile_nblocks_dense(UmbraFileContext *ctx,
										RelFileLocatorBackend rlocator,
										ForkNumber forknum);
static BlockNumber umfile_nblocks_in_seg(File vfd);
static bool umfile_preallocate_fd(File fd, off_t target_bytes);
static bool umfile_preallocate_errno_is_unsupported(int err);
static bool umfile_collect_existing_segnos_by_path(const char *seg0path,
												   BlockNumber **segnos_out,
												   int *nsegnos_out);
static bool umfile_any_segment_exists_by_path(const char *seg0path);
static inline UmfdVec *umfile_v_get(UmbraFileContext *ctx, ForkNumber forknum,
									int segindex);
static bool umfile_fork_has_open_segment(UmbraFileContext *ctx, ForkNumber forknum);
static bool umfile_fork_has_open_segment_on_disk(UmbraFileContext *ctx,
												 RelFileLocatorBackend rlocator,
												 ForkNumber forknum);
static inline bool umfile_seg_entry_is_open(const UmfdVec *seg);
static inline void umfile_seg_entry_reset(UmfdVec *seg);
static void umfile_build_segpath(UmbraFileContext *ctx, ForkNumber forknum,
								 BlockNumber segno, char *path, size_t pathlen);
static void umfile_ctx_registry_init(void);
static UmbraFileContext *umfile_ctx_create(RelFileLocatorBackend rlocator);
static void umfile_ctx_destroy_internal(UmbraFileContext *ctx);

UmbraFileContext *
umfile_ctx_lookup(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;

	umfile_ctx_registry_init();
	entry = hash_search(UmCtxRegistry, &rlocator, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	return entry->ctx;
}

UmbraFileContext *
umfile_ctx_acquire(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;
	bool found;

	umfile_ctx_registry_init();
	entry = hash_search(UmCtxRegistry, &rlocator, HASH_ENTER, &found);
	if (!found)
		entry->ctx = umfile_ctx_create(rlocator);

	return entry->ctx;
}

UmbraFileContext *
umfile_ctx_create_temporary(RelFileLocatorBackend rlocator)
{
	umfile_ctx_registry_init();
	return umfile_ctx_create(rlocator);
}

void
umfile_ctx_forget(RelFileLocatorBackend rlocator)
{
	UmCtxRegistryEntry *entry;

	if (UmCtxRegistry == NULL)
		return;

	entry = hash_search(UmCtxRegistry, &rlocator, HASH_FIND, NULL);
	if (entry == NULL)
		return;

	umfile_ctx_destroy_internal(entry->ctx);
	(void) hash_search(UmCtxRegistry, &rlocator, HASH_REMOVE, NULL);
}

void
umfile_ctx_destroy_temporary(UmbraFileContext *ctx)
{
	umfile_ctx_destroy_internal(ctx);
}

/*
 * MAP-layer context helpers
 *
 * These operate directly on the ctx+rlocator core.
 *
 * Important: these helpers intentionally do not register fsync requests for
 * writes/extends. The MAP layer calls umfile_ctx_register_dirty() explicitly.
 */

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

static void
umfile_ctx_ensure_fork(UmbraFileContext *ctx, ForkNumber forknum)
{
	Assert(ctx != NULL);
	if (!umfile_exists(ctx, forknum,
					   umfile_fork_allows_sparse_segments(forknum) ?
					   UMFILE_EXISTS_SPARSE :
					   UMFILE_EXISTS_DENSE))
		umfile_create(ctx, forknum, false /* isRedo */ );
}

static void
umfile_ctx_ensure_block_exists(UmbraFileContext *ctx, ForkNumber forknum,
							   BlockNumber blkno)
{
	Assert(ctx != NULL);

	if (umfile_ctx_block_exists(ctx, forknum, blkno))
		return;

	/*
	 * Materialize just the requested block. For sparse mapped forks we do not
	 * need an authoritative current EOF here; FileZero() can create the target
	 * segment and make blkno BLCKSZ-addressable directly.
	 */
	Assert(blkno < MaxBlockNumber);
	umfile_zeroextend(ctx, forknum, blkno, 1, true /* skipFsync */ );
}

void
umfile_ctx_read(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				char *buffer, int nbytes)
{
	UmfdVec	   *v;
	off_t		seekpos;
	int			got;

	Assert(ctx != NULL);
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	v = umfile_getseg(ctx, ctx->rlocator, forknum, blkno,
					  false /* skipFsync */,
					  UM_EXTENSION_FAIL,
					  false /* isTempRelation */);
	seekpos = (off_t) BLCKSZ * (blkno % ((BlockNumber) RELSEG_SIZE));

	got = FileRead(v->umfd_vfd, buffer, nbytes, seekpos,
				   WAIT_EVENT_DATA_FILE_READ);
	if (got != nbytes)
	{
		if (got < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							FilePathName(v->umfd_vfd))));
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read file \"%s\": read only %d of %d bytes at block %u",
						FilePathName(v->umfd_vfd), got, nbytes, blkno)));
	}
}

void
umfile_ctx_write(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				 const char *buffer, int nbytes, bool skipFsync)
{
	UmfdVec	   *v;
	off_t		seekpos;
	int			wrote;

	Assert(ctx != NULL);
	Assert(buffer != NULL);
	Assert(nbytes > 0 && nbytes <= BLCKSZ);

	/*
	 * Ensure the target block exists at BLCKSZ granularity even if we're about
	 * to write only a sector-sized header.
	 */
	umfile_ctx_ensure_block_exists(ctx, forknum, blkno);

	v = umfile_getseg(ctx, ctx->rlocator, forknum, blkno,
					  true /* skipFsync */,
					  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE,
					  false /* isTempRelation */);
	seekpos = (off_t) BLCKSZ * (blkno % ((BlockNumber) RELSEG_SIZE));

	wrote = FileWrite(v->umfd_vfd, buffer, nbytes, seekpos,
					  WAIT_EVENT_DATA_FILE_WRITE);
	if (wrote != nbytes)
	{
		if (wrote < 0 && errno == ENOSPC)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							FilePathName(v->umfd_vfd)),
					 errhint("Check free disk space.")));
		if (wrote < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							FilePathName(v->umfd_vfd))));
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->umfd_vfd), wrote, nbytes, blkno)));
	}

	/*
	 * Intentionally do not register dirty here. The MAP layer does that via
	 * umfile_ctx_register_dirty() so it can control skipFsync consistently.
	 */
	(void) skipFsync;
}

void
umfile_ctx_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
				  const char *buffer)
{
	Assert(ctx != NULL);
	Assert(buffer != NULL);

	umfile_ctx_ensure_fork(ctx, forknum);

	/* Use the existing extension path but suppress fsync registration. */
	umfile_extend(ctx, forknum, blkno, buffer, true /* skipFsync */ );
}

bool
umfile_ctx_preallocate_blocks(UmbraFileContext *ctx, ForkNumber forknum,
							  UmFileNblocksMode mode,
							  BlockNumber target_nblocks, bool skipFsync)
{
	BlockNumber	nblocks;
	BlockNumber	cur_nblocks;
	bool		isTempRelation;

	if (ctx == NULL)
		return false;

	if (!umfile_exists(ctx, forknum,
					   mode == UMFILE_NBLOCKS_SPARSE ?
					   UMFILE_EXISTS_SPARSE :
					   UMFILE_EXISTS_DENSE))
		return false;

	isTempRelation = RelFileLocatorBackendIsTemp(ctx->rlocator);
	nblocks = umfile_nblocks(ctx, forknum, mode);
	if (target_nblocks <= nblocks)
		return true;

	if (target_nblocks > (uint64) MaxBlockNumber + 1)
		return false;

	/*
	 * Keep preallocation as a capacity operation: make the target segment
	 * BLCKSZ-addressable up to target_nblocks, but do not write page content.
	 * Later page writes may fill holes created by this step.
	 */
	cur_nblocks = nblocks;
	while (cur_nblocks < target_nblocks)
	{
		BlockNumber	target_blkno;
		BlockNumber	targetseg;
		BlockNumber	targetseg_nblocks;
		uint64		seg_start;
		uint64		seg_end;
		off_t		target_bytes;
		UmfdVec	   *v;

		targetseg = cur_nblocks / ((BlockNumber) RELSEG_SIZE);
		seg_start = (uint64) targetseg * (uint64) RELSEG_SIZE;
		seg_end = seg_start + (uint64) RELSEG_SIZE;
		if (seg_end > (uint64) target_nblocks)
			seg_end = (uint64) target_nblocks;

		targetseg_nblocks = (BlockNumber) (seg_end - seg_start);
		target_blkno = (BlockNumber) (seg_start + targetseg_nblocks - 1);
		target_bytes = (off_t) targetseg_nblocks * BLCKSZ;

		v = umfile_getseg(ctx, ctx->rlocator, forknum, target_blkno,
						  skipFsync,
						  UM_EXTENSION_CREATE,
						  isTempRelation);
		if (!umfile_preallocate_fd(v->umfd_vfd, target_bytes))
			return false;
		if (!skipFsync && !isTempRelation)
			umfile_register_dirty_seg(ctx->rlocator, false, forknum, v);

		cur_nblocks = (BlockNumber) seg_end;
	}

	return true;
}

void
umfile_ctx_prefetch(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno)
{
	if (ctx == NULL)
		return;
	(void) umfile_prefetch(ctx, forknum, blkno, 1);
}

bool
umfile_ctx_block_exists(UmbraFileContext *ctx, ForkNumber forknum,
						BlockNumber blkno)
{
	UmfdVec	   *v;
	BlockNumber	segno;
	BlockNumber	segblocks;

	if (ctx == NULL)
		return false;

	segno = blkno / ((BlockNumber) RELSEG_SIZE);

	if (segno < (BlockNumber) ctx->num_open_segs[forknum])
	{
		v = umfile_v_get(ctx, forknum, (int) segno);
		if (!umfile_seg_entry_is_open(v))
			return false;
	}
	else
	{
		/*
		 * Dense forks can only materialize the next segment in order. Sparse
		 * forks may legitimately skip lower segments.
		 */
		if (!umfile_fork_allows_sparse_segments(forknum) &&
			segno > (BlockNumber) ctx->num_open_segs[forknum])
			return false;

		v = umfile_openseg(ctx, ctx->rlocator, forknum, segno,
						   UM_EXTENSION_RETURN_NULL);
		if (v == NULL)
			return false;
	}

	segblocks = umfile_nblocks_in_seg(v->umfd_vfd);
	return (blkno % ((BlockNumber) RELSEG_SIZE)) < segblocks;
}

bool
umfile_ctx_segment_exists(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber segno)
{
	char		path[MAXPGPATH];

	if (ctx == NULL)
		return false;

	umfile_build_segpath(ctx, forknum, segno, path, sizeof(path));
	return access(path, F_OK) == 0;
}

void
umfile_ctx_register_dirty(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blkno, bool skipFsync,
						  bool isTempRelation)
{
	UmfdVec	   *v;

	if (skipFsync || isTempRelation)
		return;

	Assert(ctx != NULL);

	/*
	 * Ensure we can fall back to immediate fsync if the sync request queue is
	 * full, mirroring md.c behavior.
	 */
	v = umfile_getseg(ctx, ctx->rlocator, forknum, blkno,
					  false /* skipFsync */,
					  UM_EXTENSION_FAIL,
					  isTempRelation);
	umfile_register_dirty_seg(ctx->rlocator, isTempRelation, forknum, v);
}

void
umfile_ctx_unlinkfork(RelFileLocatorBackend rlocator, ForkNumber forkNum,
					  bool isRedo)
{
	umfile_unlink(rlocator, forkNum, isRedo);
}

/*
 * Build a FileTag for Umbra relation segment files.  MAP fork uses Umbra-only
 * naming and cannot safely reuse md's unlink callback.
 */
#define INIT_UM_FILETAG(tag, rlocator_, forknum_, segno_)	\
	do {												\
		memset(&(tag), 0, sizeof(FileTag));				\
		(tag).handler = SYNC_HANDLER_UMBRA;			\
		(tag).rlocator = (rlocator_);					\
		(tag).forknum = (forknum_);						\
		(tag).segno = (segno_);							\
	} while (0)

static inline int
_umfd_open_flags(void)
{
	int			flags = O_RDWR | PG_BINARY;

	if (io_direct_flags & IO_DIRECT_DATA)
		flags |= PG_O_DIRECT;

	return flags;
}

static void
umfile_fdvec_resize(UmbraFileContext *ctx, ForkNumber forknum, int nseg)
{
	Assert(nseg >= 0);

	if (nseg == 0)
	{
		if (ctx->num_open_segs[forknum] > 0)
		{
			pfree(ctx->seg_fds[forknum]);
			ctx->seg_fds[forknum] = NULL;
		}
		ctx->seg_fds[forknum] = NULL;
		ctx->num_open_segs[forknum] = 0;
		return;
	}

	if (ctx->num_open_segs[forknum] == 0)
	{
		ctx->seg_fds[forknum] =
			MemoryContextAlloc(UmCxt, sizeof(UmfdVec) * nseg);
	}
	else if (nseg > ctx->num_open_segs[forknum])
	{
		ctx->seg_fds[forknum] =
			repalloc(ctx->seg_fds[forknum],
					 sizeof(UmfdVec) * nseg);
	}
	else
	{
		/*
		 * Don't reallocate a smaller array: keep truncate usable in critical
		 * sections (mirrors md.c behavior).
		 */
	}

	ctx->num_open_segs[forknum] = nseg;
}

static inline UmfdVec *
umfile_v_get(UmbraFileContext *ctx, ForkNumber forknum, int segindex)
{
	Assert(segindex >= 0);
	Assert(segindex < ctx->num_open_segs[forknum]);
	return &ctx->seg_fds[forknum][segindex];
}

static BlockNumber
umfile_nblocks_in_seg(File vfd)
{
	off_t		len;

	len = FileSize(vfd);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(vfd))));

	return (BlockNumber) (len / BLCKSZ);
}

static RelPathStr
umfile_segpath(RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber segno)
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
	UmfdVec    *v;
	RelPathStr	fullpath;
	File		fd;
	int			old_nseg;
	int			i;

	fullpath = umfile_segpath(rlocator, forknum, segno);

	fd = PathNameOpenFile(fullpath.str, _umfd_open_flags() | oflags);

	if (fd < 0)
		return NULL;

	old_nseg = ctx->num_open_segs[forknum];
	if (umfile_fork_allows_sparse_segments(forknum))
	{
		if (segno >= (BlockNumber) old_nseg)
		{
			umfile_fdvec_resize(ctx, forknum, segno + 1);
			for (i = old_nseg; i < ctx->num_open_segs[forknum]; i++)
				umfile_seg_entry_reset(umfile_v_get(ctx, forknum, i));
		}
		v = umfile_v_get(ctx, forknum, (int) segno);
		Assert(!umfile_seg_entry_is_open(v));
	}
	else
	{
		/*
		 * Segments are opened in increasing order, so we must be adding a new
		 * one at the end.
		 */
		Assert(segno == (BlockNumber) old_nseg);
		umfile_fdvec_resize(ctx, forknum, segno + 1);
		v = umfile_v_get(ctx, forknum, (int) segno);
	}

	v->umfd_vfd = fd;
	v->umfd_segno = segno;
	Assert(umfile_nblocks_in_seg(v->umfd_vfd) <= (BlockNumber) RELSEG_SIZE);
	return v;
}

static UmfdVec *
umfile_openfork(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				ForkNumber forknum, int behavior)
{
	RelPathStr	path;
	File		fd;
	UmfdVec	   *v;

	/* No work if already open */
	if (ctx->num_open_segs[forknum] > 0)
		return umfile_v_get(ctx, forknum, 0);

	if (forknum == UMBRA_METADATA_FORKNUM)
		path = UmMetadataRelPathBackend(rlocator);
	else
		path = relpath(rlocator, forknum);
	fd = PathNameOpenFile(path.str, _umfd_open_flags());

	if (fd < 0)
	{
		if ((behavior & UM_EXTENSION_RETURN_NULL) &&
			FILE_POSSIBLY_DELETED(errno))
			return NULL;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path.str)));
	}

	umfile_fdvec_resize(ctx, forknum, 1);
	v = umfile_v_get(ctx, forknum, 0);
	v->umfd_vfd = fd;
	v->umfd_segno = 0;

	Assert(umfile_nblocks_in_seg(v->umfd_vfd) <= (BlockNumber) RELSEG_SIZE);

	return v;
}

static bool
umfile_fork_allows_sparse_segments(ForkNumber forknum)
{
	switch (forknum)
	{
		case MAIN_FORKNUM:
		case FSM_FORKNUM:
		case VISIBILITYMAP_FORKNUM:
			return true;
		default:
			return false;
	}
}

static bool umfile_collect_existing_segnos_by_path(const char *seg0path,
												   BlockNumber **segnos_out,
												   int *nsegnos_out);

static bool
umfile_sparse_fork_scan_segments(UmbraFileContext *ctx,
								 ForkNumber forknum,
								 BlockNumber *minsegno,
								 BlockNumber *maxsegno)
{
	char		seg0path[MAXPGPATH];
	BlockNumber *segnos = NULL;
	int			nsegnos = 0;

	Assert(umfile_fork_allows_sparse_segments(forknum));

	umfile_build_segpath(ctx, forknum, 0, seg0path, sizeof(seg0path));
	if (!umfile_collect_existing_segnos_by_path(seg0path, &segnos, &nsegnos))
		return false;
	if (nsegnos == 0)
		return false;

	if (minsegno != NULL)
		*minsegno = segnos[0];
	if (maxsegno != NULL)
		*maxsegno = segnos[nsegnos - 1];
	pfree(segnos);
	return true;
}

static bool
umfile_any_segment_exists_by_path(const char *seg0path)
{
	char		dirpath[MAXPGPATH];
	char	   *slash;
	const char *basename;
	size_t		baselen;
	DIR		   *dir;
	struct dirent *de;

	Assert(seg0path != NULL);

	strlcpy(dirpath, seg0path, sizeof(dirpath));
	slash = strrchr(dirpath, '/');
	if (slash == NULL)
		return false;

	*slash = '\0';
	basename = slash + 1;
	baselen = strlen(basename);

	dir = AllocateDir(dirpath);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return false;
		return false;
	}

	while ((de = ReadDir(dir, dirpath)) != NULL)
	{
		const char *name = de->d_name;

		if (strcmp(name, basename) == 0)
		{
			FreeDir(dir);
			return true;
		}

		if (strncmp(name, basename, baselen) == 0 &&
			name[baselen] == '.')
		{
			char	   *endptr = NULL;
			unsigned long parsed;

			errno = 0;
			parsed = strtoul(name + baselen + 1, &endptr, 10);
			if (errno == 0 &&
				endptr != name + baselen + 1 &&
				*endptr == '\0' &&
				parsed <= MaxBlockNumber)
			{
				FreeDir(dir);
				return true;
			}
		}
	}

	FreeDir(dir);
	return false;
}

static inline bool
umfile_seg_entry_is_open(const UmfdVec *seg)
{
	return (seg != NULL && seg->umfd_vfd >= 0);
}

static bool
umfile_fork_has_open_segment(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			i;

	for (i = 0; i < ctx->num_open_segs[forknum]; i++)
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
	int			i;
	bool		have_live = false;

	for (i = 0; i < ctx->num_open_segs[forknum]; i++)
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

static inline void
umfile_seg_entry_reset(UmfdVec *seg)
{
	seg->umfd_vfd = -1;
	seg->umfd_segno = InvalidBlockNumber;
}

static int
umfile_compare_blocknumbers(const void *a, const void *b)
{
	BlockNumber	va = *(const BlockNumber *) a;
	BlockNumber	vb = *(const BlockNumber *) b;

	if (va < vb)
		return -1;
	if (va > vb)
		return 1;
	return 0;
}

static bool
umfile_collect_existing_segnos_by_path(const char *seg0path,
									   BlockNumber **segnos_out,
									   int *nsegnos_out)
{
	char		dirpath[MAXPGPATH];
	char	   *slash;
	const char *basename;
	size_t		baselen;
	DIR		   *dir;
	struct dirent *de;
	BlockNumber *segnos = NULL;
	int			nsegnos = 0;
	int			capacity = 0;
	int			i;
	int			uniq;

	Assert(seg0path != NULL);
	Assert(segnos_out != NULL);
	Assert(nsegnos_out != NULL);

	*segnos_out = NULL;
	*nsegnos_out = 0;

	strlcpy(dirpath, seg0path, sizeof(dirpath));
	slash = strrchr(dirpath, '/');
	if (slash == NULL)
		return false;

	*slash = '\0';
	basename = slash + 1;
	baselen = strlen(basename);

	dir = AllocateDir(dirpath);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return true;
		return false;
	}

	while ((de = ReadDir(dir, dirpath)) != NULL)
	{
		const char *name = de->d_name;
		BlockNumber	segno;

		if (strcmp(name, basename) == 0)
			segno = 0;
		else if (strncmp(name, basename, baselen) == 0 &&
				 name[baselen] == '.')
		{
			char	   *endptr = NULL;
			unsigned long parsed;

			errno = 0;
			parsed = strtoul(name + baselen + 1, &endptr, 10);
			if (errno != 0 ||
				endptr == name + baselen + 1 ||
				*endptr != '\0' ||
				parsed > MaxBlockNumber)
				continue;
			segno = (BlockNumber) parsed;
		}
		else
			continue;

		if (nsegnos == capacity)
		{
			int new_capacity = (capacity == 0) ? 16 : capacity * 2;

			if (segnos == NULL)
				segnos = (BlockNumber *) MemoryContextAlloc(UmCxt,
															sizeof(BlockNumber) * new_capacity);
			else
				segnos = (BlockNumber *) repalloc(segnos,
												  sizeof(BlockNumber) * new_capacity);
			capacity = new_capacity;
		}
		segnos[nsegnos++] = segno;
	}

	FreeDir(dir);

	if (nsegnos == 0)
	{
		if (segnos != NULL)
			pfree(segnos);
		return true;
	}

	qsort(segnos, nsegnos, sizeof(BlockNumber), umfile_compare_blocknumbers);

	uniq = 1;
	for (i = 1; i < nsegnos; i++)
	{
		if (segnos[i] != segnos[uniq - 1])
			segnos[uniq++] = segnos[i];
	}

	*segnos_out = segnos;
	*nsegnos_out = uniq;
	return true;
}

/*
 * umfile_build_segpath() -- Build segment path in caller-provided buffer.
 *
 * This is a no-allocation path builder so callers can use it safely in
 * critical sections.
 */
static void
umfile_build_segpath(UmbraFileContext *ctx, ForkNumber forknum,
					 BlockNumber segno, char *path, size_t pathlen)
{
	int			n;
	RelFileLocatorBackend rlocator;

	Assert(forknum >= 0 && forknum <= UMBRA_METADATA_FORKNUM);

	/* Build RelFileLocatorBackend for use with relpath */
	rlocator.locator = ctx->rlocator.locator;
	rlocator.backend = ctx->rlocator.backend;

	/* Build the base path using public forks or Umbra private metadata. */
	{
		RelPathStr relpath_str;

		if (forknum == UMBRA_METADATA_FORKNUM)
			relpath_str = UmMetadataRelPathBackend(rlocator);
		else
			relpath_str = relpath(rlocator, forknum);
		n = strlcpy(path, relpath_str.str, pathlen);
	}

	if (segno == 0)
		return;

	Assert(segno < RELSEG_SIZE);
	snprintf(path + n, pathlen - n, ".%u", segno);
}

static UmfdVec *
umfile_getseg(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			  ForkNumber forknum, BlockNumber blkno,
			  bool skipFsync, int behavior, bool isTempRelation)
{
	UmfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	Assert(behavior &
		   (UM_EXTENSION_FAIL | UM_EXTENSION_CREATE | UM_EXTENSION_RETURN_NULL |
			UM_EXTENSION_DONT_OPEN));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < (BlockNumber) ctx->num_open_segs[forknum])
	{
		v = umfile_v_get(ctx, forknum, (int) targetseg);
		if (!umfile_fork_allows_sparse_segments(forknum) ||
			umfile_seg_entry_is_open(v))
			return v;
	}

	/* The caller only wants the segment if we already had it open. */
	if (behavior & UM_EXTENSION_DONT_OPEN)
		return NULL;

	/*
	 * Mapped data forks can use sparse physical segment numbering. Open/create
	 * the target segment directly without checking continuity of previous
	 * segments.
	 */
	if (umfile_fork_allows_sparse_segments(forknum))
	{
		int flags = 0;

		if ((behavior & UM_EXTENSION_CREATE) ||
			(InRecovery && (behavior & UM_EXTENSION_CREATE_RECOVERY)))
			flags = O_CREAT;

		v = umfile_openseg(ctx, rlocator, forknum, targetseg, flags);
		if (v == NULL)
		{
			if ((behavior & UM_EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							umfile_segpath(rlocator, forknum, targetseg).str,
							blkno)));
		}
		return v;
	}

	/*
	 * The target segment is not yet open. Iterate over all the segments between
	 * the last opened and the target segment.
	 */
	if (ctx->num_open_segs[forknum] > 0)
		v = umfile_v_get(ctx, forknum, ctx->num_open_segs[forknum] - 1);
	else
	{
		v = umfile_openfork(ctx, rlocator, forknum, behavior);
		if (!v)
			return NULL;
	}

	for (nextsegno = ctx->num_open_segs[forknum];
		 nextsegno <= targetseg;
		 nextsegno++)
	{
		BlockNumber	nblocks = umfile_nblocks_in_seg(v->umfd_vfd);
		int			flags = 0;

		Assert(nextsegno == v->umfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & UM_EXTENSION_CREATE) ||
			(InRecovery && (behavior & UM_EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Maintain the invariant that segments before the last active
			 * segment are exactly RELSEG_SIZE blocks. Pad with zeros if needed.
			 * This can happen e.g. in recovery or for discontiguous extension.
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

		v = umfile_openseg(ctx, rlocator, forknum, nextsegno, flags);
		if (v == NULL)
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

	return v;
}

static void
umfile_register_dirty_seg(RelFileLocatorBackend rlocator, bool isTempRelation,
						  ForkNumber forknum, UmfdVec *seg)
{
	FileTag		tag;

	if (!RelFileNumberIsValid(rlocator.locator.relNumber) ||
		!OidIsValid(rlocator.locator.spcOid))
		elog(PANIC,
			 "invalid Umbra relation locator in fsync registration %u/%u/%u fork=%d seg=%u",
			 rlocator.locator.spcOid,
			 rlocator.locator.dbOid,
			 rlocator.locator.relNumber,
			 forknum,
			 seg->umfd_segno);

	INIT_UM_FILETAG(tag, rlocator.locator, forknum, seg->umfd_segno);

	/* Temp relations should never be fsync'd */
	Assert(!isTempRelation);

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

		pgstat_count_io_op_time(IOOBJECT_RELATION, IOCONTEXT_NORMAL,
								IOOP_FSYNC, io_start, 1, 0);
	}
}

static void
umfile_register_unlink_seg(RelFileLocatorBackend rlocator, ForkNumber forknum,
						   BlockNumber segno)
{
	FileTag		tag;

	INIT_UM_FILETAG(tag, rlocator.locator, forknum, segno);
	Assert(!RelFileLocatorBackendIsTemp(rlocator));

	RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

static void
umfile_register_forget_seg(RelFileLocatorBackend rlocator, ForkNumber forknum,
						   BlockNumber segno)
{
	FileTag		tag;

	INIT_UM_FILETAG(tag, rlocator.locator, forknum, segno);
	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

static void
umfile_register_dense_existing_segs_for_unlink(RelFileLocatorBackend rlocator,
											   ForkNumber forknum,
											   const char *seg0path)
{
	char		segpath[MAXPGPATH];
	BlockNumber segno;

	Assert(seg0path != NULL);

	for (segno = 0;; segno++)
	{
		if (segno == 0)
			strlcpy(segpath, seg0path, sizeof(segpath));
		else
			snprintf(segpath, sizeof(segpath), "%s.%u", seg0path, segno);

		if (!pg_file_exists(segpath))
			break;

		umfile_register_unlink_seg(rlocator, forknum, segno);
	}
}

void
umfile_init(void)
{
	HASHCTL info;

	if (UmCxt != NULL)
		return;
	UmCxt = AllocSetContextCreate(TopMemoryContext,
								  "UmFile",
								  ALLOCSET_DEFAULT_SIZES);
	/*
	 * smgr callbacks (including truncate during WAL replay) can run inside a
	 * critical section. Umbra's per-relation file context is used by openfork
	 * and fdvec management, so it must be permitted to allocate there.
	 *
	 * This matches the expectation in core smgr implementations that their
	 * internal contexts can allocate while in a critical section.
	 */
	MemoryContextAllowInCriticalSection(UmCxt, true);

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(RelFileLocatorBackend);
	info.entrysize = sizeof(UmCtxRegistryEntry);
	info.hcxt = UmCxt;
	UmCtxRegistry = hash_create("Umbra file context registry",
								256,
								&info,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
umfile_ctx_registry_init(void)
{
	if (UmCxt == NULL)
		umfile_init();

	Assert(UmCtxRegistry != NULL);
}

static UmbraFileContext *
umfile_ctx_create(RelFileLocatorBackend rlocator)
{
	UmbraFileContext *ctx;

	ctx = MemoryContextAllocZero(UmCxt, sizeof(UmbraFileContext));
	ctx->rlocator = rlocator;

	for (int forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
	{
		ctx->num_open_segs[forknum] = 0;
		ctx->seg_fds[forknum] = NULL;
	}

	return ctx;
}

static void
umfile_ctx_destroy_internal(UmbraFileContext *ctx)
{
	if (ctx == NULL)
		return;

	for (int forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
	{
		while (ctx->num_open_segs[forknum] > 0)
		{
			UmfdVec *seg = umfile_v_get(ctx, forknum,
										ctx->num_open_segs[forknum] - 1);

			if (umfile_seg_entry_is_open(seg))
				FileClose(seg->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, ctx->num_open_segs[forknum] - 1);
		}
	}

	pfree(ctx);
}

void
umfile_create(UmbraFileContext *ctx, ForkNumber forknum, bool isRedo)
{
	RelFileLocatorBackend rlocator;
	bool		isTempRelation;
	RelPathStr	path;
	File		fd;
	UmfdVec	   *v;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;
	isTempRelation = RelFileLocatorBackendIsTemp(rlocator);

	if (isRedo && ctx->num_open_segs[forknum] > 0)
		return;

	if (ctx->num_open_segs[forknum] > 0)
		while (ctx->num_open_segs[forknum] > 0)
		{
			UmfdVec *seg = umfile_v_get(ctx, forknum,
										ctx->num_open_segs[forknum] - 1);

			if (umfile_seg_entry_is_open(seg))
				FileClose(seg->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, ctx->num_open_segs[forknum] - 1);
		}

	Assert(ctx->num_open_segs[forknum] == 0);

	TablespaceCreateDbspace(rlocator.locator.spcOid,
							rlocator.locator.dbOid,
							isRedo);

	if (forknum == UMBRA_METADATA_FORKNUM)
		path = UmMetadataRelPathBackend(rlocator);
	else
		path = relpath(rlocator, forknum);

	fd = PathNameOpenFile(path.str, _umfd_open_flags() | O_CREAT | O_EXCL);
	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path.str, _umfd_open_flags());
		if (fd < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path.str)));
		}
	}

	umfile_fdvec_resize(ctx, forknum, 1);
	v = umfile_v_get(ctx, forknum, 0);
	v->umfd_vfd = fd;
	v->umfd_segno = 0;

	if (!isTempRelation)
		umfile_register_dirty_seg(rlocator, false, forknum, v);
}

void
umfile_ctx_close_fork(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			nopensegs;

	if (ctx == NULL)
		return;

	nopensegs = ctx->num_open_segs[forknum];
	if (nopensegs == 0)
		return;

	while (nopensegs > 0)
	{
		UmfdVec    *v = umfile_v_get(ctx, forknum, nopensegs - 1);

		if (umfile_seg_entry_is_open(v))
			FileClose(v->umfd_vfd);
		umfile_fdvec_resize(ctx, forknum, nopensegs - 1);
		nopensegs--;
	}
}

bool
umfile_exists(UmbraFileContext *ctx, ForkNumber forknum, UmFileExistsMode mode)
{
	RelFileLocatorBackend rlocator;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;

	if (!InRecovery &&
		umfile_fork_has_open_segment(ctx, forknum))
	{
		/*
		 * Any still-open segment whose path still exists is enough evidence
		 * that the fork exists. If all open fds are stale after an unlink or
		 * rewrite, drop them before falling back to slower on-disk probes.
		 */
		if (umfile_fork_has_open_segment_on_disk(ctx, rlocator, forknum))
			return true;

		while (ctx->num_open_segs[forknum] > 0)
		{
			UmfdVec *v = umfile_v_get(ctx, forknum, ctx->num_open_segs[forknum] - 1);

			if (umfile_seg_entry_is_open(v))
				FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, ctx->num_open_segs[forknum] - 1);
		}
	}

	if (mode == UMFILE_EXISTS_SPARSE)
	{
		/*
		 * Most sparse forks still keep seg0 around. Probe that first so the
		 * common case stays as cheap as the legacy exists() path. Only fall
		 * back to a directory scan when seg0 is absent and the fork may still
		 * exist solely via higher sparse segments.
		 */
		if (umfile_openfork(ctx, rlocator, forknum, UM_EXTENSION_RETURN_NULL) != NULL)
			return true;

		return umfile_any_segment_exists_by_path(
			umfile_segpath(rlocator, forknum, 0).str);
	}

	return (umfile_openfork(ctx, rlocator, forknum, UM_EXTENSION_RETURN_NULL) != NULL);
}

/*
 * umfile_open_or_create() -- open existing fork or create new one.
 *
 * For redo, attempt to reuse existing file. For normal create, always create
 * with O_EXCL to avoid binding to stale on-disk contents. Returns true on
 * success, and sets *created to indicate whether a new file was created.
 */
bool
umfile_open_or_create(UmbraFileContext *ctx, ForkNumber forknum,
					  bool isRedo, bool *created)
{
	UmfdVec    *v;

	if (created)
		*created = false;

	/*
	 * Redo can legitimately see pre-existing files and should reuse them.
	 */
	if (isRedo)
	{
		v = umfile_openfork(ctx, ctx->rlocator, forknum,
							UM_EXTENSION_RETURN_NULL);
		if (v != NULL)
			return true;
	}

	/* Create new file (with O_EXCL for normal path) */
	umfile_create(ctx, forknum, isRedo);

	/* Verify creation succeeded */
	v = umfile_openfork(ctx, ctx->rlocator, forknum,
						UM_EXTENSION_RETURN_NULL);
	if (v != NULL)
	{
		if (created)
			*created = true;
		return true;
	}

	return false;
}

/*
 * Unlink logic mirrors mdunlink(), but uses Umbra segment tracking.
 */
void
umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	RelPathStr	path;
	int			ret;
	int			save_errno;

	if (forknum == InvalidForkNumber)
	{
		for (forknum = 0; forknum <= UMBRA_METADATA_FORKNUM; forknum++)
			umfile_unlink(rlocator, forknum, isRedo);
		return;
	}

	if (forknum == UMBRA_METADATA_FORKNUM)
		path = UmMetadataRelPathBackend(rlocator);
	else
		path = relpath(rlocator, forknum);

	/*
	 * Keep all MAP segments physically intact until checkpoint-time unlink, so
	 * remap-related lookup state is preserved throughout the checkpoint window.
	 */
	if (!isRedo &&
		!RelFileLocatorBackendIsTemp(rlocator) &&
		forknum == UMBRA_METADATA_FORKNUM)
	{
		umfile_register_dense_existing_segs_for_unlink(rlocator, forknum,
														   path.str);
		return;
	}

	if (isRedo || IsBinaryUpgrade || forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(rlocator))
	{
		if (!RelFileLocatorBackendIsTemp(rlocator))
		{
			ret = pg_truncate(path.str, 0);
			if (ret < 0 && errno != ENOENT)
			{
				save_errno = errno;
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m", path.str)));
				errno = save_errno;
			}

			save_errno = errno;
			umfile_register_forget_seg(rlocator, forknum, 0 /* first seg */ );
			errno = save_errno;
		}
		else
			ret = 0;

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
		ret = pg_truncate(path.str, 0);
		if (ret < 0 && errno != ENOENT)
		{
			save_errno = errno;
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not truncate file \"%s\": %m", path.str)));
			errno = save_errno;
		}

		save_errno = errno;
		umfile_register_unlink_seg(rlocator, forknum, 0 /* first seg */ );
		errno = save_errno;
	}

	/* Remove additional segments. */
	if (ret >= 0 || errno != ENOENT)
	{
		char		segpath[MAXPGPATH];
		BlockNumber segno;

		for (segno = 1;; segno++)
		{
			snprintf(segpath, sizeof(segpath), "%s.%u", path.str, segno);

			if (!RelFileLocatorBackendIsTemp(rlocator))
			{
				ret = pg_truncate(segpath, 0);
				save_errno = errno;
				umfile_register_forget_seg(rlocator, forknum, segno);
				errno = save_errno;
			}
			else
				ret = 0;

			if (ret < 0 && errno != ENOENT)
				break;

			ret = unlink(segpath);
			if (ret < 0)
			{
				if (errno != ENOENT)
				{
					save_errno = errno;
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath)));
					errno = save_errno;
				}
				break;
			}
		}
	}
}

void
umfile_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			  const void *buffer, bool skipFsync)
{
	RelFileLocatorBackend rlocator;
	bool		isTempRelation;
	UmfdVec    *v;
	off_t		seekpos;
	int			nbytes;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;
	isTempRelation = RelFileLocatorBackendIsTemp(rlocator);

	v = umfile_getseg(ctx, rlocator, forknum, blocknum, skipFsync,
					  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE,
					  isTempRelation);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 rlocator.locator.spcOid,
										 rlocator.locator.dbOid,
										 rlocator.locator.relNumber,
										 rlocator.backend);

	nbytes = FileWrite(v->umfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_EXTEND);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										rlocator.locator.spcOid,
										rlocator.locator.dbOid,
										rlocator.locator.relNumber,
										rlocator.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0 && errno == ENOSPC)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->umfd_vfd)),
					 errhint("Check free disk space.")));
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
							FilePathName(v->umfd_vfd), nbytes, BLCKSZ, blocknum)));
	}

	if (!skipFsync && !isTempRelation)
		umfile_register_dirty_seg(rlocator, false, forknum, v);
}

/*
 * Reserve bytes for a relation segment without writing page contents.
 *
 * Return true only if the whole range up to target_bytes is backed by a real
 * preallocation primitive. Unsupported filesystems return false so callers do
 * not publish capacity that is only a logical EOF extension.
 *
 * Linux uses the fallocate syscall directly so we don't inherit glibc's
 * posix_fallocate()->userspace zero-fill fallback. macOS uses F_PREALLOCATE,
 * and other platforms may use posix_fallocate().
 */
static bool
umfile_preallocate_fd(File fd, off_t target_bytes)
{
	off_t		current_bytes;
	off_t		delta_bytes;
	int			rawfd;

	current_bytes = FileSize(fd);
	if (current_bytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not determine file size for \"%s\": %m",
						FilePathName(fd))));

	if (current_bytes >= target_bytes)
		return true;

	delta_bytes = target_bytes - current_bytes;
	rawfd = FileGetRawDesc(fd);

#if defined(__linux__)
#ifdef SYS_fallocate
	{
		long		rc;

retry_fallocate:
		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
		rc = syscall(SYS_fallocate, rawfd, 0, current_bytes, delta_bytes);
		pgstat_report_wait_end();

		if (rc < 0)
		{
			if (errno == EINTR)
				goto retry_fallocate;
			if (umfile_preallocate_errno_is_unsupported(errno))
				return false;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not preallocate file \"%s\" to %llu bytes: %m",
							FilePathName(fd),
							(unsigned long long) target_bytes)));
		}
	}
#else
	return false;
#endif
#elif defined(__APPLE__) && defined(F_PREALLOCATE)
	{
		fstore_t	fst;
		int			rc;

		/*
		 * F_PEOFPOSMODE interprets fst_length as newly allocated bytes beyond
		 * current EOF, not the final file size. Passing target_bytes here would
		 * over-reserve on repeated top-ups of the same segment.
		 */
		MemSet(&fst, 0, sizeof(fst));
		fst.fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL;
		fst.fst_posmode = F_PEOFPOSMODE;
		fst.fst_offset = 0;
		fst.fst_length = delta_bytes;

retry_f_preallocate_contig:
		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
		rc = fcntl(rawfd, F_PREALLOCATE, &fst);
		pgstat_report_wait_end();
		if (rc < 0 && errno == EINTR)
			goto retry_f_preallocate_contig;

		if (rc < 0)
		{
			fst.fst_flags = F_ALLOCATEALL;
			fst.fst_bytesalloc = 0;

retry_f_preallocate_all:
			errno = 0;
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
			rc = fcntl(rawfd, F_PREALLOCATE, &fst);
			pgstat_report_wait_end();
			if (rc < 0 && errno == EINTR)
				goto retry_f_preallocate_all;

			if (rc < 0)
			{
				if (umfile_preallocate_errno_is_unsupported(errno))
					return false;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not preallocate file \"%s\" to %llu bytes: %m",
								FilePathName(fd),
								(unsigned long long) target_bytes)));
			}
		}

		if (fst.fst_bytesalloc < delta_bytes)
			return false;
	}
#elif defined(HAVE_POSIX_FALLOCATE)
	{
		int			rc;

retry_posix_fallocate:
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
		rc = posix_fallocate(rawfd, current_bytes, delta_bytes);
		pgstat_report_wait_end();

		if (rc == EINTR)
			goto retry_posix_fallocate;

		if (rc != 0)
		{
			errno = rc;
			if (umfile_preallocate_errno_is_unsupported(errno))
				return false;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not preallocate file \"%s\" to %llu bytes: %m",
							FilePathName(fd),
							(unsigned long long) target_bytes)));
		}
	}
#else
	return false;
#endif

	current_bytes = FileSize(fd);
	if (current_bytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not determine file size for \"%s\": %m",
						FilePathName(fd))));
	if (current_bytes < target_bytes &&
		FileTruncate(fd, target_bytes, WAIT_EVENT_DATA_FILE_EXTEND) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not extend preallocated file \"%s\" to %llu bytes: %m",
						FilePathName(fd),
						(unsigned long long) target_bytes)));

	return true;
}

static bool
umfile_preallocate_errno_is_unsupported(int err)
{
	if (err == EINVAL || err == EOPNOTSUPP)
		return true;
#ifdef ENOSYS
	if (err == ENOSYS)
		return true;
#endif
#ifdef ENOTSUP
	if (err == ENOTSUP)
		return true;
#endif
	return false;
}

void
umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
				  int nblocks, bool skipFsync)
{
	RelFileLocatorBackend rlocator;
	bool		isTempRelation;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;
	isTempRelation = RelFileLocatorBackendIsTemp(rlocator);

	while (nblocks > 0)
	{
		int			numblocks;
		off_t		seekpos;
		UmfdVec    *v;
		int			ret;
		int			remblocks;
		BlockNumber curblocknum;

		curblocknum = blocknum;
		remblocks = nblocks;

		numblocks = Min(remblocks, RELSEG_SIZE - (curblocknum % RELSEG_SIZE));
		numblocks = Min(numblocks, PG_IOV_MAX);

			v = umfile_getseg(ctx, rlocator, forknum, curblocknum, skipFsync,
							  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE,
							  isTempRelation);

		seekpos = (off_t) BLCKSZ * (curblocknum % ((BlockNumber) RELSEG_SIZE));
		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

			ret = FileZero(v->umfd_vfd,
						   seekpos,
						   (off_t) BLCKSZ * numblocks,
						   WAIT_EVENT_DATA_FILE_EXTEND);
			if (ret < 0)
			{
				int save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not extend file \"%s\": %m",
								FilePathName(v->umfd_vfd)),
						 errhint(save_errno == ENOSPC ? "Check free disk space." : NULL)));
			}

		if (!skipFsync && !isTempRelation)
			umfile_register_dirty_seg(rlocator, false, forknum, v);

		nblocks -= numblocks;
		blocknum += numblocks;
	}
}

bool
umfile_prefetch(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
				int nblocks)
{
	RelFileLocatorBackend rlocator;
	bool		isTempRelation;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;
	isTempRelation = RelFileLocatorBackendIsTemp(rlocator);

#ifdef USE_PREFETCH
	Assert((io_direct_flags & IO_DIRECT_DATA) == 0);

	if ((uint64) blocknum + nblocks > (uint64) MaxBlockNumber + 1)
		return false;

	while (nblocks > 0)
	{
		off_t		seekpos;
		UmfdVec    *v;
		int			nblocks_this_segment;

			v = umfile_getseg(ctx, rlocator, forknum, blocknum,
							  false /* skipFsync */,
							  InRecovery ? UM_EXTENSION_RETURN_NULL : UM_EXTENSION_FAIL,
							  isTempRelation);
		if (v == NULL)
			return false;

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks, RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));

		(void) FilePrefetch(v->umfd_vfd, seekpos, BLCKSZ * nblocks_this_segment,
							WAIT_EVENT_DATA_FILE_PREFETCH);

		blocknum += nblocks_this_segment;
		nblocks -= nblocks_this_segment;
	}
#endif
	return true;
}

uint32
umfile_maxcombine(ForkNumber forknum, BlockNumber blocknum)
{
	uint32		maxblocks;

	(void) forknum;
	maxblocks = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));
	return maxblocks;
}

static int
umfile_buffers_to_iovec(struct iovec *iov, void **buffers, int nblocks)
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

void
umfile_readv(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber nblocks)
{
	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		int			iovcnt;
		off_t		seekpos;
		int			nbytes;
		UmfdVec    *v;
		BlockNumber nblocks_this_segment;
		size_t		transferred_this_segment;
		size_t		size_this_segment;

		v = umfile_getseg(ctx, ctx->rlocator,
						  forknum, blocknum, false /* skipFsync */,
						  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY,
						  RelFileLocatorBackendIsTemp(ctx->rlocator));

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks,
				RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
		nblocks_this_segment = Min(nblocks_this_segment, (BlockNumber) lengthof(iov));

		if (nblocks_this_segment != nblocks)
			elog(ERROR, "read crosses segment boundary");

		iovcnt = umfile_buffers_to_iovec(iov, buffers, (int) nblocks_this_segment);
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
				 * Mirror mdreadv() behavior: in production builds we can
				 * zero-fill if zero_damaged_pages or in recovery, but this
				 * codepath is expected to be unreachable for normal reads.
				 */
				if (zero_damaged_pages || InRecovery)
				{
					Assert(false);	/* see md.c commentary */

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

void
umfile_startreadv(PgAioHandle *ioh, UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	off_t		seekpos;
	UmfdVec    *v;
	BlockNumber nblocks_this_segment;
	struct iovec *iov;
	int			iovcnt;
	int			ret;

	v = umfile_getseg(ctx, ctx->rlocator,
					  forknum, blocknum, false /* skipFsync */,
					  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY,
					  RelFileLocatorBackendIsTemp(ctx->rlocator));

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nblocks_this_segment =
		Min(nblocks, RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));

	if (nblocks_this_segment != nblocks)
		elog(ERROR, "read crossing segment boundary");

	iovcnt = pgaio_io_get_iovec(ioh, &iov);
	Assert(nblocks <= (BlockNumber) iovcnt);

	iovcnt = umfile_buffers_to_iovec(iov, buffers, (int) nblocks_this_segment);

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
}

void
umfile_startreadv_physical(PgAioHandle *ioh, UmbraFileContext *ctx,
						   ForkNumber forknum,
						   BlockNumber logical_blocknum,
						   BlockNumber physical_blocknum,
						   void **buffers, BlockNumber nblocks)
{
	off_t		seekpos;
	UmfdVec    *v;
	BlockNumber nblocks_this_segment;
	struct iovec *iov;
	int			iovcnt;
	int			ret;

	/*
	 * Caller is responsible for not crossing physical segment boundaries.
	 * Umbra MAP translation enforces single-block I/O via ummaxcombine().
	 */
	Assert(nblocks >= 1);
	v = umfile_getseg(ctx, ctx->rlocator,
					  forknum, physical_blocknum, false /* skipFsync */,
					  UM_EXTENSION_FAIL,
					  RelFileLocatorBackendIsTemp(ctx->rlocator));

	seekpos = (off_t) BLCKSZ * (physical_blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nblocks_this_segment =
		Min(nblocks,
			RELSEG_SIZE - (physical_blocknum % ((BlockNumber) RELSEG_SIZE)));

	if (nblocks_this_segment != nblocks)
		elog(ERROR, "read crossing segment boundary");

	iovcnt = pgaio_io_get_iovec(ioh, &iov);
	Assert(nblocks <= (BlockNumber) iovcnt);

	iovcnt = umfile_buffers_to_iovec(iov, buffers, (int) nblocks_this_segment);

	if (!(io_direct_flags & IO_DIRECT_DATA))
		pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);

	/*
	 * Preserve logical identity for AIO completion reporting and reopen.
	 * The started I/O uses physical addressing (file/seekpos).
	 */
	ret = FileStartReadV(ioh, v->umfd_vfd, iovcnt, seekpos,
						 WAIT_EVENT_DATA_FILE_READ);
	if (ret != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not start reading blocks %u..%u in file \"%s\": %m",
						logical_blocknum,
						logical_blocknum + nblocks_this_segment - 1,
						FilePathName(v->umfd_vfd))));
}

void
umfile_writev(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
			  const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		int			iovcnt;
		off_t		seekpos;
		int			nbytes;
		UmfdVec    *v;
		BlockNumber nblocks_this_segment;
		size_t		transferred_this_segment;
		size_t		size_this_segment;

		v = umfile_getseg(ctx, ctx->rlocator,
						  forknum, blocknum, false /* skipFsync */,
						  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY,
						  RelFileLocatorBackendIsTemp(ctx->rlocator));

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

		nblocks_this_segment =
			Min(nblocks, RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
		nblocks_this_segment = Min(nblocks_this_segment, (BlockNumber) lengthof(iov));

		if (nblocks_this_segment != nblocks)
			elog(ERROR, "write crosses segment boundary");

		iovcnt = umfile_buffers_to_iovec(iov, (void **) buffers,
										 (int) nblocks_this_segment);

		size_this_segment = nblocks_this_segment * BLCKSZ;
		transferred_this_segment = 0;

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

			transferred_this_segment += nbytes;
			Assert(transferred_this_segment <= size_this_segment);
			if (transferred_this_segment == size_this_segment)
				break;

			/* Adjust position and vectors after a short write. */
			seekpos += nbytes;
			iovcnt = compute_remaining_iovec(iov, iov, iovcnt, nbytes);
		}

		if (!skipFsync && !RelFileLocatorBackendIsTemp(ctx->rlocator))
			umfile_register_dirty_seg(ctx->rlocator,
									  RelFileLocatorBackendIsTemp(ctx->rlocator),
									  forknum, v);

		nblocks -= nblocks_this_segment;
		buffers += nblocks_this_segment;
		blocknum += nblocks_this_segment;
	}
}

void
umfile_writeback(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
				 BlockNumber nblocks)
{
	UmfdVec    *v;
	off_t		seekpos;

	while (nblocks > 0)
	{
		BlockNumber nflush;

			v = umfile_getseg(ctx, ctx->rlocator,
							  forknum, blocknum, false /* skipFsync */,
							  UM_EXTENSION_FAIL | UM_EXTENSION_CREATE_RECOVERY,
							  RelFileLocatorBackendIsTemp(ctx->rlocator));
		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

		nflush = Min(nblocks, (BlockNumber) RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
		Assert(nflush >= 1);
		Assert(nflush <= nblocks);

		FileWriteback(v->umfd_vfd, seekpos, (off_t) BLCKSZ * nflush, WAIT_EVENT_DATA_FILE_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}

static BlockNumber
umfile_nblocks_sparse(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					  ForkNumber forknum)
{
	UmfdVec    *v;
	BlockNumber nblocks;
	BlockNumber	minsegno;
	BlockNumber	maxsegno;

	Assert(umfile_fork_allows_sparse_segments(forknum));

	if (!umfile_sparse_fork_scan_segments(ctx, forknum, &minsegno, &maxsegno))
		return 0;

	if (maxsegno >= (BlockNumber) ctx->num_open_segs[forknum] ||
		!umfile_seg_entry_is_open(umfile_v_get(ctx, forknum, (int) maxsegno)))
	{
		v = umfile_openseg(ctx, rlocator, forknum, maxsegno, 0);
		if (v == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m",
							umfile_segpath(rlocator, forknum, maxsegno).str)));
	}
	else
		v = umfile_v_get(ctx, forknum, (int) maxsegno);

	nblocks = umfile_nblocks_in_seg(v->umfd_vfd);
	if (nblocks > (BlockNumber) RELSEG_SIZE)
		elog(FATAL, "segment too big");
	return (maxsegno * ((BlockNumber) RELSEG_SIZE)) + nblocks;
}

static BlockNumber
umfile_nblocks_dense(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 ForkNumber forknum)
{
	UmfdVec    *v;
	BlockNumber nblocks;
	BlockNumber segno;

	/*
	 * Match md.c semantics: missing forks read as size 0.
	 *
	 * This is relied on by size-reporting code paths (pg_table_size, psql \d+),
	 * and by callers that probe optional forks without doing smgrexists() first.
	 */
	if (umfile_openfork(ctx, rlocator, forknum, UM_EXTENSION_RETURN_NULL) == NULL)
		return 0;
	Assert(ctx->num_open_segs[forknum] > 0);

	segno = ctx->num_open_segs[forknum] - 1;
	v = umfile_v_get(ctx, forknum, segno);

	for (;;)
	{
		nblocks = umfile_nblocks_in_seg(v->umfd_vfd);
		if (nblocks > (BlockNumber) RELSEG_SIZE)
			elog(FATAL, "segment too big");
		if (nblocks < (BlockNumber) RELSEG_SIZE)
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		segno++;
		v = umfile_openseg(ctx, rlocator, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

BlockNumber
umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum, UmFileNblocksMode mode)
{
	RelFileLocatorBackend rlocator;

	Assert(ctx != NULL);
	rlocator = ctx->rlocator;

	if (mode == UMFILE_NBLOCKS_SPARSE)
		return umfile_nblocks_sparse(ctx, rlocator, forknum);

	return umfile_nblocks_dense(ctx, rlocator, forknum);
}

void
umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
				BlockNumber curnblk, BlockNumber nblocks)
{
	BlockNumber priorblocks;
	int			curopensegs;

	if (nblocks > curnblk)
	{
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(ctx->rlocator, forknum).str,
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;

	curopensegs = ctx->num_open_segs[forknum];
	while (curopensegs > 0)
	{
		UmfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;
		v = umfile_v_get(ctx, forknum, curopensegs - 1);

		if (priorblocks > nblocks)
		{
			if (FileTruncate(v->umfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->umfd_vfd))));

			if (!RelFileLocatorBackendIsTemp(ctx->rlocator))
				umfile_register_dirty_seg(ctx->rlocator,
										  RelFileLocatorBackendIsTemp(ctx->rlocator),
										  forknum, v);

			Assert(v != umfile_v_get(ctx, forknum, 0));

			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (FileTruncate(v->umfd_vfd, (off_t) lastsegblocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->umfd_vfd),
								nblocks)));

			if (!RelFileLocatorBackendIsTemp(ctx->rlocator))
				umfile_register_dirty_seg(ctx->rlocator,
										  RelFileLocatorBackendIsTemp(ctx->rlocator),
										  forknum, v);
		}
		else
		{
			break;
		}
		curopensegs--;
	}
}

void
umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	if (umfile_fork_allows_sparse_segments(forknum))
	{
		RelPathStr	path = umfile_segpath(ctx->rlocator, forknum, 0);
		BlockNumber *segnos = NULL;
		int			nsegnos = 0;
		int			i;

		if (!umfile_collect_existing_segnos_by_path(path.str, &segnos, &nsegnos))
			return;

		for (i = 0; i < nsegnos; i++)
		{
			BlockNumber	curseg = segnos[i];
			UmfdVec	   *v;

			if (curseg < (BlockNumber) ctx->num_open_segs[forknum] &&
				umfile_seg_entry_is_open(umfile_v_get(ctx, forknum, (int) curseg)))
				v = umfile_v_get(ctx, forknum, (int) curseg);
			else
			{
				v = umfile_openseg(ctx, ctx->rlocator, forknum, curseg, 0);
				if (v == NULL)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									umfile_segpath(ctx->rlocator, forknum, curseg).str)));
			}

			umfile_register_dirty_seg(ctx->rlocator,
									  RelFileLocatorBackendIsTemp(ctx->rlocator),
									  forknum, v);
		}

		if (segnos != NULL)
			pfree(segnos);
		return;
	}

	(void) umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	while (umfile_openseg(ctx, ctx->rlocator, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *v = umfile_v_get(ctx, forknum, segno - 1);

		umfile_register_dirty_seg(ctx->rlocator,
								  RelFileLocatorBackendIsTemp(ctx->rlocator),
								  forknum, v);

		if (segno > min_inactive_seg)
		{
			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, segno - 1);
		}

		segno--;
	}
}

void
umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	if (umfile_fork_allows_sparse_segments(forknum))
	{
		RelPathStr	path = umfile_segpath(ctx->rlocator, forknum, 0);
		BlockNumber *segnos = NULL;
		int			nsegnos = 0;
		int			i;

		if (!umfile_collect_existing_segnos_by_path(path.str, &segnos, &nsegnos))
			return;

		for (i = 0; i < nsegnos; i++)
		{
			BlockNumber	curseg = segnos[i];
			UmfdVec	   *v;

			if (curseg < (BlockNumber) ctx->num_open_segs[forknum] &&
				umfile_seg_entry_is_open(umfile_v_get(ctx, forknum, (int) curseg)))
				v = umfile_v_get(ctx, forknum, (int) curseg);
			else
			{
				v = umfile_openseg(ctx, ctx->rlocator, forknum, curseg, 0);
				if (v == NULL)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									umfile_segpath(ctx->rlocator, forknum, curseg).str)));
			}

			if (FileSync(v->umfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						 errmsg("could not fsync file \"%s\": %m",
								FilePathName(v->umfd_vfd))));
		}

		if (segnos != NULL)
			pfree(segnos);
		return;
	}

	(void) umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);

	min_inactive_seg = segno = ctx->num_open_segs[forknum];

	while (umfile_openseg(ctx, ctx->rlocator, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		UmfdVec    *v = umfile_v_get(ctx, forknum, segno - 1);

		if (FileSync(v->umfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->umfd_vfd))));

		if (segno > min_inactive_seg)
		{
			FileClose(v->umfd_vfd);
			umfile_fdvec_resize(ctx, forknum, segno - 1);
		}

		segno--;
	}

}

int
umfile_fd(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	UmfdVec    *v;

	/*
	 * Sparse mapped forks can address a target segment even when segment 0 is
	 * absent. Reopen the specific target segment directly instead of insisting
	 * that segment 0 exists.
	 */
	if (!umfile_fork_allows_sparse_segments(forknum))
		(void) umfile_openfork(ctx, ctx->rlocator, forknum, UM_EXTENSION_FAIL);

	v = umfile_getseg(ctx, ctx->rlocator,
					  forknum, blocknum, false /* skipFsync */,
					  UM_EXTENSION_FAIL,
					  RelFileLocatorBackendIsTemp(ctx->rlocator));

	*off = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(*off < (off_t) BLCKSZ * RELSEG_SIZE);

	return FileGetRawDesc(v->umfd_vfd);
}
