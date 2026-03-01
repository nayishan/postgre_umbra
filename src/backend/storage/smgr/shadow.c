
/*-------------------------------------------------------------------------
 *
 * shadow.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/shadow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "miscadmin.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/bufmgr.h"
#include "storage/shadow.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "storage/copydir.h"
#include <sys/stat.h>




#include "access/slru.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "access/xact.h"

#ifdef SHD_DEBUG
#define debug_elog2(a,b) elog(a,b)
#define debug_elog3(a,b,c) elog(a,b,c)
#define debug_elog4(a,b,c,d) elog(a,b,c,d)
#define debug_elog5(a,b,c,d,e) elog(a,b,c,d,e)
#define debug_elog6(a,b,c,d,e,f) elog(a,b,c,d,e,f)
#else
#define debug_elog2(a,b)
#define debug_elog3(a,b,c)
#define debug_elog4(a,b,c,d)
#define debug_elog5(a,b,c,d,e)
#define debug_elog6(a,b,c,d,e,f)
#endif

typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
}			MdfdVec;

static MemoryContext ShdCxt;	/* context for all MdfdVec objects */


/* Populate a file tag describing an md.c segment file. */

/*
 * for now SYNC_HANDLER_MD;
 */
#define INIT_SHD_FILETAG(a,xx_rnode,xx_forknum,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_SHD, \
	(a).rnode = (xx_rnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)


/*** behavior for mdopen & _mdfd_getseg ***/
/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/*
 * Allow opening segments which are preceded by segments smaller than
 * RELSEG_SIZE, e.g. inactive segments (see above). Note that this breaks
 * mdnblocks() and related functionality henceforth - which currently is ok,
 * because this is only required in the checkpointer which never uses
 * mdnblocks().
 */
#define EXTENSION_DONT_CHECK_SIZE	(1 << 4)


#define UNUSED InvalidOid

/* Reserved Shadow database IDs */
#define SHD_SDBID_SHARED		0	/* shared objects (tablespaces, roles,
									 * etc.) */
#define SHD_SDBID_TEMPLATE1		1	/* template1 database */
#define SHD_SDBID_NORMAL		2	/* minimum allocatable sdbId */

#define SHD_REG_LIMIT_PERCENT (0.9)

#define TABLETICK  ((Oid)1)		/* 1 is not used for now, should forbin use
								 * reloid 1 later in postgre */
#define BASETICK	((Oid)2)	/* 2,3 is a dangerous number, should avoid
								 * repeat a normal database , to do ... */

/* local routines */
static void shdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
						  bool isRedo);
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, int behavior);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum,
								   MdfdVec *seg);
static void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void _fdvec_resize(SMgrRelation reln,
						  ForkNumber forknum,
						  int nseg);
static char *_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno,
							   BlockNumber segno, int oflags);
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno,
							  BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum,
							  MdfdVec *seg);
static int	ShdRelBaseOffset(int sdbId);
static int	ShdRelMaxPerDb(void);
static ShdBlkStatus ShdBlkMetaGetStatus(SMgrRelation reln, ForkNumber fork, BlockNumber blk);

static void ShdDbDropInternal(int sdbId);
static int	ShdDbIdLookupInternal(Oid dbOid);
static int	ShdRelIdLookup(Oid relOid, int sdbId);
static ShdBlkStatus
ShdBlkMetaGetOppositeBit(BlockNumber blk, int index);

static ShdBlkStatus ShdBlkMetaGetStatusBit(BlockNumber blk, int index);
static void
ShdBlkSetBit(BlockNumber blk, int index, ShdBlkStatus status);
static void ShdBlkMetaExtendFor(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum);

/* static void ShdRelMetaCleanAll(int sdbId); */
/* static void ShdBlkMetaCleanAll(int grelId); */

static void ShdBlkMetaTruncate(Oid relOid);

static void ShdRelDropInternal(ShdRelMeta meta);
static void ShdBlkMetaExtend(int grelId, BlockNumber blk);

static int	ZeroShdDbMetaPage(int pageno);
static int	ZeroShdBlkMetaPage(int pageno, int grelId);
static int	ZeroShdRelMetaPage(int pageno, int sdbId);
static void ShdRelCreateRedo(xl_shd_relmeta *xlrec);
static void ShdDbCreateRedo(xl_shd_dbmeta *xlrec);

static bool ShdDbMetaPagePrecedes(int page1, int page2);
static bool ShdRelMetaPagePrecedes(int page1, int page2);
static bool ShdBlkMetaPagePrecedes(int page1, int page2);

static bool ShdSlruScanFindLatest(SlruCtl ctl, char *filename, int segpage, void *data);
static int	ShdRelIdAllocate(Oid relOid, int sdbId);
/* static int ShdDbIdAllocate(Oid dbOid, Oid srcOid, int srcSdbId); */
static int	ShdDbIdAllocate(Oid dbOid, Oid srcOid, int srcSdbId, bool isRedo);


static ShdBlkStatus
ShdBlkToggleBit(BlockNumber blk, int index, XLogRecPtr lsn);
static int	ShdRelOidToLocalId(Oid relOid, int sdbId);
static void ShdAssertValid(RelFileNode node, ForkNumber fork, int grelId);
static Oid ShdDbIdToOid(int sdbId);
static void ShdDbAssertValid(RelFileNode node, int sdbId);
static void ShdRelAssertValid(RelFileNode node, ForkNumber fork, int grelId);
static Oid ShdRelIdToOid(int grelId);
static void ShdRelMetaCopyForDb(int srcSdbId, int dstSdbId);
static void ShdBlkMetaCopyForRel(int srcRelId, int dstRelId);

static void ShdRelMetaCloneFromSrc(int srcId, int dstId);
static void ShdBlkMetaCloneFromSrc(int sdbSrcId, int sdbDstId);

static int	cachedSdbId = -1;
static Oid cachedDbOid = InvalidOid;

static int	cachedLrelId = -1;
static Oid cachedRelOid = InvalidOid;

/* ============ DbMeta Cache Operations ============ */

/* Set DbMeta cache */
static inline void
ShdDbMetaSetCache(Oid dbOid, int sdbId)
{
	cachedSdbId = sdbId;
	cachedDbOid = dbOid;
}

/* Invalidate DbMeta cache (conditional match) + cascade invalidate RelMeta cache */
static inline void
ShdDbMetaInvalidateCacheIfMatch(Oid dbOid)
{
	if (cachedDbOid == dbOid)
	{
		cachedSdbId = -1;
		cachedDbOid = InvalidOid;
		/* Cascade invalidation: invalidate relation cache (unconditional) */
		cachedLrelId = -1;
		cachedRelOid = InvalidOid;
	}
}

/* ============ RelMeta Cache Operations ============ */

/* Set RelMeta cache */
static inline void
ShdRelMetaSetCache(Oid relOid, int lrelId)
{
	cachedLrelId = lrelId;
	cachedRelOid = relOid;
}

/* Invalidate RelMeta cache (conditional match) */
static inline void
ShdRelMetaInvalidateCacheIfMatch(Oid relOid)
{
	if (cachedRelOid == relOid)
	{
		cachedLrelId = -1;
		cachedRelOid = InvalidOid;
	}
}

/* ============ Fork Utilities ============ */

/*
 * ShdForkGroupIndex
 *		Calculate the fork group index from fork number.
 *
 * MAIN_FORKNUM (0) and FSM_FORKNUM (1) map to group 0.
 * VISIBILITYMAP_FORKNUM (2) maps to group 1.
 *
 * This is used to convert fork numbers to a smaller index space,
 * which is needed for indexing block metadata.
 */
static inline int
ShdForkGroupIndex(ForkNumber fork)
{
	return fork / 2;
}

/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void
shdinit(void)
{
	ShdCxt = AllocSetContextCreate(TopMemoryContext,
								   "ShdSmgr",
								   ALLOCSET_DEFAULT_SIZES);
}

static ForkNumber IsInitFork(ForkNumber forknum)
{
	if (forknum == INIT_FORKNUM)
		return true;
	return false;
}
static bool
IsTempRel(RelFileNodeBackend rnode)
{
	return (rnode.backend != InvalidBackendId);
}
static bool
IsShadow(ForkNumber forknum)
{
	if (forknum == SHADOW_FORKNUM || forknum == VISIBILITYMAP_SHADOW_FORKNUM
		|| forknum == FSM_SHADOW_FORKNUM)
	{
		return true;
	}
	return false;
}

static bool
NeedShadow(RelFileNodeBackend rnode, ForkNumber forknum)
{
	return (!IsTempRel(rnode) && !IsInitFork(forknum));
}


static ForkNumber ShadowFork(ForkNumber forknum)
{
	ForkNumber	shadow;

	debug_elog5(LOG, "%s,%d,forknum:%d", __func__, __LINE__, forknum);
	Assert(forknum == MAIN_FORKNUM || forknum == VISIBILITYMAP_FORKNUM
		   || forknum == FSM_FORKNUM);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			shadow = SHADOW_FORKNUM;
			break;
		case VISIBILITYMAP_FORKNUM:
			shadow = VISIBILITYMAP_SHADOW_FORKNUM;
			break;
		case FSM_FORKNUM:
			shadow = FSM_SHADOW_FORKNUM;
			break;
		default:
			elog(PANIC, "wrong forknumber");
	}
	return shadow;
}

void
ShdDropRelationMetas(ShdRelMeta * delmetas, int ndelmetas, bool isRedo)
{
	int			i;

	for (i = 0; i < ndelmetas; i++)
	{
		smgrreldrop(delmetas[i], isRedo);
	}
}

/*
 *	mdexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
shdexists(SMgrRelation reln, ForkNumber forkNum)
{
	bool		isShadow = IsShadow(forkNum);
	bool		needShd = NeedShadow(reln->smgr_rnode, forkNum);
	ForkNumber	shadow;

	if (isShadow)
	{
		return false;
	}
	if (needShd)
	{
		shadow = ShadowFork(forkNum);
	}

	/*
	 * Close it first, to ensure that we notice if the fork has been unlinked
	 * since we opened it.
	 */
	shdclose(reln, forkNum);

	if (needShd)
	{
		return ((mdopen(reln, forkNum, EXTENSION_RETURN_NULL) != NULL) &&
				(mdopen(reln, shadow, EXTENSION_RETURN_NULL) != NULL));
	}
	else
	{
		return (mdopen(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
	}
}

/*
 *	mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
shdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	MdfdVec    *mdfd;
	MdfdVec    *shdmdfd;
	char	   *path;
	File		fd;
	char	   *shdpath;
	File		shdfd;
	bool		isShadow = IsShadow(forkNum);
	bool		needShd = NeedShadow(reln->smgr_rnode, forkNum);
	ForkNumber	shadow;

	if (isShadow)
	{
		return;
	}
	if (needShd)
	{
		shadow = ShadowFork(forkNum);
	}

	debug_elog5(WARNING, "%s,%d,forknum:%d", __func__, __LINE__, forkNum);
	if (isRedo && reln->md_num_open_segs[forkNum] > 0 && reln->md_num_open_segs[shadow] > 0)
		return;					/* created and opened already... */

	Assert(reln->md_num_open_segs[forkNum] == 0);
#ifdef USE_ASSERT_CHECKING
	if (needShd)
	{
		Assert(reln->md_num_open_segs[shadow] == 0);
	}
#endif

	path = relpath(reln->smgr_rnode, forkNum);

	debug_elog5(WARNING, "%s,%d,path:%s", __func__, __LINE__, path);
	fd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (fd < 0)
		{
			/* be sure to report the error reported by create, not open */
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));
		}
	}

	pfree(path);

	_fdvec_resize(reln, forkNum, 1);
	mdfd = &reln->md_seg_fds[forkNum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;

	if (!SmgrIsTemp(reln))
	{
		register_dirty_segment(reln, forkNum, mdfd);
	}

	if (!needShd)
		return;

	{
		shdpath = relpath(reln->smgr_rnode, shadow);
		debug_elog5(WARNING, "%s,%d,shdpath:%s", __func__, __LINE__, shdpath);

		shdfd = PathNameOpenFile(shdpath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

		if (shdfd < 0)
		{
			int			save_errno = errno;

			if (isRedo)
				shdfd = PathNameOpenFile(shdpath, O_RDWR | PG_BINARY);
			if (shdfd < 0)
			{
				/* be sure to report the error reported by create, not open */
				errno = save_errno;
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create file \"%s\": %m", shdpath)));
			}
		}
		pfree(shdpath);

		_fdvec_resize(reln, shadow, 1);
		shdmdfd = &reln->md_seg_fds[shadow][0];
		shdmdfd->mdfd_vfd = shdfd;
		shdmdfd->mdfd_segno = 0;
		if (!SmgrIsTemp(reln))
			register_dirty_segment(reln, shadow, shdmdfd);

		/*
		 * if redo we do it in shdcontrol redo not now
		 */
	}
}

/*
 *	mdunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenode
 * number from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenode as
 *	  the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as for instance CLUSTER and CREATE INDEX do),
 * the contents of the file would be lost forever.  By leaving the empty file
 * until after the next checkpoint, we prevent reassignment of the relfilenode
 * number until it's safe, because relfilenode assignment skips over any
 * existing file.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenode number.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenode number from being recycled.  Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
shdunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/* Now do the per-fork work */
	if (IsShadow(forkNum))
	{
		return;
	}
	debug_elog5(WARNING, "%s,%d,shadow:%d", __func__, __LINE__, forkNum);
	if (forkNum == InvalidForkNumber)
	{
		for (forkNum = 0; forkNum < MAX_FORKNUM; forkNum++)
			shdunlinkfork(rnode, forkNum, isRedo);
	}
	else
		shdunlinkfork(rnode, forkNum, isRedo);
}

/*
 * Truncate a file to release disk space.
 */
static int
do_truncate(const char *path)
{
	int			save_errno;
	int			ret;
	int			fd;

	/* truncate(2) would be easier here, but Windows hasn't got it */
	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd >= 0)
	{
		ret = ftruncate(fd, 0);
		save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
	}
	else
		ret = -1;

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
shdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	char	   *path;
	int			ret;
	char	   *shdpath;
	int			shdret;
	ForkNumber	shadow;
	bool		needShd = NeedShadow(rnode, forkNum);

	if (IsShadow(forkNum))
	{
		return;
	}

	if (needShd)
		shadow = ShadowFork(forkNum);


	path = relpath(rnode, forkNum);

	if (needShd)
	{
		shdpath = relpath(rnode, shadow);
	}

	/*
	 * Delete or truncate the first segment.
	 */
	if (isRedo || forkNum != MAIN_FORKNUM || RelFileNodeBackendIsTemp(rnode))
	{
		if (!RelFileNodeBackendIsTemp(rnode))
		{
			/* Prevent other backends' fds from holding on to the disk space */
			ret = do_truncate(path);

			/* Forget any pending sync requests for the first segment */
			register_forget_request(rnode, forkNum, 0 /* first seg */ );
			if (needShd)
			{
				shdret = do_truncate(shdpath);

				/* Forget any pending sync requests for the first segment */
				register_forget_request(rnode, shadow, 0 /* first seg */ );
			}
		}
		else
		{
			ret = 0;
			if (needShd)
			{
				shdret = 0;
			}
		}

		/* Next unlink the file, unless it was already found to be missing */
		if (ret == 0 || errno != ENOENT)
		{
			ret = unlink(path);
			if (ret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", path)));
		}
		if (needShd && (shdret == 0 || errno != ENOENT))
		{
			shdret = unlink(shdpath);
			if (shdret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", shdpath)));
		}
	}
	else
	{
		/* Prevent other backends' fds from holding on to the disk space */
		ret = do_truncate(path);

		debug_elog5(WARNING, "%s,%d,path:%s", __func__, __LINE__, path);
		/* Register request to unlink first segment later */
		register_unlink_segment(rnode, forkNum, 0 /* first seg */ );
		if (needShd)
		{
			shdret = do_truncate(shdpath);

			debug_elog5(WARNING, "%s,%d,shdpath:%s", __func__, __LINE__, shdpath);
			/* Register request to unlink first segment later */
			register_unlink_segment(rnode, shadow, 0 /* first seg */ );

		}
	}

	/*
	 * Delete any additional segments.
	 */
	if (ret >= 0)
	{
		char	   *segpath = (char *) palloc(strlen(path) + 12);
		BlockNumber segno;

		/*
		 * Note that because we loop until getting ENOENT, we will correctly
		 * remove all inactive segments as well as active ones.
		 */
		for (segno = 1;; segno++)
		{
			sprintf(segpath, "%s.%u", path, segno);
			debug_elog5(WARNING, "%s,%d,segpath:%s", __func__, __LINE__, segpath);

			if (!RelFileNodeBackendIsTemp(rnode))
			{
				/*
				 * Prevent other backends' fds from holding on to the disk
				 * space.
				 */
				if (do_truncate(segpath) < 0 && errno == ENOENT)
					break;

				/*
				 * Forget any pending sync requests for this segment before we
				 * try to unlink.
				 */
				register_forget_request(rnode, forkNum, segno);
			}

			if (unlink(segpath) < 0)
			{
				/* ENOENT is expected after the last segment... */
				if (errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath)));
				break;
			}
		}
		pfree(segpath);
	}

	pfree(path);

	if (!needShd)
		return;

	if (shdret >= 0)
	{
		char	   *segpath = (char *) palloc(strlen(shdpath) + 12);
		BlockNumber segno;

		/*
		 * Note that because we loop until getting ENOENT, we will correctly
		 * remove all inactive segments as well as active ones.
		 */
		for (segno = 1;; segno++)
		{
			sprintf(segpath, "%s.%u", shdpath, segno);

			debug_elog5(WARNING, "%s,%d,segpath:%s", __func__, __LINE__, segpath);
			if (!RelFileNodeBackendIsTemp(rnode))
			{
				/*
				 * Prevent other backends' fds from holding on to the disk
				 * space.
				 */
				if (do_truncate(segpath) < 0 && errno == ENOENT)
					break;

				/*
				 * Forget any pending sync requests for this segment before we
				 * try to unlink.
				 */
				register_forget_request(rnode, shadow, segno);
			}

			if (unlink(segpath) < 0)
			{
				/* ENOENT is expected after the last segment... */
				if (errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath)));
				break;
			}
		}
		pfree(segpath);
	}
	pfree(shdpath);

}

/*
 *	mdextend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
shdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	ForkNumber	shadow;
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= shdnblocks(reln, forknum));
#endif
	if (needShd)
	{
		ShdBlkMetaExtendFor(reln->smgr_rnode.node, forknum, blocknum);
		shadow = ShadowFork(forknum);
	}

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
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	if ((nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->mdfd_vfd)),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ, blocknum),
				 errhint("Check free disk space.")));
	}
	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));

	if (!needShd)
		return;

	{
		v = _mdfd_getseg(reln, shadow, blocknum, skipFsync, EXTENSION_CREATE);

		if ((nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ)
		{
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not extend file \"%s\": %m",
								FilePathName(v->mdfd_vfd)),
						 errhint("Check free disk space.")));
			/* short write: complain appropriately */
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
							FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ, blocknum),
					 errhint("Check free disk space.")));
		}
		if (!skipFsync && !SmgrIsTemp(reln))
			register_dirty_segment(reln, shadow, v);
		Assert(_mdnblocks(reln, shadow, v) <= ((BlockNumber) RELSEG_SIZE));
	}

}


/*
 *	mdopen() -- Open the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
 * EXTENSION_CREATE means it's OK to extend an existing relation, not to
 * invent one out of whole cloth.
 */
/*
 * need not return shadow
 */
static MdfdVec *
mdopen(SMgrRelation reln, ForkNumber forknum, int behavior)
{
	MdfdVec    *mdfd;
	char	   *path;
	File		fd;

	/* No work if already open */
	if (reln->md_num_open_segs[forknum] > 0)
		return &reln->md_seg_fds[forknum][0];

	path = relpath(reln->smgr_rnode, forknum);

	fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);

	if (fd < 0)
	{
		if ((behavior & EXTENSION_RETURN_NULL) &&
			FILE_POSSIBLY_DELETED(errno))
		{
			pfree(path);
			return NULL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	pfree(path);

	_fdvec_resize(reln, forknum, 1);
	mdfd = &reln->md_seg_fds[forknum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;

	Assert(_mdnblocks(reln, forknum, mdfd) <= ((BlockNumber) RELSEG_SIZE));

	return mdfd;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 */
void
shdclose(SMgrRelation reln, ForkNumber forknum)
{
	int			nopensegs = reln->md_num_open_segs[forknum];
	int			nopenshadow;
	ForkNumber	shadow;
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);

	if (IsShadow(forknum))
	{
		return;
	}
	if (needShd)
	{
		shadow = ShadowFork(forknum);
		nopenshadow = reln->md_num_open_segs[shadow];
		debug_elog5(WARNING, "%s,%d,forknum:%d", __func__, __LINE__, forknum);
		debug_elog6(WARNING, "%s,%d,nopens:%d,nshadows:%d", __func__, __LINE__, nopensegs, nopenshadow);
	}

	/* No work if already closed */
	if (!needShd)
	{
		if (nopensegs == 0)
			return;
	}
	else
	{
		if (nopensegs == 0 && nopenshadow == 0)
			return;
	}

	/* close segments starting from the end */
	while (nopensegs > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][nopensegs - 1];
#ifdef SHD_DEBUG
		char	   *path = relpath(reln->smgr_rnode, forknum);

		debug_elog6(WARNING, "%s,%d,shadow:%d,path:%s", __func__, __LINE__, forknum, path);
#endif

		FileClose(v->mdfd_vfd);
		_fdvec_resize(reln, forknum, nopensegs - 1);
		nopensegs--;
	}

	if (!needShd)
		return;

	if (nopenshadow == 0)
	{
		return;
	}

	while (nopenshadow > 0)
	{
		MdfdVec    *shdv = &reln->md_seg_fds[shadow][nopenshadow - 1];
#ifdef SHD_DEBUG
		char	   *path = relpath(reln->smgr_rnode, shadow);

		debug_elog6(WARNING, "%s,%d,shadow:%d,path:%s", __func__, __LINE__, shadow, path);
#endif
		Assert(shdv != NULL);
		FileClose(shdv->mdfd_vfd);
		_fdvec_resize(reln, shadow, nopenshadow - 1);
		nopenshadow--;
	}
}

/*
 *	mdprefetch() -- Initiate asynchronous read of the specified block of a relation
 */
/*
 * do not support for now
 */
void
shdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
#endif							/* USE_PREFETCH */
}

/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
shdwriteback(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, BlockNumber nblocks)
{
	ForkNumber	shadow;
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
#ifdef SHD_DEBUG
	char	   *path = relpath(reln->smgr_rnode, forknum);

	debug_elog6(WARNING, "%s,%d,shadow:%d,path:%s", __func__, __LINE__, forknum, path);
#endif
	Assert(!IsShadow(forknum));

	if (needShd)
	{
		shadow = ShadowFork(forknum);
	}

	/*
	 * Issue flush requests in as few requests as possible; have to split at
	 * segment boundaries though, since those are actually separate files.
	 */
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		off_t		seekpos;
		MdfdVec    *v;
		int			segnum_start,
					segnum_end;

		if (needShd)
		{
			ShdBlkStatus status = ShdBlkMetaGetStatus(reln, forknum, blocknum);

			if (PONG == status)
				forknum = shadow;
		}

		v = _mdfd_getseg(reln, forknum, blocknum, true /* not used */ ,
						 EXTENSION_RETURN_NULL);

		/*
		 * We might be flushing buffers of already removed relations, that's
		 * ok, just ignore that case.
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

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush, WAIT_EVENT_DATA_FILE_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}

/*
 *	mdread() -- Read the specified block from a relation.
 */
void
shdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer)
{
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
	ForkNumber	shadow;
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	Assert(!IsShadow(forknum));

	/* trace: not calculated for shadow for now */
	if (needShd)
	{
		ShdBlkStatus status = ShdBlkMetaGetStatus(reln, forknum, blocknum);

		shadow = ShadowFork(forknum);
		if (PONG == status)
		{
			forknum = shadow;
		}
	}

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileRead(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_READ);

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode,
									   reln->smgr_rnode.backend,
									   nbytes,
									   BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ)));
	}
}

/*
 *	mdwrite() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
shdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	ForkNumber	shadow;

	Assert(!IsShadow(forknum));


	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < shdnblocks(reln, forknum));
#endif

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 reln->smgr_rnode.node.spcNode,
										 reln->smgr_rnode.node.dbNode,
										 reln->smgr_rnode.node.relNode,
										 reln->smgr_rnode.backend);

	if (needShd)
	{
		ShdBlkStatus status = ShdBlkMetaGetStatus(reln, forknum, blocknum);

		shadow = ShadowFork(forknum);
		if (PONG == status)
		{
			forknum = shadow;
		}
	}
	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_WRITE);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

/*
 *	mdnblocks() -- Get the number of blocks stored in a relation.
 *
 *		Important side effect: all active segments of the relation are opened
 *		and added to the mdfd_seg_fds array.  If this routine has not been
 *		called, then only segments up to the last one actually touched
 *		are present in the array.
 */
BlockNumber
shdnblocks(SMgrRelation reln, ForkNumber forknum)
{
	MdfdVec    *v = mdopen(reln, forknum, EXTENSION_FAIL);
	BlockNumber nblocks;
	BlockNumber segno = 0;
	BlockNumber shdSegno = 0;
#if USE_ASSERTION_CHECKING
	MdfdVec    *shdv_check;
	BlockNumber shdNblocks;
#endif
	bool		isShadow = IsShadow(forknum);
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
	ForkNumber	shadow;

	if (needShd && !isShadow)
	{
		/*
		 * we keep shadow with main
		 */
		shadow = ShadowFork(forknum);
#if USE_ASSERTION_CHECKING
		shdv_check = mdopen(reln, shadow, EXTENSION_FAIL);
#else
		(void) mdopen(reln, shadow, EXTENSION_FAIL);
#endif
	}

	/* mdopen has opened the first segment */
	Assert(reln->md_num_open_segs[forknum] > 0);

	/*
	 * Start from the last open segments, to avoid redundant seeks.  We have
	 * previously verified that these segments are exactly RELSEG_SIZE long,
	 * and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.  We rely on higher code levels to handle that
	 * scenario by closing and re-opening the md fd, which is handled via
	 * relcache flush.  (Since the checkpointer doesn't participate in
	 * relcache flush, it could have segment entries for inactive segments;
	 * that's OK because the checkpointer never needs to compute relation
	 * size.)
	 */
	segno = reln->md_num_open_segs[forknum] - 1;
	v = &reln->md_seg_fds[forknum][segno];

	if (needShd && !isShadow)
	{
		shdSegno = reln->md_num_open_segs[shadow] - 1;
#if USE_ASSERTION_CHECKING
		shdv_check = &reln->md_seg_fds[shadow][shdSegno];
#endif
	}

	for (;;)
	{
		nblocks = _mdnblocks(reln, forknum, v);
#if USE_ASSERTION_CHECKING
		if (needShd && !isShadow)
		{
			shdNblocks = _mdnblocks(reln, shadow, shdv_check);
		}
#endif
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
		{
#if USE_ASSERTION_CHECKING
			if (needShd && !isShadow)
			{
				Assert(((segno * ((BlockNumber) RELSEG_SIZE)) + nblocks) == (shdSegno * ((BlockNumber) RELSEG_SIZE)) + shdNblocks);
			}
#endif
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;
		}

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		/*
		 * We used to pass O_CREAT here, but that has the disadvantage that it
		 * might create a segment which has vanished through some operating
		 * system misadventure.  In such a case, creating the segment here
		 * undermines _mdfd_getseg's attempts to notice and report an error
		 * upon access to a missing segment.
		 */
		v = _mdfd_openseg(reln, forknum, segno, 0);
		if (needShd && !isShadow)
		{
			shdSegno++;
#if USE_ASSERTION_CHECKING
			shdv_check = _mdfd_openseg(reln, shadow, shdSegno, 0);
#else
			(void) _mdfd_openseg(reln, shadow, shdSegno, 0);
#endif

		}
		if (v == NULL)
		{
#if USE_ASSERTION_CHECKING
			if (needShd && !isShadow)
			{
				Assert(shdv_check == NULL);
				Assert((segno * ((BlockNumber) RELSEG_SIZE)) == (shdSegno * ((BlockNumber) RELSEG_SIZE)));
			}
#endif
			return segno * ((BlockNumber) RELSEG_SIZE);
		}
	}
}

/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 */
void
shdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	BlockNumber curnblk;
	BlockNumber priorblocks;
	int			curopensegs;
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
	ForkNumber	shadow;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * truncation loop will get them all!
	 */
	curnblk = shdnblocks(reln, forknum);
	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(reln->smgr_rnode, forknum),
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */
/*
 * if return before, ShdBlkMetaTruncate has already excute.
 */
	if (needShd)
	{
		shadow = ShadowFork(forknum);
		ShdBlkMetaTruncate(reln->smgr_rnode.node.relNode);
	}

	/*
	 * Truncate segments, starting at the last one. Starting at the end makes
	 * managing the memory for the fd array easier, should there be errors.
	 */
	curopensegs = reln->md_num_open_segs[forknum];
	while (curopensegs > 0)
	{
		MdfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &reln->md_seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->mdfd_vfd))));

			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);

			/* we never drop the 1st segment */
			Assert(v != &reln->md_seg_fds[forknum][0]);

			FileClose(v->mdfd_vfd);
			_fdvec_resize(reln, forknum, curopensegs - 1);
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

			if (FileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->mdfd_vfd),
								nblocks)));
			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);
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

	if (!needShd)
		return;

	curopensegs = reln->md_num_open_segs[shadow];
	while (curopensegs > 0)
	{
		MdfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &reln->md_seg_fds[shadow][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->mdfd_vfd))));

			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, shadow, v);

			/* we never drop the 1st segment */
			Assert(v != &reln->md_seg_fds[shadow][0]);

			FileClose(v->mdfd_vfd);
			_fdvec_resize(reln, shadow, curopensegs - 1);
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

			if (FileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->mdfd_vfd),
								nblocks)));
			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, shadow, v);
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
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.
 */
/*
 * xingneng pingjing !!!!!
 */
void
shdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	int			segno;
	bool		needShd = NeedShadow(reln->smgr_rnode, forknum);
	ForkNumber	shadow;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * fsync loop will get them all!
	 */
	shdnblocks(reln, forknum);

	segno = reln->md_num_open_segs[forknum];

	while (segno > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][segno - 1];

		if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->mdfd_vfd))));
		segno--;
	}


	if (!needShd)
		return;

	shadow = ShadowFork(forknum);

	segno = reln->md_num_open_segs[shadow];

	while (segno > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[shadow][segno - 1];

		if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->mdfd_vfd))));
		segno--;
	}

}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * ProcessSyncRequests to process later.  Otherwise, try to pass off the
 * fsync request to the checkpointer process.  If that fails, just do the
 * fsync locally before returning (we hope this will not happen often
 * enough to be a performance problem).
 */
static void
register_dirty_segment(SMgrRelation reln, ForkNumber forknum, MdfdVec * seg)
{
	FileTag		tag;

	INIT_SHD_FILETAG(tag, reln->smgr_rnode.node, forknum, seg->mdfd_segno);

	/* Temp relations should never be fsync'd */
	Assert(!SmgrIsTemp(reln));

	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */ ))
	{
		ereport(DEBUG1,
				(errmsg("could not forward fsync request because request queue is full")));

		if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(seg->mdfd_vfd))));
	}
}

/*
 * register_unlink_segment() -- Schedule a file to be deleted after next checkpoint
 */
static void
register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
	FileTag		tag;

	INIT_SHD_FILETAG(tag, rnode.node, forknum, segno);

	/* Should never be used with temp relations */
	Assert(!RelFileNodeBackendIsTemp(rnode));

	RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

/*
 * register_forget_request() -- forget any fsyncs for a relation fork's segment
 */
static void
register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
	FileTag		tag;

	INIT_SHD_FILETAG(tag, rnode.node, forknum, segno);

	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

static void
RegisterRelMetaUnlink(ShdRelMeta meta)
{
	FileTag		tag;
	RelFileNode faker = {.dbNode = meta.sdbId,.relNode = meta.lrelId};

	INIT_SHD_FILETAG(tag, faker, 0, 0);
	RegisterSyncRequest(&tag, SYNC_UNLINK_META_REQUEST, true);
}

static void
RegisterDbUnlink(int sdbId)
{
	FileTag		tag;
	RelFileNode faker = {.dbNode = sdbId};

	INIT_SHD_FILETAG(tag, faker, 0, 0);
	RegisterSyncRequest(&tag, SYNC_UNLINK_DB_META_REQUEST, true);
}

/*
 *	_fdvec_resize() -- Resize the fork's open segments array
 */
static void
_fdvec_resize(SMgrRelation reln,
			  ForkNumber forknum,
			  int nseg)
{
	if (nseg == 0)
	{
		if (reln->md_num_open_segs[forknum] > 0)
		{
			pfree(reln->md_seg_fds[forknum]);
			reln->md_seg_fds[forknum] = NULL;
		}
	}
	else if (reln->md_num_open_segs[forknum] == 0)
	{
		reln->md_seg_fds[forknum] =
			MemoryContextAlloc(ShdCxt, sizeof(MdfdVec) * nseg);
	}
	else
	{
		/*
		 * It doesn't seem worthwhile complicating the code to amortize
		 * repalloc() calls.  Those are far faster than PathNameOpenFile() or
		 * FileClose(), and the memory context internally will sometimes avoid
		 * doing an actual reallocation.
		 */
		reln->md_seg_fds[forknum] =
			repalloc(reln->md_seg_fds[forknum],
					 sizeof(MdfdVec) * nseg);
	}

	reln->md_num_open_segs[forknum] = nseg;
}

/*
 * Return the filename for the specified segment of the relation. The
 * returned string is palloc'd.
 */
static char *
_mdfd_segpath(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
	char	   *path,
			   *fullpath;

	path = relpath(reln->smgr_rnode, forknum);

	if (segno > 0)
	{
		fullpath = psprintf("%s.%u", path, segno);
		pfree(path);
	}
	else
		fullpath = path;

	return fullpath;
}

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 */
static MdfdVec *
_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags)
{
	MdfdVec    *v;
	int			fd;
	char	   *fullpath;

#ifdef SHD_DEBUG
	char	   *path = relpath(reln->smgr_rnode, forknum);

	debug_elog6(WARNING, "%s,%d,shadow:%d,path:%s", __func__, __LINE__, forknum, path);
#endif
	fullpath = _mdfd_segpath(reln, forknum, segno);

	/* open the file */
	fd = PathNameOpenFile(fullpath, O_RDWR | PG_BINARY | oflags);

	pfree(fullpath);

	if (fd < 0)
		return NULL;

	if (segno <= reln->md_num_open_segs[forknum])
		_fdvec_resize(reln, forknum, segno + 1);

	/* fill the entry */
	v = &reln->md_seg_fds[forknum][segno];
	v->mdfd_vfd = fd;
	v->mdfd_segno = segno;

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}

/*
 *	_mdfd_getseg() -- Find the segment of the relation holding the
 *		specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
static MdfdVec *
_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior)
{
	MdfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

#ifdef SHD_DEBUG
	char	   *path = relpath(reln->smgr_rnode, forknum);

	debug_elog6(WARNING, "%s,%d,shadow:%d,path:%s", __func__, __LINE__, forknum, path);
#endif
	/* some way to handle non-existent segments needs to be specified */
	Assert(behavior &
		   (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < reln->md_num_open_segs[forknum])
	{
		v = &reln->md_seg_fds[forknum][targetseg];
		return v;
	}

	/*
	 * The target segment is not yet open. Iterate over all the segments
	 * between the last opened and the target segment. This way missing
	 * segments either raise an error, or get created (according to
	 * 'behavior'). Start with either the last opened, or the first segment if
	 * none was opened before.
	 */
	if (reln->md_num_open_segs[forknum] > 0)
		v = &reln->md_seg_fds[forknum][reln->md_num_open_segs[forknum] - 1];
	else
	{
		v = mdopen(reln, forknum, behavior);
		if (!v)
			return NULL;		/* if behavior & EXTENSION_RETURN_NULL */
	}

	for (nextsegno = reln->md_num_open_segs[forknum];
		 nextsegno <= targetseg; nextsegno++)
	{
		BlockNumber nblocks = _mdnblocks(reln, forknum, v);
		int			flags = 0;

		Assert(nextsegno == v->mdfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & EXTENSION_CREATE) ||
			(InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Normally we will create new segments only if authorized by the
			 * caller (i.e., we are doing mdextend()).  But when doing WAL
			 * recovery, create segments anyway; this allows cases such as
			 * replaying WAL data that has a write into a high-numbered
			 * segment of a relation that was later deleted. We want to go
			 * ahead and create the segments so we can finish out the replay.
			 * However if the caller has specified
			 * EXTENSION_REALLY_RETURN_NULL, then extension is not desired
			 * even in recovery; we won't reach this point in that case.
			 *
			 * We have to maintain the invariant that segments before the last
			 * active segment are of size RELSEG_SIZE; therefore, if
			 * extending, pad them out with zeroes if needed.  (This only
			 * matters if in recovery, or if the caller is extending the
			 * relation discontiguously, but that can happen in hash indexes.)
			 */
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = palloc0(BLCKSZ);

				shdextend(reln, forknum,
						  nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
						  zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (!(behavior & EXTENSION_DONT_CHECK_SIZE) &&
				 nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			/*
			 * When not extending (or explicitly including truncated
			 * segments), only open the next segment if the current one is
			 * exactly RELSEG_SIZE.  If not (this branch), either return NULL
			 * or fail.
			 */
			if (behavior & EXTENSION_RETURN_NULL)
			{
				/*
				 * Some callers discern between reasons for _mdfd_getseg()
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
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno, nblocks)));
		}

		v = _mdfd_openseg(reln, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno)));
		}
	}

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber
_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec * seg)
{
	off_t		len;

	len = FileSize(seg->mdfd_vfd);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(seg->mdfd_vfd))));
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
shdsyncfiletag(const FileTag * ftag, char *path)
{
	SMgrRelation reln = smgropen(ftag->rnode, InvalidBackendId);
	File		file;
	bool		need_to_close;
	int			result,
				save_errno;

	debug_elog5(WARNING, "%s,%d,path:%s", __func__, __LINE__, path);
	/* Compute the path. */
	/* See if we already have the file open, or need to open it. */
	if (ftag->segno < reln->md_num_open_segs[ftag->forknum])
	{
		file = reln->md_seg_fds[ftag->forknum][ftag->segno].mdfd_vfd;
		strlcpy(path, FilePathName(file), MAXPGPATH);
		need_to_close = false;
	}
	else
	{
		char	   *p;

		p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
		strlcpy(path, p, MAXPGPATH);
		pfree(p);

		file = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (file < 0)
			return -1;
		need_to_close = true;
	}

	/* Sync the file. */
	result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	save_errno = errno;

	if (need_to_close)
		FileClose(file);

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
shdunlinkfiletag(const FileTag * ftag, char *path)
{
	char	   *p;

	/* Compute the path. */
	p = relpathperm(ftag->rnode, ftag->forknum);
	strlcpy(path, p, MAXPGPATH);
	pfree(p);

	debug_elog5(WARNING, "%s,%d,path:%s", __func__, __LINE__, path);
	/* Try to unlink the file. */
	return unlink(path);
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
shdfiletagmatches(const FileTag * ftag, const FileTag * candidate)
{
	/* Compute the path. */
	/*
	 * For now we only use filter requests as a way to drop all scheduled
	 * callbacks relating to a given database, when dropping the database.
	 * We'll return true for all candidates that have the same database OID as
	 * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
	 */
	return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

void
shd_unlink_relmeta(const FileTag * faker)
{
	ShdRelMeta	meta = {.sdbId = faker->rnode.dbNode,.lrelId = faker->rnode.relNode};

	ShdRelDropInternal(meta);
}

void
shd_unlink_dbmeta(const FileTag * faker)
{
	int			sdbId = faker->rnode.dbNode;

	ShdDbDropInternal(sdbId);
}
static SlruCtlData ShdDbCtlData;
static SlruCtlData ShdRelCtlData[MAX_SHD_DBS];
static SlruCtlData ShdBlkCtlData[MAX_SHD_REL];



#define ShdDbCtl	(&ShdDbCtlData)
#define SHD_DB_PER_PAGE (BLCKSZ / sizeof(Oid))
#define ShdDbIdToPage(sdbId) ((sdbId) / (Oid)SHD_DB_PER_PAGE)
#define ShdDbIdToPgIndex(sdbId) ((sdbId) % (Oid)SHD_DB_PER_PAGE)

#define SHD_REL_INDEX(index)	((index) % MAX_SHD_DBS)
#define ShdRelCtl(index)  (&ShdRelCtlData[SHD_REL_INDEX(index)])
#define ShdRelControlLock(index) (&MainLWLockArray[SHD_RELID_MAPPING_LWLOCK_OFFSET + SHD_REL_INDEX(index)].lock)
#define ShdRelLock(index, lockmode) ((void)LWLockAcquire(ShdRelControlLock(index),lockmode))
#define ShdRelUnlock(index) (LWLockRelease(ShdRelControlLock(index)))

#define SHD_BLK_INDEX(index) ((index) % MAX_SHD_REL)
#define ShdBlkCtl(index) (&ShdBlkCtlData[SHD_BLK_INDEX(index)])

#define ShdBlkControlLock(index) (&MainLWLockArray[SHDBLK_MAPPING_LWLOCK_OFFSET + SHD_BLK_INDEX(index)].lock)
#define ShdBlkLock(index, lockmode) ((void)LWLockAcquire(ShdBlkControlLock(index),lockmode))
#define ShdBlkUnlock(index) (LWLockRelease(ShdBlkControlLock(index)))

#define SHD_REL_PER_PAGE	(BLCKSZ / sizeof(Oid))
#define ShdRelIdToPage(grelId)  ((grelId) / (Oid)SHD_REL_PER_PAGE)
#define ShdRelIdToPgIndex(grelId)    ((grelId) % (Oid)SHD_REL_PER_PAGE)

#define SHD_BITS_PER_BLK	1
#define SHD_BLK_PER_BYTE 	8
#define SHD_BLK_PER_PAGE (BLCKSZ * SHD_BLK_PER_BYTE)
#define SHD_BLK_BITMASK	((1 << SHD_BITS_PER_BLK) - 1)

#define ShdBlkIdToPage(blk)	 ((blk) / (BlockNumber) SHD_BLK_PER_PAGE)
#define ShdBlkIdToPgIndex(blk) ((blk) % (BlockNumber) SHD_BLK_PER_PAGE)
#define ShdBlkIdToByte(blk)	 (ShdBlkIdToPgIndex(blk) / SHD_BLK_PER_BYTE)
#define ShdBlkIdToBIndex(blk)	 ((blk) % (BlockNumber) SHD_BLK_PER_BYTE)

#define SHD_BLK_PER_LSN_GROUP 64
#define SHD_BLK_LSN_PER_PAGE  (SHD_BLK_PER_PAGE / SHD_BLK_PER_LSN_GROUP)
#define GetShdBlkMetaLSNIndex(slotno, blk) ((slotno) * SHD_BLK_LSN_PER_PAGE) + \
	((blk) % (BlockNumber) SHD_BLK_PER_PAGE) / SHD_BLK_PER_LSN_GROUP


#define NUM_SHDDBS_BUFFERS	(1) /* only (MAX_SHD_DBS) */
#define NUM_SHD_RELID_BUFFERS 	(1)
/* 3 type files */
#define NUM_SHDBLK_BUFFERS (60)


#define INVALID_REGID (-1)


void
ShdRelCreate(SMgrRelation reln, ShdRelMeta *meta)
{
	/*
	 * We need a global counter for register, then we can extend shdreg.
	 * For now it is hardcoded "MAX_SHD_REL". TODO: ExtendShdReg
	 */
	int			slotno;
	int			sdbId;
	int			grelId;
	int			lrelId;
	int			j;

	Assert(meta != NULL);
	sdbId = ShdDbIdLookupInternal(reln->smgr_rnode.node.dbNode);
	meta->sdbId = sdbId;
	lrelId = ShdRelIdAllocate(reln->smgr_rnode.node.relNode, sdbId);
	Assert(lrelId != INVALID_REGID);
	meta->lrelId = lrelId;
	grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + lrelId * NUM_FORK + ShdForkGroupIndex(MAIN_FORKNUM);
	for (j = grelId; j < grelId + NUM_FORK; j++)
	{
		/* CleanShdBlkMetas(j); */
		ShdBlkLock(j, LW_EXCLUSIVE);
		slotno = ZeroShdBlkMetaPage(0, j);

		/* Make sure it's written out */
		SimpleLruWritePage(ShdBlkCtl(j), slotno);
		ShdBlkUnlock(j);
	}
}

/*
 * lazy delete shdblk
 */
void
ShdRelDrop(ShdRelMeta meta, bool isRedo)
{
	if (isRedo)
		ShdRelDropInternal(meta);
	else
		RegisterRelMetaUnlink(meta);
}

void
ShdDbGetId(Oid dbOid, int *sdbId)
{
	*sdbId = ShdDbIdLookupInternal(dbOid);

	/*
	 * Invalidate cache immediately after lookup (precise matching + cascade
	 * invalidate relation cache)
	 */
	ShdDbMetaInvalidateCacheIfMatch(dbOid);
}

void
ShdDbCreate(Oid srcOid, Oid dbOid, bool isRedo, int *sdbId)
{
	int			srcSdbId = ShdDbIdLookupInternal(srcOid);

	*sdbId = ShdDbIdAllocate(dbOid, srcOid, srcSdbId, isRedo);

	/* clean up and copy src to dbOid */
	ShdRelMetaCloneFromSrc(srcSdbId, *sdbId);
	ShdBlkMetaCloneFromSrc(srcSdbId, *sdbId);
}

void
ShdDbDrop(int sdbId, bool isRedo)
{
	if (isRedo)
		ShdDbDropInternal(sdbId);
	else
		RegisterDbUnlink(sdbId);

}

int
ShdBlkToggle(RelFileNode node, ForkNumber fork, BlockNumber blk, XLogRecPtr lsn)
{
	int			grelId;
	int			statusNow;
	int			sdbId = ShdDbIdLookupInternal(node.dbNode);
	int			lrelId;

	lrelId = ShdRelIdLookup(node.relNode, sdbId);
	grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + lrelId * NUM_FORK + ShdForkGroupIndex(fork);
	/* ShdBlkMetaExtend(*grelId, blk); */
	statusNow = ShdBlkToggleBit(blk, grelId, lsn);
	return statusNow;
}

void
ShdBlkSetOpp(RelFileNode node, ForkNumber fork, BlockNumber blk, int grelId, int status)
{
#ifdef USE_ASSERT_CHECKING
	ShdAssertValid(node, fork, grelId);
#endif
	ShdBlkSetBit(blk, grelId, (ShdBlkStatus) (status ^ SHD_BLK_BITMASK));
}

void
ShdBlkSet(RelFileNode node, ForkNumber fork, BlockNumber blk, int grelId, int status)
{
#ifdef USE_ASSERT_CHECKING
	ShdAssertValid(node, fork, grelId);
#endif
	ShdBlkSetBit(blk, grelId, (ShdBlkStatus) status);
}

/*
 * performance issu so we no use it for now
 * need cache in reg
 */
ShdBlkStatus
ShdBlkMetaGetStatus(SMgrRelation reln, ForkNumber fork, BlockNumber blk)
{

	int			sdbId = ShdDbIdLookupInternal(reln->smgr_rnode.node.dbNode);
	int			lrelId = ShdRelIdLookup(reln->smgr_rnode.node.relNode, sdbId);
	int			grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + lrelId * NUM_FORK + ShdForkGroupIndex(fork);
	ShdBlkStatus status = ShdBlkMetaGetStatusBit(blk, grelId);

	return status;
}


int
ShdBlkGetOpp(RelFileNode node, ForkNumber fork, BlockNumber blk, int *grelId)
{
	ShdBlkStatus status;
	int			sdbId = ShdDbIdLookupInternal(node.dbNode);
	int			lrelId = ShdRelIdLookup(node.relNode, sdbId);

	*grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + lrelId * NUM_FORK + ShdForkGroupIndex(fork);
	status = ShdBlkMetaGetOppositeBit(blk, *grelId);
	return status;
}

/*
 * we do it alias shdtruncate ,redo also
 * pagetorn is also ok.
 * something wrong, truncateshdblk?
 */
void
ShdBlkMetaTruncate(Oid relOid)
{
	/*
	 * nothing to do , lazy.
	 */
	return;
}

static void
ShdBlkMetaExtendFor(RelFileNode node, ForkNumber forknum, BlockNumber blocknum)
{
	int			sdbId = ShdDbIdLookupInternal(node.dbNode);
	int			lrelId = ShdRelIdLookup(node.relNode, sdbId);
	int			grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + lrelId * NUM_FORK + ShdForkGroupIndex(forknum);

	ShdBlkMetaExtend(grelId, blocknum);
}


static int
BootStrapShdDbIdAllocate(void)
{
	int			pageno = 0;

	Oid			dbOid = SHD_SDBID_TEMPLATE1;
	int			sdbId = SHD_SDBID_TEMPLATE1;
	Oid		   *oidPtr;
	int			slotno;

	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);
	START_CRIT_SECTION();

	slotno = SimpleLruReadPage(ShdDbCtl, pageno, true, (TransactionId) dbOid);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];
	oidPtr[sdbId] = dbOid;
	ShdDbCtl->shared->page_dirty[slotno] = true;
	END_CRIT_SECTION();
	LWLockRelease(ShdDbControlLock);
	return sdbId;
}

/*
 * Reserved sdbId values (do not allocate):
 * 0: shared objects (tablespaces, roles, etc.)
 * 1: template1 database
 * Allocation starts from sdbId 2
 */
static int
ShdDbIdAllocate(Oid dbOid, Oid srcOid, int srcSdbId, bool isRedo)
{
	int			pageno = 0;
	int			sdbId = INVALID_REGID;

	int			i;
	Oid		   *oidPtr;
	int			slotno;

	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);
	START_CRIT_SECTION();

	slotno = SimpleLruReadPage(ShdDbCtl, pageno, true, (TransactionId) dbOid);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];
	for (i = SHD_SDBID_NORMAL; i < MAX_SHD_DBS; i++)
	{
		if (oidPtr[i] == UNUSED)
		{
			if (!isRedo)
			{
				xl_shd_dbmeta xlrec;

				xlrec.sdbId = i;
				xlrec.dbOid = dbOid;
				xlrec.srcDbOid = srcOid;
				xlrec.srcSdbId = srcSdbId;
				XLogBeginInsert();
				XLogRegisterData((char *) (&xlrec), SizeOfShdDbMeta);
				(void) XLogInsert(RM_SHD_ID, XLOG_SHD_DBMETA_ALLOCATE);
			}

			oidPtr[i] = dbOid;
			ShdDbCtl->shared->page_dirty[slotno] = true;
			END_CRIT_SECTION();
			LWLockRelease(ShdDbControlLock);
			ShdDbMetaSetCache(dbOid, i);
			return i;
		}
	}
	END_CRIT_SECTION();
	LWLockRelease(ShdDbControlLock);
#ifdef USE_ASSERT_CHECKING
	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		elog(WARNING, "%s,%d,oidPtr:%d,registerdbOid:%d", __func__, __LINE__, oidPtr[i], dbOid);
	}
#endif
	elog(PANIC, "shadow databases reach max number.");
	return sdbId;
}

static int
ShdDbIdLookupInternal(Oid dbOid)
{
	int			pageno = 0;
	int			sdbId = INVALID_REGID;
	int			i;
	Oid		   *oidPtr;
	int			slotno;

	if (cachedSdbId != -1 && dbOid == cachedDbOid)
	{
		return cachedSdbId;
	}

	slotno = SimpleLruReadPage_ReadOnly(ShdDbCtl, pageno, (TransactionId) dbOid);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];

	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		if (oidPtr[i] == dbOid)
		{
			LWLockRelease(ShdDbControlLock);
			sdbId = i;
			ShdDbMetaSetCache(dbOid, i);
			return sdbId;
		}
	}
	LWLockRelease(ShdDbControlLock);
	Assert(sdbId != INVALID_REGID);
	elog(PANIC, "%s,%d, dbOid:%u must find sdbId", __func__, __LINE__, dbOid);
	return sdbId;
}

static int
ShdRelMaxPerDb(void)
{
	return SHD_MAX_REL_PER_DB;
}
static int
ShdRelBaseOffset(int sdbId)
{
	return sdbId * SHD_MAX_REL_PER_DB;
}
static int
ShdRelSetLocalId(Oid setOid, int sdbId, bool needsWal)
{
	int			pageno = 0;	/* only 1 page hardcoded */
	int			i;
	Oid		   *oidPtr;
	int			slotno;

	ShdRelLock(sdbId, LW_EXCLUSIVE);
	START_CRIT_SECTION();

	slotno = SimpleLruReadPage(ShdRelCtl(sdbId), pageno, true, (TransactionId) 0);
	oidPtr = (Oid *) ShdRelCtl(sdbId)->shared->page_buffer[slotno];

	/*
	 * maybe the mainfork exist but not used. if we find InvalidOid first,
	 * because of find seq , we will never use the relOid we have created
	 * before;
	 */
	for (i = 0; i < ShdRelMaxPerDb(); i++)
	{
		if (oidPtr[i] == UNUSED)
		{
			if (needsWal)
			{
				xl_shd_relmeta xlrec;

				xlrec.sdbId = sdbId;
				xlrec.lrelId = i;
				xlrec.oid = setOid;
				XLogBeginInsert();
				XLogRegisterData((char *) (&xlrec), SizeOfShdRelMeta);
				(void) XLogInsert(RM_SHD_ID, XLOG_SHD_RELMETA_ALLOCATE);
			}
			oidPtr[i] = setOid;
			ShdRelCtl(sdbId)->shared->page_dirty[slotno] = true;
			END_CRIT_SECTION();
			ShdRelUnlock(sdbId);
			return i;
		}
	}
	END_CRIT_SECTION();
	ShdRelUnlock(sdbId);
#ifdef USE_ASSERT_CHECKING
	elog(LOG, "%s,%d,ShdRelMaxPerDb():%d,sdbId:%d", __func__, __LINE__, ShdRelMaxPerDb(), sdbId);
	for (i = 0; i < ShdRelMaxPerDb(); i++)
	{
		elog(LOG, "%s,%d,oidPtr[%d]:%d,sdbId:%d",
			 __func__, __LINE__, i, oidPtr[i], sdbId);
	}
#endif
	return INVALID_REGID;
}

/*
 * we do it before delete file wal, in price we may waste one register position for a while.
 * Returns: lrelId (local relation ID within the database)
 */
static int
ShdRelIdAllocate(Oid relOid, int sdbId)
{
	int			lrelId = INVALID_REGID;

	if (IsBootstrapProcessingMode())
		lrelId = ShdRelSetLocalId(relOid, sdbId, false);
	else
		lrelId = ShdRelSetLocalId(relOid, sdbId, true);
	if (lrelId != INVALID_REGID)
	{
		ShdRelMetaSetCache(relOid, lrelId);
		return lrelId;
	}

	RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

	if (IsBootstrapProcessingMode())
		lrelId = ShdRelSetLocalId(relOid, sdbId, false);
	else
		lrelId = ShdRelSetLocalId(relOid, sdbId, true);

	if (lrelId == INVALID_REGID)
		elog(PANIC, "shd relId reaches max number.");
	ShdRelMetaSetCache(relOid, lrelId);
	return lrelId;
}

#ifdef USE_ASSERT_CHECKING
static void
ShdAssertValid(RelFileNode node, ForkNumber fork, int grelId)
{
	int			sdbId = grelId / (SHD_MAX_REL_PER_DB * NUM_FORK);

	ShdDbAssertValid(node, sdbId);
	ShdRelAssertValid(node, fork, grelId);
}
static Oid
ShdDbIdToOid(int sdbId)
{
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	Oid		   *oidPtr;
	int			slotno;
	Oid			dbOid;

	slotno = SimpleLruReadPage_ReadOnly(ShdDbCtl, pageno, (TransactionId) 0);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];
	dbOid = oidPtr[sdbId];
	LWLockRelease(ShdDbControlLock);
	return dbOid;

}

static void
ShdDbAssertValid(RelFileNode node, int sdbId)
{
	Oid			dbOid = ShdDbIdToOid(sdbId);

	Assert(dbOid == node.dbNode);
}

static Oid
ShdRelIdToOid(int grelId)
{
	int			sdbId = grelId / (SHD_MAX_REL_PER_DB * NUM_FORK);
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	Oid		   *oidPtr;
	int			slotno;
	int			lrelId;
	Oid			oidVal;

	slotno = SimpleLruReadPage_ReadOnly(ShdRelCtl(sdbId), pageno, (TransactionId) 0);
	oidPtr = (Oid *) ShdRelCtl(sdbId)->shared->page_buffer[slotno];
	lrelId = (grelId - ShdRelBaseOffset(sdbId) * NUM_FORK) / NUM_FORK;
	oidVal = oidPtr[lrelId];
	ShdRelUnlock(sdbId);
	return oidVal;
}

static void
ShdRelAssertValid(RelFileNode node, ForkNumber forknum, int grelId)
{
	int			sdbId = grelId / (SHD_MAX_REL_PER_DB * NUM_FORK);
	Oid			relOid;

	/* check fork */
	Assert((grelId - ShdRelBaseOffset(sdbId) * NUM_FORK - forknum / 2) % NUM_FORK == 0);
	/* check reloid */
	relOid = ShdRelIdToOid(grelId);
	Assert(relOid == node.relNode);
}
#endif


void
ShdRelGet(RelFileNode node, ShdRelMeta * meta)
{
	int			sdbId = ShdDbIdLookupInternal(node.dbNode);
	int			lrelId = ShdRelOidToLocalId(node.relNode, sdbId);

	meta->sdbId = sdbId;
	meta->lrelId = lrelId;
	/* Invalidate cache immediately after lookup (precise matching) */
	ShdRelMetaInvalidateCacheIfMatch(node.relNode);
}

/*
 * 1 reg control 3 locks
 * it is not parallel function , so cache is not pretect by lock
 * Returns: lrelId (local relation ID within the database)
 */
int
ShdRelIdLookup(Oid relOid, int sdbId)
{
	int			lrelId;

	if (cachedLrelId != -1 && relOid == cachedRelOid)
	{
		return cachedLrelId;
	}

	lrelId = ShdRelOidToLocalId(relOid, sdbId);
	ShdRelMetaSetCache(relOid, lrelId);
	return lrelId;
}


/*
 * drop database
 */
static void
ShdDbDropInternal(int sdbId)
{
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	Oid		   *oidPtr;
	int			slotno;

	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);

	START_CRIT_SECTION();
	slotno = SimpleLruReadPage(ShdDbCtl, pageno, true, (TransactionId) 0);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];

	oidPtr[sdbId] = UNUSED;

	ShdDbCtl->shared->page_dirty[slotno] = true;
	END_CRIT_SECTION();
	LWLockRelease(ShdDbControlLock);
}

static int
ShdRelOidToLocalId(Oid relOid, int sdbId)
{
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	int			lrelId = INVALID_REGID;
	int			i;
	Oid		   *oidPtr;
	int			slotno;

	slotno = SimpleLruReadPage_ReadOnly(ShdRelCtl(sdbId), pageno, (TransactionId) relOid);
	oidPtr = (Oid *) ShdRelCtl(sdbId)->shared->page_buffer[slotno];

	for (i = 0; i < ShdRelMaxPerDb(); i++)
	{
		if (oidPtr[i] == relOid)
		{
			ShdRelUnlock(sdbId);
			return i;
		}
	}
	ShdRelUnlock(sdbId);
#ifdef USE_ASSERT_CHECKING
	for (i = 0; i < ShdRelMaxPerDb(); i++)
	{
		elog(LOG, "%s,%d,oidPtr[%d]:%d,relOid:%d,sdbId:%d",
			 __func__, __LINE__, i, oidPtr[i], relOid, sdbId);
	}
#endif
	Assert(lrelId != INVALID_REGID);
	elog(PANIC, "%s,%d, Oid:%d must find grelId", __func__, __LINE__, relOid);
	return lrelId;
}

/*
 * we can ensure it atomic with rel delete , so we do it in logical without wal
 */
static void
ShdRelDropInternal(ShdRelMeta meta)
{
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	int			lrelId = meta.lrelId;
	int			sdbId = meta.sdbId;
	Oid		   *oidPtr;
	int			slotno;

	ShdRelLock(sdbId, LW_EXCLUSIVE);
	START_CRIT_SECTION();
	slotno = SimpleLruReadPage(ShdRelCtl(sdbId), pageno, true, (TransactionId) 0);
	oidPtr = (Oid *) ShdRelCtl(sdbId)->shared->page_buffer[slotno];
	oidPtr[lrelId] = UNUSED;
	ShdRelCtl(sdbId)->shared->page_dirty[slotno] = true;

	/*
	 * drop it using vm, so fork is not compare
	 */
	END_CRIT_SECTION();
	ShdRelUnlock(sdbId);
}


ShdBlkStatus
ShdBlkMetaGetStatusBit(BlockNumber blk, int index)
{
	int			pageno = ShdBlkIdToPage(blk);
	int			byteno = ShdBlkIdToByte(blk);
	int			bshift = ShdBlkIdToBIndex(blk) * SHD_BITS_PER_BLK;
	int			slotno;
	char	   *byteptr;
	ShdBlkStatus status;

	slotno = SimpleLruReadPage_ReadOnly(ShdBlkCtl(index), pageno, (TransactionId) blk);
	byteptr = ShdBlkCtl(index)->shared->page_buffer[slotno] + byteno;
	status = (*byteptr >> bshift) & SHD_BLK_BITMASK;
	ShdBlkUnlock(index);

	return status;
}

static void
ShdBlkSetBit(BlockNumber blk, int index, ShdBlkStatus status)
{
	int			pageno = ShdBlkIdToPage(blk);
	int			byteno = ShdBlkIdToByte(blk);
	int			bshift = ShdBlkIdToBIndex(blk) * SHD_BITS_PER_BLK;
	int			slotno;
	char	   *byteptr;

	ShdBlkLock(index, LW_EXCLUSIVE);
	START_CRIT_SECTION();
	slotno = SimpleLruReadPage(ShdBlkCtl(index), pageno, true, (TransactionId) blk);

	byteptr = ShdBlkCtl(index)->shared->page_buffer[slotno] + byteno;
	*byteptr = (*byteptr & ~(SHD_BLK_BITMASK << bshift))
		| ((status & SHD_BLK_BITMASK) << bshift);

	ShdBlkCtl(index)->shared->page_dirty[slotno] = true;
	END_CRIT_SECTION();

	ShdBlkUnlock(index);
}

static ShdBlkStatus
ShdBlkToggleBit(BlockNumber blk, int index, XLogRecPtr lsn)
{
	int			pageno = ShdBlkIdToPage(blk);
	int			byteno = ShdBlkIdToByte(blk);
	int			bshift = ShdBlkIdToBIndex(blk) * SHD_BITS_PER_BLK;
	int			slotno;
	char	   *byteptr;
	ShdBlkStatus status;

	ShdBlkLock(index, LW_EXCLUSIVE);
	START_CRIT_SECTION();
	slotno = SimpleLruReadPage(ShdBlkCtl(index), pageno, true, (TransactionId) blk);

	byteptr = ShdBlkCtl(index)->shared->page_buffer[slotno] + byteno;
	*byteptr = (*byteptr) ^ (SHD_BLK_BITMASK << bshift);
	status = (*byteptr >> bshift) & SHD_BLK_BITMASK;

	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnIndex = GetShdBlkMetaLSNIndex(slotno, blk);

		if (ShdBlkCtl(index)->shared->group_lsn[lsnIndex] < lsn)
			ShdBlkCtl(index)->shared->group_lsn[lsnIndex] = lsn;
	}

	ShdBlkCtl(index)->shared->page_dirty[slotno] = true;
	END_CRIT_SECTION();

	ShdBlkUnlock(index);

	return status;
}

/*
 * only get reversestaus not do
 */
static ShdBlkStatus
ShdBlkMetaGetOppositeBit(BlockNumber blk, int index)
{
	int			pageno = ShdBlkIdToPage(blk);
	int			byteno = ShdBlkIdToByte(blk);
	int			bshift = ShdBlkIdToBIndex(blk) * SHD_BITS_PER_BLK;
	int			slotno;
	char	   *byteptr;
	char		byteVal;
	ShdBlkStatus status;

	ShdBlkLock(index, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(ShdBlkCtl(index), pageno, true, (TransactionId) blk);

	byteptr = ShdBlkCtl(index)->shared->page_buffer[slotno] + byteno;
	byteVal = *byteptr;
	ShdBlkUnlock(index);
	byteVal = byteVal ^ (SHD_BLK_BITMASK << bshift);
	status = (byteVal >> bshift) & SHD_BLK_BITMASK;


	return status;
}

Size
ShdMetaShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, hash_estimate_size((1 + MAX_SHD_DBS + MAX_SHD_REL), sizeof(ShmemIndexEnt)));
	size = add_size(size, SimpleLruShmemSize(NUM_SHDDBS_BUFFERS, 0));
	size = add_size(size, mul_size(MAX_SHD_DBS, SimpleLruShmemSize(NUM_SHD_RELID_BUFFERS, 0)));
	size = add_size(size, mul_size((MAX_SHD_REL), SimpleLruShmemSize(NUM_SHDBLK_BUFFERS, SHD_BLK_LSN_PER_PAGE)));


	return size;
}

void
ShdMetaShmemInit(void)
{
	char		shdRelIdName[SLRU_MAX_NAME_LENGTH];
	char		shdRelIdDir[SLRU_MAX_NAME_LENGTH];
	char		shdBlkName[SLRU_MAX_NAME_LENGTH];
	char		shdBlkDir[SLRU_MAX_NAME_LENGTH];
	int			i;

	ShdDbCtl->PagePrecedes = ShdDbMetaPagePrecedes;

	SimpleLruInit(ShdDbCtl,
				  "shd_db", NUM_SHDDBS_BUFFERS, 0,
				  ShdDbControlLock, "pg_shadow/db",
				  LWTRANCHE_SHDDBS_BUFFERS);
	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		snprintf(shdRelIdName, SLRU_MAX_NAME_LENGTH, "shd_rel_%d", i);
		snprintf(shdRelIdDir, SLRU_MAX_NAME_LENGTH, "pg_shadow/rel/shd_rel_%d", i);
		ShdRelCtl(i)->PagePrecedes = ShdRelMetaPagePrecedes;

		SimpleLruInit(ShdRelCtl(i),
					  shdRelIdName, NUM_SHD_RELID_BUFFERS, 0,
					  ShdRelControlLock(i), shdRelIdDir,
					  LWTRANCHE_SHD_RELID_BUFFERS);
		/* no need test wrap */
		/* SlruPagePrecedesUnitTests(ShdRelCtl, SHD_REG_PER_PAGE); */
	}

	for (i = 0; i < MAX_SHD_REL; i++)
	{
		snprintf(shdBlkName, SLRU_MAX_NAME_LENGTH, "shd_blk_%d", i);
		snprintf(shdBlkDir, SLRU_MAX_NAME_LENGTH, "pg_shadow/block/shd_blk_%d", i);
		ShdBlkCtl(i)->PagePrecedes = ShdBlkMetaPagePrecedes;
		SimpleLruInit(ShdBlkCtl(i),
					  shdBlkName, NUM_SHDBLK_BUFFERS, SHD_BLK_LSN_PER_PAGE,
					  ShdBlkControlLock(i), shdBlkDir,
					  LWTRANCHE_SHDBLK_BUFFERS);
		/* no need test wrap */
		/* SlruPagePrecedesUnitTests(ShdBlkCtl(i), SHD_BLK_PER_PAGE); */
	}

}

/*
 * This func must be called ONCE on system install.
 */
void
BootStrapShdMeta(void)
{
	int			slotno;
	int			i;

	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);

	slotno = ZeroShdDbMetaPage(0);
	SimpleLruWritePage(ShdDbCtl, slotno);
	Assert(!ShdDbCtl->shared->page_dirty[slotno]);

	LWLockRelease(ShdDbControlLock);

	/*
	 * share systable = 0, GLOBALTABLESPACE_OID, database = 0 template1 = 1
	 */
	BootStrapShdDbIdAllocate();


	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		ShdRelLock(i, LW_EXCLUSIVE);

		/* Create and zero the first page of the offsets log */
		slotno = ZeroShdRelMetaPage(0, i);

		/* Make sure it's written out */
		SimpleLruWritePage(ShdRelCtl(i), slotno);
		Assert(!ShdRelCtl(i)->shared->page_dirty[slotno]);

		ShdRelUnlock(i);
	}

	for (i = 0; i < MAX_SHD_REL; i++)
	{
		ShdBlkLock(i, LW_EXCLUSIVE);

		/* Create and zero the first page of the members log */
		slotno = ZeroShdBlkMetaPage(0, i);

		/* Make sure it's written out */
		SimpleLruWritePage(ShdBlkCtl(i), slotno);
		Assert(!ShdBlkCtl(i)->shared->page_dirty[slotno]);

		ShdBlkUnlock(i);
	}
}


static int
ZeroShdDbMetaPage(int pageno)
{
	int			slotno;

	slotno = SimpleLruZeroPage(ShdDbCtl, pageno);

	return slotno;
}

/*
 * we do not writexlog for zero , we just redo it in bootstrap for now
 * one page one lock, one page is one database
 */
static int
ZeroShdRelMetaPage(int pageno, int sdbId)
{
	int			slotno;

	Assert(sdbId < MAX_SHD_DBS && sdbId >= 0);
	slotno = SimpleLruZeroPage(ShdRelCtl(sdbId), pageno);

	return slotno;
}

/*
 * we do not writexlog for zero, we just redo it in extendshd
 */
static int
ZeroShdBlkMetaPage(int pageno, int grelId)
{
	int			slotno;

	Assert(grelId < MAX_SHD_REL && grelId >= 0);
	slotno = SimpleLruZeroPage(ShdBlkCtl(grelId), pageno);

	return slotno;
}


static void
SetRelLastPageNo(int sdbId)
{
	char		path[MAXPGPATH];
	int			fd;
	int			latestSeg = -1;
	int			latestPage;
	int			segno;

	SlruScanDirectory(ShdRelCtl(sdbId), ShdSlruScanFindLatest, &latestSeg);
	segno = latestSeg / SLRU_PAGES_PER_SEGMENT;
	snprintf(path, MAXPGPATH, "%s/%04X", ShdRelCtl(sdbId)->Dir, segno);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd > 0)
	{
		int			pages_in_seg;
		off_t		size = lseek(fd, 0, SEEK_END);

		/* Get the file size in bytes */
		CloseTransientFile(fd);

		/* Calculate the number of 8 KB pages in this specific file */
		pages_in_seg = size / BLCKSZ;

		/* Calculate the final, absolute latest page number */
		latestPage = latestSeg + pages_in_seg - 1;
	}
	else
	{
		elog(ERROR, "file %s is not found", ShdRelCtl(sdbId)->Dir);
	}
	ShdRelLock(sdbId, LW_EXCLUSIVE);
	ShdRelCtl(sdbId)->shared->latest_page_number = latestPage;
	ShdRelUnlock(sdbId);
}

static void
SetBlkLastPageNo(int grelId)
{
	char		path[MAXPGPATH];
	int			fd;
	int			latestSeg = -1;
	int			latestPage;
	int			segno;

	SlruScanDirectory(ShdBlkCtl(grelId), ShdSlruScanFindLatest, &latestSeg);
	segno = latestSeg / SLRU_PAGES_PER_SEGMENT;
	snprintf(path, MAXPGPATH, "%s/%04X", ShdBlkCtl(grelId)->Dir, segno);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd > 0)
	{
		int			pages_in_seg;
		off_t		size = lseek(fd, 0, SEEK_END);

		/* Get the file size in bytes */
		CloseTransientFile(fd);

		/* Calculate the number of 8 KB pages in this specific file */
		pages_in_seg = size / BLCKSZ;

		latestPage = latestSeg + pages_in_seg - 1;
	}
	else
	{
		elog(ERROR, "file %s is not found", ShdBlkCtl(grelId)->Dir);
	}
	ShdBlkLock(grelId, LW_EXCLUSIVE);
	ShdBlkCtl(grelId)->shared->latest_page_number = latestPage;
	ShdBlkUnlock(grelId);
}

static void
SetDbLastPageNo()
{
	char		path[MAXPGPATH];
	int			fd;
	int			latestSeg = 0;
	int			latestPage;
	int			segno;

	SlruScanDirectory(ShdDbCtl, ShdSlruScanFindLatest, &latestSeg);
	segno = latestSeg / SLRU_PAGES_PER_SEGMENT;
	Assert(latestSeg >= 0);
	snprintf(path, MAXPGPATH, "%s/%04X", ShdDbCtl->Dir, segno);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd >= 0)
	{
		int			pages_in_seg;
		off_t		size = lseek(fd, 0, SEEK_END);

		/* Get the file size in bytes */
		CloseTransientFile(fd);

		/* Calculate the number of 8 KB pages in this specific file */
		pages_in_seg = size / BLCKSZ;

		/* Calculate the final, absolute latest page number */
		latestPage = latestSeg + pages_in_seg - 1;
	}
	else
	{
		elog(ERROR, "file %s is not found", ShdDbCtl->Dir);
	}
	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);
	ShdDbCtl->shared->latest_page_number = latestPage;
	LWLockRelease(ShdDbControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup.
 */
void
StartupShdMeta(void)
{
	int			i;

	SetDbLastPageNo();

	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		SetRelLastPageNo(i);
	}

	for (i = 0; i < MAX_SHD_REL; i++)
	{
		SetBlkLastPageNo(i);
	}
}



/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownShdMeta(void)
{
	int			i;

	SimpleLruFlush(ShdDbCtl, false);

	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		SimpleLruFlush(ShdRelCtl(i), false);
	}

	for (i = 0; i < MAX_SHD_REL; i++)
	{
		SimpleLruFlush(ShdBlkCtl(i), false);
	}
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointShdMeta(void)
{
	int			i;

	/* ReuseShdDbs(); */
	SimpleLruFlush(ShdDbCtl, true);

	for (i = 0; i < MAX_SHD_DBS; i++)
	{
		/* ReuseShdReg(i); */
		SimpleLruFlush(ShdRelCtl(i), true);
	}

	for (i = 0; i < MAX_SHD_REL; i++)
	{
		SimpleLruFlush(ShdBlkCtl(i), true);
	}
	fsync_fname("pg_shadow", true);
}

static void
ShdBlkMetaExtend(int grelId, BlockNumber blk)
{
	int			pageno;

	/* int latestPageno; */
	if (ShdBlkIdToPgIndex(blk) != 0 || blk == 0)
		return;

	pageno = ShdBlkIdToPage(blk);

	/*
	 * latestPageno = FindShdBlkLastPageno(grelId); if (pageno <=
	 * latestPageno) return;
	 */
	ShdBlkLock(grelId, LW_EXCLUSIVE);
	ZeroShdBlkMetaPage(pageno, grelId);
	ShdBlkUnlock(grelId);
}


/*
 * SlruScanDirectory callback
 *		This callback determines the earliest existing page number.
 */
static bool
ShdSlruScanFindLatest(SlruCtl ctl, char *filename, int segpage, void *data)
{
	int		   *tmpPage = (int *) data;

	if (*tmpPage == -1 ||
		ctl->PagePrecedes(*tmpPage, segpage))
	{
		*(int *) data = segpage;
	}

	return false;				/* keep going */
}


/*
 * lazy
 */
static void
ShdRelMetaCloneFromSrc(int srcId, int dstId)
{
	/* CleanShdRegMetas(dstId); */
	ShdRelMetaCopyForDb(srcId, dstId);
}

static void
ShdRelMetaCopyForDb(int srcSdbId, int dstSdbId)
{
	int			src_slot;
	int			dest_slot;
	char	   *src_buffer;
	char	   *dest_buffer;
	int			src_lpn;
	int			pageno;

	ShdRelLock(srcSdbId, LW_SHARED);
	src_lpn = ShdRelCtl(srcSdbId)->shared->latest_page_number;
	ShdRelUnlock(srcSdbId);
	for (pageno = 0; pageno <= src_lpn; pageno++)
	{
		src_slot = SimpleLruReadPage_ReadOnly(ShdRelCtl(srcSdbId), pageno, InvalidTransactionId);
		src_buffer = ShdRelCtl(srcSdbId)->shared->page_buffer[src_slot];
		ShdRelLock(dstSdbId, LW_EXCLUSIVE);
		dest_slot = SimpleLruZeroPage(ShdRelCtl(dstSdbId), pageno);
		dest_buffer = ShdRelCtl(dstSdbId)->shared->page_buffer[dest_slot];
		memcpy(dest_buffer, src_buffer, BLCKSZ);
		ShdRelUnlock(srcSdbId);
		ShdRelUnlock(dstSdbId);
	}
	SimpleLruFlush(ShdRelCtl(dstSdbId), true);
}

static void
ShdBlkMetaCloneFromSrc(int sdbSrcId, int sdbDstId)
{
	int			i;
	int			j = 0;

	for (i = ShdRelBaseOffset(sdbDstId) * NUM_FORK; i < ShdRelBaseOffset(sdbDstId + 1) * NUM_FORK; i++, j++)
	{
		int			srcIndex = ShdRelBaseOffset(sdbSrcId) * NUM_FORK + j;

		/* CleanShdBlkMetas(i); */
		ShdBlkMetaCopyForRel(srcIndex, i);
	}
}

static void
ShdBlkMetaCopyForRel(int srcRelId, int dstRelId)
{
	int			src_slot;
	int			dest_slot;
	char	   *src_buffer;
	char	   *dest_buffer;
	int			src_lpn;
	int			pageno;

	ShdBlkLock(srcRelId, LW_SHARED);
	src_lpn = ShdBlkCtl(srcRelId)->shared->latest_page_number;
	ShdBlkUnlock(srcRelId);
	for (pageno = 0; pageno <= src_lpn; pageno++)
	{
		src_slot = SimpleLruReadPage_ReadOnly(ShdBlkCtl(srcRelId), pageno, InvalidTransactionId);
		src_buffer = ShdBlkCtl(srcRelId)->shared->page_buffer[src_slot];
		ShdBlkLock(dstRelId, LW_EXCLUSIVE);
		dest_slot = SimpleLruZeroPage(ShdBlkCtl(dstRelId), pageno);
		dest_buffer = ShdBlkCtl(dstRelId)->shared->page_buffer[dest_slot];
		memcpy(dest_buffer, src_buffer, BLCKSZ);
		ShdBlkUnlock(srcRelId);
		ShdBlkUnlock(dstRelId);
	}
	SimpleLruFlush(ShdBlkCtl(dstRelId), true);
}

static bool
ShdDbMetaPagePrecedes(int page1, int page2)
{
	return page1 < page2;
}

static bool
ShdRelMetaPagePrecedes(int page1, int page2)
{
	return page1 < page2;
}

static bool
ShdBlkMetaPagePrecedes(int page1, int page2)
{
	return page1 < page2;
}

static void
ShdDbCreateRedo(xl_shd_dbmeta *xlrec)
{
	int			pageno = 0;	/* for now hardcoded to 0, only 1 page */
	int			sdbId = xlrec->sdbId;
	Oid			dbOid = xlrec->dbOid;
	int			srcId = xlrec->srcSdbId;
	Oid		   *oidPtr;
	int			slotno;

	LWLockAcquire(ShdDbControlLock, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(ShdDbCtl, pageno, true, (TransactionId) dbOid);
	oidPtr = (Oid *) ShdDbCtl->shared->page_buffer[slotno];
	oidPtr[sdbId] = dbOid;
	ShdDbCtl->shared->page_dirty[slotno] = true;
	LWLockRelease(ShdDbControlLock);
	ShdRelMetaCloneFromSrc(srcId, sdbId);
	ShdBlkMetaCloneFromSrc(srcId, sdbId);
}

static void
ShdRelCreateRedo(xl_shd_relmeta *xlrec)
{
	int			sdbId = xlrec->sdbId;
	Oid			relOid = xlrec->oid;
	int			i = xlrec->lrelId;
	int			slotno;
	Oid		   *oidPtr;
	int			grelId = ShdRelBaseOffset(sdbId) * NUM_FORK + i * NUM_FORK + ShdForkGroupIndex(MAIN_FORKNUM);
	int			pageno = 0;	/* hardcode for now */
	int			j;

	Assert(i >= 0 && i < ShdRelMaxPerDb());
	ShdRelLock(sdbId, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(ShdRelCtl(sdbId), pageno, true, (TransactionId) relOid);
	oidPtr = (Oid *) ShdRelCtl(sdbId)->shared->page_buffer[slotno];
	ShdRelCtl(sdbId)->shared->page_dirty[slotno] = true;
	oidPtr[i] = relOid;
	ShdRelUnlock(sdbId);
	for (j = grelId; j < grelId + NUM_FORK; j++)
	{
		/* CleanShdBlkMetas(j); */
		ShdBlkLock(j, LW_EXCLUSIVE);
		slotno = ZeroShdBlkMetaPage(0, j);

		/* Make sure it's written out */
		SimpleLruWritePage(ShdBlkCtl(j), slotno);
		ShdBlkUnlock(j);
	}
}

void
shd_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SHD_RELMETA_ALLOCATE)
	{
		xl_shd_relmeta *xlrec = (xl_shd_relmeta *) XLogRecGetData(record);

		ShdRelCreateRedo(xlrec);
	}
	else if (info == XLOG_SHD_DBMETA_ALLOCATE)
	{
		xl_shd_dbmeta *xlrec = (xl_shd_dbmeta *) XLogRecGetData(record);

		ShdDbCreateRedo(xlrec);
	}
	else
		elog(PANIC, "shd_redo: unknown op code %u", info);
}
