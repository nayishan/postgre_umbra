/*-------------------------------------------------------------------------
 *
 * shadow.h
 *	  shadow storage manager public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shadow.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHADOW_H
#define SHADOW_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/* shd storage manager functionality */
extern void shdinit(void);
extern void shdclose(SMgrRelation reln, ForkNumber forknum);
extern void shdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool shdexists(SMgrRelation reln, ForkNumber forknum);
extern void shdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void shdextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void shdprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void shdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void shdwrite(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void shdwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber shdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void shdtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
extern void shdimmedsync(SMgrRelation reln, ForkNumber forknum);

extern void ShdForgetDatabaseSyncRequests(Oid dbid);
extern void ShdDropRelationMetas(ShdRelMeta *delrels, int ndelrels, bool isRedo);

/* shd sync callbacks */
extern int	shdsyncfiletag(const FileTag *ftag, char *path);
extern int	shdunlinkfiletag(const FileTag *ftag, char *path);
extern bool shdfiletagmatches(const FileTag *ftag, const FileTag *candidate);
extern void shd_unlink_relmeta(const FileTag *faker);
extern void shd_unlink_dbmeta(const FileTag *faker);

/* shadow metadata lifecycle functions */
extern void BootStrapShdMeta(void);
extern void ShdMetaShmemInit(void);
extern Size ShdMetaShmemSize(void);
extern void StartupShdMeta(void);
extern void ShutdownShdMeta(void);
extern void CheckPointShdMeta(void);

/* WAL redo */
extern void shd_redo(XLogReaderState *record);
extern void shd_desc(StringInfo buf, XLogReaderState *record);
extern const char *shd_identify(uint8 info);

/* Block metadata status operations */
extern int ShdBlkToggle(RelFileNode node, ForkNumber fork, BlockNumber blk, XLogRecPtr lsn);
extern int ShdBlkGetOpp(RelFileNode node, ForkNumber fork, BlockNumber blk, int *grelId);
extern void ShdBlkSet(RelFileNode node, ForkNumber fork, BlockNumber blk, int grelId, int status);
extern void ShdBlkSetOpp(RelFileNode node, ForkNumber fork, BlockNumber blk, int grelId, int status);

/* Database metadata operations */
extern void ShdDbGetId(Oid dbOid, int *sdbId);
extern void ShdDbCreate(Oid srcOid, Oid dbOid, bool isRedo, int *sdbId);
extern void ShdDbDrop(int sdbId, bool isRedo);

/* Relation metadata operations */
extern void ShdRelGet(RelFileNode node, ShdRelMeta *meta);
extern void ShdRelCreate(SMgrRelation reln, ShdRelMeta *meta);
extern void ShdRelDrop(ShdRelMeta meta, bool isRedo);


#define XLOG_SHD_RELMETA_ALLOCATE	  0x00
#define XLOG_SHD_DBMETA_ALLOCATE	  0x10

typedef enum ShdBlkStatus
{
	PING,
	PONG
}ShdBlkStatus;
typedef struct xl_shd_relmeta
{
	int			sdbId;
	int 		lrelId;		/* Local Relation ID - 用于 RelMeta 页内索引 */
	Oid			oid;
} xl_shd_relmeta;
#define SizeOfShdRelMeta (sizeof(xl_shd_relmeta))

typedef struct xl_shd_dbmeta
{
	int 		sdbId;
	Oid			dbOid;
	int			srcSdbId;
	Oid			srcDbOid;
}xl_shd_dbmeta;
#define SizeOfShdDbMeta (sizeof(xl_shd_dbmeta))

typedef struct xl_shd_blkmeta
{
	int 			grelId;		/* Global Relation ID - 用于 BlkMeta 全局索引 */
	ShdBlkStatus	status;
}xl_shd_blkmeta;
#define SizeOfShdBlkMeta		(sizeof(xl_shd_blkmeta))

#endif							/* SHADOW_H */
