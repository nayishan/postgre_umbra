/*-------------------------------------------------------------------------
 *
 * map.h
 *	  Umbra MAP page buffer pool declarations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/storage/map.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_H
#define MAP_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"

typedef struct UmbraFileContext UmbraFileContext;
typedef struct MapPageDesc MapPageDesc;
typedef struct MapSuperDesc MapSuperDesc;

typedef struct MapPageBuffer
{
	MapPageDesc *desc;
} MapPageBuffer;

typedef struct MapSuperBuffer
{
	MapSuperDesc *desc;
} MapSuperBuffer;

extern void MapInvalidateRelation(RelFileLocatorBackend rlocator);
extern void MapInvalidateDatabase(Oid dbid);
extern void MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapFlushRelation(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
extern void MapFlushDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapCheckpoint(void);
extern void MapReclaimBackendInit(void);
extern void MapReclaimCheckpointStart(void);
extern void MapReclaimCheckpointComplete(void);
extern int MapPageBgWriterFlush(int max_pages);
extern uint32 MapPageTakeRecentAllocations(void);
extern void MapStrategyNotifyWriter(int mapwriter_procno);
extern void MapWakeWriter(void);
extern void MapStrategyNotifyCompactor(int mapcompactor_procno);
extern void MapWakeCompactor(void);
extern int MapPreallocStep(int max_relations);
extern int MapReclaimStep(int max_tasks);

extern PGDLLIMPORT bool map_compactor_enable;
extern PGDLLIMPORT int map_compactor_extent_blocks;
extern PGDLLIMPORT int map_compactor_low_live_percent;
extern PGDLLIMPORT int map_compactor_max_moves;
extern int MapCompactorStep(int max_relations);

extern MapPageBuffer MapPageBufferRead(UmbraFileContext *ctx,
									   RelFileLocatorBackend rlocator,
									   BlockNumber map_blkno,
									   bool extend, bool skipFsync,
									   LWLockMode mode);
extern MapPageBuffer MapPageBufferReadBlocking(UmbraFileContext *ctx,
										   RelFileLocatorBackend rlocator,
										   BlockNumber map_blkno,
										   LWLockMode mode);
extern char *MapPageBufferGetData(MapPageBuffer buffer);
extern void MapPageMarkBufferDirty(MapPageBuffer buffer, bool skipFsync,
								   XLogRecPtr wal_flush_lsn);
extern void MapPageReleaseBuffer(MapPageBuffer buffer);

extern MapSuperBuffer MapSuperBufferRead(UmbraFileContext *ctx,
										 RelFileLocatorBackend rlocator,
										 LWLockMode mode);
extern char *MapSuperBufferGetData(MapSuperBuffer buffer);
extern void MapSuperMarkBufferDirty(MapSuperBuffer buffer, bool skipFsync,
									XLogRecPtr wal_flush_lsn);
extern void MapSuperReleaseBuffer(MapSuperBuffer buffer);

#endif							/* MAP_H */
