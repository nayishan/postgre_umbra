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

#include "storage/block.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"

typedef struct UmbraFileContext UmbraFileContext;
typedef struct MapPageDesc MapPageDesc;

typedef struct MapPageBuffer
{
	MapPageDesc *desc;
} MapPageBuffer;

extern void MapInvalidateRelation(RelFileLocatorBackend rlocator);
extern void MapInvalidateDatabase(Oid dbid);
extern void MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapFlushRelation(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
extern void MapFlushDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapCheckpoint(void);

extern MapPageBuffer MapPageBufferRead(UmbraFileContext *ctx,
									   RelFileLocatorBackend rlocator,
									   BlockNumber map_blkno,
									   bool extend, bool skipFsync,
									   LWLockMode mode);
extern char *MapPageBufferGetData(MapPageBuffer buffer);
extern void MapPageMarkBufferDirty(MapPageBuffer buffer, bool skipFsync);
extern void MapPageReleaseBuffer(MapPageBuffer buffer);

#endif							/* MAP_H */
