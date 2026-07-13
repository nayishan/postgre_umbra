/*-------------------------------------------------------------------------
 *
 * map.h
 *	  Umbra MAP metadata cache declarations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/storage/map.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_H
#define MAP_H

#include "storage/lwlock.h"
#include "storage/relfilelocator.h"

typedef struct UmbraFileContext UmbraFileContext;
typedef struct MapSuperDesc MapSuperDesc;

#define MAP_BLOCK_SUPER 0

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

extern MapSuperBuffer MapSuperBufferRead(UmbraFileContext *ctx,
										 RelFileLocatorBackend rlocator,
										 LWLockMode mode);
extern char *MapSuperBufferGetData(MapSuperBuffer buffer);
extern void MapSuperMarkBufferDirty(MapSuperBuffer buffer);
extern void MapSuperReleaseBuffer(MapSuperBuffer buffer);

#endif							/* MAP_H */
