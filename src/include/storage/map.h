/*-------------------------------------------------------------------------
 *
 * map.h
 *    Umbra active-slot metadata page cache declarations.
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMBRA_MAP_H
#define UMBRA_MAP_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"

typedef struct UmbraFileContext UmbraFileContext;

/* The metadata fork contains selector pages only; its first page is block 0. */
#define UMBRA_MAP_SELECTOR_FIRST_BLOCK 0
#define UMBRA_MAP_SELECTOR_BITS 2
#define UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE \
	((BLCKSZ * BITS_PER_BYTE) / UMBRA_MAP_SELECTOR_BITS)

extern void MapInvalidateRelation(RelFileLocatorBackend rlocator);
extern void MapInvalidateDatabase(Oid dbid);
extern void MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapFlushRelation(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator);
extern void MapFlushDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapCheckpoint(void);

extern uint8 MapGetActiveSlot(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator,
						  BlockNumber logical_block);

#endif                          /* UMBRA_MAP_H */
