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

/* A raw pin held from WAL preparation through selector publication. */
typedef struct MapSlotShift
{
	RelFileLocatorBackend rlocator;
	BlockNumber logical_block;
	uint8		source_slot;
	uint8		target_slot;
	int			map_slot_id;
	bool		prepared;
} MapSlotShift;

/* Block zero is the resident root; selector pages begin at block one. */
#define UMBRA_MAP_SELECTOR_FIRST_BLOCK 1
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
extern void MapEnsureActiveSlotPages(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 BlockNumber first_block, BlockNumber nblocks,
							 bool skipFsync);
extern bool MapPrepareSlotShift(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator,
								 BlockNumber logical_block, MapSlotShift *shift);
extern void MapAbortSlotShift(MapSlotShift *shift);
extern void MapPublishSlotShift(MapSlotShift *shift, XLogRecPtr lsn);
extern void MapRedoSetActiveSlot(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 BlockNumber logical_block, uint8 active_slot);
extern void MapRedoSlotShift(UmbraFileContext *ctx,
					 RelFileLocatorBackend rlocator,
					 BlockNumber logical_block, uint8 source_slot,
					 uint8 target_slot);

#endif                          /* UMBRA_MAP_H */
