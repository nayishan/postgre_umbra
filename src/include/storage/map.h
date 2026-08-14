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

/*
 * Block zero is the resident root. Each following 258-page group contains
 * one FSM selector, one VM selector, and 256 MAIN selectors.
 */
#define UMBRA_MAP_SELECTOR_FIRST_BLOCK 1
#define UMBRA_MAP_SELECTOR_BITS 2
#define UMBRA_MAP_SELECTOR_ENTRIES_PER_PAGE \
	((BLCKSZ * BITS_PER_BYTE) / UMBRA_MAP_SELECTOR_BITS)
#define UMBRA_MAP_SELECTOR_FSM_PAGES_PER_GROUP 1
#define UMBRA_MAP_SELECTOR_VM_PAGES_PER_GROUP 1
#define UMBRA_MAP_SELECTOR_MAIN_PAGES_PER_GROUP 256
#define UMBRA_MAP_SELECTOR_GROUP_PAGES \
	(UMBRA_MAP_SELECTOR_FSM_PAGES_PER_GROUP + \
	 UMBRA_MAP_SELECTOR_VM_PAGES_PER_GROUP + \
	 UMBRA_MAP_SELECTOR_MAIN_PAGES_PER_GROUP)

extern void MapInvalidateRelation(RelFileLocatorBackend rlocator);
extern void MapInvalidateDatabase(Oid dbid);
extern void MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapFlushRelation(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator);
extern void MapFlushDatabaseTablespace(Oid dbid, Oid spcOid);
extern void MapCheckpoint(void);
extern int MapPageBgWriterFlush(int max_pages);

extern uint8 MapGetActiveSlot(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator,
						  ForkNumber forknum,
						  BlockNumber logical_block);
extern void MapEnsureActiveSlotPages(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 ForkNumber forknum,
							 BlockNumber first_block,
							 BlockNumber nblocks,
							 bool skipFsync);
extern void MapPublishSlotShift(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator,
								ForkNumber forknum,
								BlockNumber logical_block, uint8 source_slot,
								uint8 target_slot, XLogRecPtr lsn);
extern void MapRedoSetActiveSlot(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator,
								 ForkNumber forknum,
								 BlockNumber logical_block, uint8 active_slot);
extern void MapRedoSlotShift(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 ForkNumber forknum,
						 BlockNumber logical_block, uint8 source_slot,
						 uint8 target_slot);

#endif                          /* UMBRA_MAP_H */
