/*-------------------------------------------------------------------------
 *
 * map.h
 *	  Umbra metadata-fork disk layout helpers.
 *
 * This header defines the stable on-disk page layout and address translation
 * helpers for Umbra's relation-local metadata file.
 *
 * src/include/storage/map.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_H
#define MAP_H

#include "storage/block.h"
#include "storage/relfilelocator.h"

#define MAP_ENTRIES_PER_PAGE (BLCKSZ / sizeof(uint32))

/*
 * Umbra metadata file page layout:
 * - block 0: superblock payload
 * - blocks 1..: repeated proportional groups
 *
 * Each group reserves one FSM map page, one VM map page, and 8192 MAIN map
 * pages. That keeps the mapping formula stable while leaving room for the
 * auxiliary forks to grow alongside MAIN.
 */
#define MAP_BLOCK_SUPER		0
#define MAP_BLOCK_FIRST_GROUP	1
#define MAP_GROUP_FSM_PAGES	1
#define MAP_GROUP_VM_PAGES	1
#define MAP_GROUP_MAIN_PAGES	8192
#define MAP_GROUP_TOTAL_PAGES \
	(MAP_GROUP_FSM_PAGES + MAP_GROUP_VM_PAGES + MAP_GROUP_MAIN_PAGES)

typedef struct MapPage
{
	uint32		pblknos[MAP_ENTRIES_PER_PAGE];
} MapPage;

extern void MapPageInit(MapPage *page);
extern BlockNumber MapPageGetEntry(const MapPage *page, int entry_idx);
extern void MapPageSetEntry(MapPage *page, int entry_idx, BlockNumber pblkno);

extern BlockNumber MapForkPageIndexToMapBlkno(ForkNumber forknum,
											  BlockNumber fork_page_idx);
extern BlockNumber MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno);
extern bool MapDecodeMapBlkno(BlockNumber map_blkno, ForkNumber *forknum,
							  BlockNumber *fork_page_idx);

#endif							/* MAP_H */
