/*-------------------------------------------------------------------------
 *
 * map.c
 *	  Umbra metadata-fork disk layout helpers.
 *
 * This file contains address-translation and in-page access routines for the
 * metadata fork disk layout.
 *
 * src/backend/storage/map/map.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/um_defs.h"

void
MapPageInit(MapPage *page)
{
	Assert(page != NULL);

	MemSet(page->pblknos, 0xFF, sizeof(page->pblknos));
}

BlockNumber
MapPageGetEntry(const MapPage *page, int entry_idx)
{
	Assert(page != NULL);

	if (entry_idx < 0 || entry_idx >= MAP_ENTRIES_PER_PAGE)
		elog(ERROR, "map entry index %d is out of range", entry_idx);

	return page->pblknos[entry_idx];
}

void
MapPageSetEntry(MapPage *page, int entry_idx, BlockNumber pblkno)
{
	Assert(page != NULL);

	if (entry_idx < 0 || entry_idx >= MAP_ENTRIES_PER_PAGE)
		elog(ERROR, "map entry index %d is out of range", entry_idx);

	page->pblknos[entry_idx] = pblkno;
}

BlockNumber
MapForkPageIndexToMapBlkno(ForkNumber forknum, BlockNumber fork_page_idx)
{
	uint64		group_no;
	uint64		blkno64;

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "Umbra metadata fork cannot be addressed as a map target");

	switch (forknum)
	{
		case FSM_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
		{
			uint64		group_page_idx = (uint64) fork_page_idx;

			group_no = group_page_idx / (uint64) MAP_GROUP_MAIN_PAGES;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES +
				(uint64) MAP_GROUP_VM_PAGES +
				(group_page_idx % (uint64) MAP_GROUP_MAIN_PAGES);
			break;
		}

		default:
			elog(ERROR, "unsupported fork number %d in map layout", (int) forknum);
			pg_unreachable();
	}

	if (blkno64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address map page %u for fork %d",
						fork_page_idx, forknum)));

	return (BlockNumber) blkno64;
}

BlockNumber
MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno)
{
	BlockNumber	fork_page_idx;
	uint64		entry64;

	fork_page_idx = lblkno / MAP_ENTRIES_PER_PAGE;
	entry64 = (uint64) MapForkPageIndexToMapBlkno(forknum, fork_page_idx) *
		(uint64) MAP_ENTRIES_PER_PAGE +
		(uint64) (lblkno % MAP_ENTRIES_PER_PAGE);

	if (entry64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address logical block %u for fork %d in map",
						lblkno, forknum)));

	return (BlockNumber) entry64;
}

bool
MapDecodeMapBlkno(BlockNumber map_blkno, ForkNumber *forknum,
				  BlockNumber *fork_page_idx)
{
	uint64		offset;
	uint64		group_no;
	uint64		in_group;

	Assert(forknum != NULL);
	Assert(fork_page_idx != NULL);

	if (map_blkno == MAP_BLOCK_SUPER || map_blkno < MAP_BLOCK_FIRST_GROUP)
		return false;

	offset = (uint64) (map_blkno - MAP_BLOCK_FIRST_GROUP);
	group_no = offset / (uint64) MAP_GROUP_TOTAL_PAGES;
	in_group = offset % (uint64) MAP_GROUP_TOTAL_PAGES;

	if (in_group < (uint64) MAP_GROUP_FSM_PAGES)
	{
		*forknum = FSM_FORKNUM;
		*fork_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) MAP_GROUP_FSM_PAGES;
	if (in_group < (uint64) MAP_GROUP_VM_PAGES)
	{
		*forknum = VISIBILITYMAP_FORKNUM;
		*fork_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) MAP_GROUP_VM_PAGES;
	if (in_group < (uint64) MAP_GROUP_MAIN_PAGES)
	{
		*forknum = MAIN_FORKNUM;
		*fork_page_idx = (BlockNumber)
			(group_no * (uint64) MAP_GROUP_MAIN_PAGES + in_group);
		return true;
	}

	return false;
}
