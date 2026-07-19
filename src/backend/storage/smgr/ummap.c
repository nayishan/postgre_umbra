/*-------------------------------------------------------------------------
 *
 * ummap.c
 *	  Umbra private map fork container.
 *
 * This file owns the private relation-local map fork used by Umbra.  The fork
 * currently stores a simple identity logical-to-physical map without a
 * superblock.  Later MAP patches can add remap, cache, and superblock behavior
 * without moving that work into umbra.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/ummap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>

#include "access/xlogutils.h"
#include "common/relpath.h"
#include "storage/map.h"
#include "storage/umfile.h"
#include "storage/ummap.h"

#define UMMAP_ENTRIES_PER_PAGE	(BLCKSZ / sizeof(BlockNumber))

/*
 * Fixed no-superblock map layout, following the rebase branch's proportional
 * grouping but starting at map block 0:
 *
 *	[FSM map page][VM map page][8192 MAIN map pages]
 */
#define UMMAP_GROUP_FSM_PAGES	1
#define UMMAP_GROUP_VM_PAGES	1
#define UMMAP_GROUP_MAIN_PAGES	8192
#define UMMAP_GROUP_TOTAL_PAGES \
	(UMMAP_GROUP_FSM_PAGES + UMMAP_GROUP_VM_PAGES + UMMAP_GROUP_MAIN_PAGES)

StaticAssertDecl((BLCKSZ % sizeof(BlockNumber)) == 0,
				 "BLCKSZ must be a multiple of BlockNumber");

static BlockNumber ummap_fork_page_index_to_map_blkno(ForkNumber forknum,
													  BlockNumber fork_page_idx);
static BlockNumber ummap_map_blkno(ForkNumber forknum, BlockNumber lblkno);
static int	ummap_entry_index(BlockNumber lblkno);
static BlockNumber ummap_page_run_limit(BlockNumber lblkno,
										BlockNumber maxblocks);
static BlockNumber ummap_page_get_entry(char *page, int entry_idx);
static void ummap_page_set_entry(char *page, int entry_idx,
								 BlockNumber pblkno);

RelPathStr
ummap_relpath(RelFileLocatorBackend rlocator)
{
	RelPathStr	base;
	RelPathStr	path;

	base = relpath(rlocator, MAIN_FORKNUM);
	snprintf(path.str, sizeof(path.str), "%s_map", base.str);
	return path;
}

bool
ummap_exists(UmbraFileContext *ctx)
{
	return umfile_exists(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_create(UmbraFileContext *ctx, bool isRedo)
{
	umfile_create(ctx, UMBRA_MAP_FORKNUM, isRedo);
}

void
ummap_close(UmbraFileContext *ctx)
{
	umfile_close(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_immedsync_if_exists(UmbraFileContext *ctx)
{
	if (umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		umfile_immedsync(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_registersync_if_exists(UmbraFileContext *ctx)
{
	if (umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		umfile_registersync(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	MapInvalidateRelation(rlocator);
	umfile_unlink(rlocator, UMBRA_MAP_FORKNUM, isRedo);
}

BlockNumber
ummap_lookup_block(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				   ForkNumber forknum,
				   BlockNumber lblkno)
{
	BlockNumber pblkno;

	(void) ummap_lookup_run(ctx, rlocator, forknum, lblkno, 1, &pblkno);
	return pblkno;
}

BlockNumber
ummap_lookup_run(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				 ForkNumber forknum,
				 BlockNumber lblkno, BlockNumber maxblocks,
				 BlockNumber *pblkno)
{
	BlockNumber map_nblocks;
	BlockNumber run_blocks = 0;

	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	if (!ummap_tracks_fork(forknum))
	{
		*pblkno = lblkno;
		return maxblocks;
	}

	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d block %u",
			 (int) forknum, lblkno);

	map_nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	while (run_blocks < maxblocks)
	{
		BlockNumber page_lblkno = lblkno + run_blocks;
		BlockNumber map_blkno;
		BlockNumber page_limit;
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx;

		map_blkno = ummap_map_blkno(forknum, page_lblkno);
		if (map_blkno >= map_nblocks)
		{
			if (InRecovery)
			{
				if (run_blocks > 0)
					return run_blocks;
				return ummap_set_identity_run(ctx, rlocator, forknum,
											  page_lblkno, maxblocks, pblkno,
											  false);
			}
			elog(ERROR,
				 "missing Umbra map page %u for fork %d block %u",
				 map_blkno, (int) forknum, page_lblkno);
		}

		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, false, false,
								   LW_SHARED);
		page = MapPageBufferGetData(buffer);
		entry_idx = ummap_entry_index(page_lblkno);
		page_limit = ummap_page_run_limit(page_lblkno,
										  maxblocks - run_blocks);

		for (BlockNumber i = 0; i < page_limit; i++)
		{
			BlockNumber entry_lblkno = page_lblkno + i;
			BlockNumber entry_pblkno;

			entry_pblkno = ummap_page_get_entry(page, entry_idx + i);
			if (!BlockNumberIsValid(entry_pblkno))
			{
				MapPageReleaseBuffer(buffer);
				if (InRecovery)
				{
					if (run_blocks > 0)
						return run_blocks;
					return ummap_set_identity_run(ctx, rlocator, forknum,
												  entry_lblkno, maxblocks,
												  pblkno, false);
				}
				elog(ERROR, "missing Umbra map entry for fork %d block %u",
					 (int) forknum, entry_lblkno);
			}

			if (run_blocks == 0)
				*pblkno = entry_pblkno;
			else if ((uint64) entry_pblkno !=
					 (uint64) *pblkno + run_blocks)
			{
				MapPageReleaseBuffer(buffer);
				return run_blocks;
			}

			run_blocks++;
		}
		MapPageReleaseBuffer(buffer);
	}

	return run_blocks;
}

BlockNumber
ummap_identity_run_limit(ForkNumber forknum, BlockNumber lblkno,
						 BlockNumber maxblocks, BlockNumber *pblkno)
{
	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	*pblkno = lblkno;
	if (!ummap_tracks_fork(forknum))
		return maxblocks;

	return ummap_page_run_limit(lblkno, maxblocks);
}

BlockNumber
ummap_set_identity_block(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator, ForkNumber forknum,
						 BlockNumber lblkno, bool skipFsync)
{
	BlockNumber pblkno;

	(void) ummap_set_identity_run(ctx, rlocator, forknum, lblkno, 1, &pblkno,
								  skipFsync);
	return pblkno;
}

BlockNumber
ummap_set_identity_run(UmbraFileContext *ctx,
					   RelFileLocatorBackend rlocator, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber maxblocks,
					   BlockNumber *pblkno, bool skipFsync)
{
	BlockNumber map_blkno;
	BlockNumber run_limit;
	MapPageBuffer buffer;
	char	   *page;
	bool		dirty = false;
	int			entry_idx;

	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	if (!ummap_tracks_fork(forknum))
	{
		*pblkno = lblkno;
		return maxblocks;
	}

	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d block %u",
			 (int) forknum, lblkno);

	map_blkno = ummap_map_blkno(forknum, lblkno);
	entry_idx = ummap_entry_index(lblkno);
	run_limit = ummap_identity_run_limit(forknum, lblkno, maxblocks, pblkno);

	buffer = MapPageBufferRead(ctx, rlocator, map_blkno, true, skipFsync,
							   LW_EXCLUSIVE);
	page = MapPageBufferGetData(buffer);

	for (BlockNumber i = 0; i < run_limit; i++)
	{
		BlockNumber entry_lblkno = lblkno + i;
		BlockNumber entry_pblkno;

		entry_pblkno = ummap_page_get_entry(page, entry_idx + i);
		if (BlockNumberIsValid(entry_pblkno))
		{
			if (entry_pblkno != entry_lblkno)
			{
				MapPageReleaseBuffer(buffer);
				elog(ERROR,
					 "non-identity Umbra map entry for fork %d block %u points to %u",
					 (int) forknum, entry_lblkno, entry_pblkno);
			}
			continue;
		}

		dirty = true;
	}

	if (dirty)
	{
		for (BlockNumber i = 0; i < run_limit; i++)
		{
			if (!BlockNumberIsValid(
									ummap_page_get_entry(page, entry_idx + i)))
				ummap_page_set_entry(page, entry_idx + i, lblkno + i);
		}
		MapPageMarkBufferDirty(buffer, skipFsync);
	}

	MapPageReleaseBuffer(buffer);
	return run_limit;
}

bool
ummap_tracks_fork(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM ||
		forknum == FSM_FORKNUM ||
		forknum == VISIBILITYMAP_FORKNUM;
}

static BlockNumber
ummap_fork_page_index_to_map_blkno(ForkNumber forknum,
								   BlockNumber fork_page_idx)
{
	uint64		group_no;
	uint64		blkno64;

	switch (forknum)
	{
		case FSM_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
				(uint64) UMMAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
			group_no = (uint64) fork_page_idx /
				(uint64) UMMAP_GROUP_MAIN_PAGES;
			blkno64 = group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
				(uint64) UMMAP_GROUP_FSM_PAGES +
				(uint64) UMMAP_GROUP_VM_PAGES +
				((uint64) fork_page_idx %
				 (uint64) UMMAP_GROUP_MAIN_PAGES);
			break;

		default:
			elog(ERROR, "unsupported fork number %d in Umbra map lookup",
				 (int) forknum);
			pg_unreachable();
	}

	if (blkno64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address map page %u for fork %d",
						fork_page_idx, (int) forknum)));

	return (BlockNumber) blkno64;
}

static BlockNumber
ummap_map_blkno(ForkNumber forknum, BlockNumber lblkno)
{
	return ummap_fork_page_index_to_map_blkno(forknum,
											  lblkno / UMMAP_ENTRIES_PER_PAGE);
}

static int
ummap_entry_index(BlockNumber lblkno)
{
	return lblkno % UMMAP_ENTRIES_PER_PAGE;
}

static BlockNumber
ummap_page_run_limit(BlockNumber lblkno, BlockNumber maxblocks)
{
	int			entry_idx = ummap_entry_index(lblkno);

	return Min(maxblocks,
			   (BlockNumber) UMMAP_ENTRIES_PER_PAGE - entry_idx);
}

static BlockNumber
ummap_page_get_entry(char *page, int entry_idx)
{
	BlockNumber *entries = (BlockNumber *) page;
	BlockNumber pblkno;

	Assert(entry_idx >= 0 && entry_idx < UMMAP_ENTRIES_PER_PAGE);

	pblkno = entries[entry_idx];
	if (!BlockNumberIsValid(pblkno))
		return InvalidBlockNumber;

	return pblkno;
}

static void
ummap_page_set_entry(char *page, int entry_idx, BlockNumber pblkno)
{
	BlockNumber *entries = (BlockNumber *) page;

	Assert(entry_idx >= 0 && entry_idx < UMMAP_ENTRIES_PER_PAGE);
	Assert(BlockNumberIsValid(pblkno));

	entries[entry_idx] = pblkno;
}
