/*-------------------------------------------------------------------------
 *
 * ummap.c
 *	  Umbra private map fork container.
 *
 * This file owns the private relation-local map fork used by Umbra.  The fork
 * stores a superblock at block 0 followed by identity logical-to-physical map
 * pages.  Later MAP patches can add remap and cache behavior without moving
 * that work into umbra.c.
 *
 * The superblock stores relation-wide logical and physical frontiers.  MAP
 * entries separately own the authoritative logical-to-physical translation.
 * A missing entry is an error during normal operation.  During recovery, this
 * identity-only format can reconstruct a requested entry as L -> L because
 * PostgreSQL WAL supplies L and the current placement rule determines P.  MAP
 * gaps do not define recovery EOF, and a future L != P format must WAL-log the
 * selected physical block.
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
#include "port/pg_crc32c.h"
#include "storage/umfile.h"
#include "storage/ummap.h"

#define UMMAP_SUPERBLOCK_MAGIC		0x554D4252U	/* "UMBR" */
#define UMMAP_SUPERBLOCK_VERSION	1U
#define UMMAP_SUPERBLOCK_SIZE		512
#define UMMAP_SUPERBLOCK_PAYLOAD_SIZE 64

#define UMMAP_ENTRIES_PER_PAGE	(BLCKSZ / sizeof(BlockNumber))

/*
 * Fixed map layout, following the rebase branch's proportional grouping:
 *
 *	block 0: superblock
 *	block 1..: [FSM map page][VM map page][8192 MAIN map pages]
 */
#define UMMAP_BLOCK_SUPER		0
#define UMMAP_BLOCK_FIRST_GROUP	1
#define UMMAP_GROUP_FSM_PAGES	1
#define UMMAP_GROUP_VM_PAGES	1
#define UMMAP_GROUP_MAIN_PAGES	8192
#define UMMAP_GROUP_TOTAL_PAGES \
	(UMMAP_GROUP_FSM_PAGES + UMMAP_GROUP_VM_PAGES + UMMAP_GROUP_MAIN_PAGES)

StaticAssertDecl((BLCKSZ % sizeof(BlockNumber)) == 0,
				 "BLCKSZ must be a multiple of BlockNumber");

typedef struct pg_attribute_packed() UmbraMapSuperblockData
{
	uint32		magic;
	uint32		version;
	uint32		blcksz;
	uint32		flags;

	/* Identity-mode physical allocator caches. */
	BlockNumber next_free_phys_block_main;
	BlockNumber phys_capacity_main;
	BlockNumber next_free_phys_block_fsm;
	BlockNumber phys_capacity_fsm;
	BlockNumber next_free_phys_block_vm;
	BlockNumber phys_capacity_vm;

	/* Relation-wide logical EOF frontiers. */
	BlockNumber logical_nblocks_main;
	BlockNumber logical_nblocks_fsm;
	BlockNumber logical_nblocks_vm;

	uint8		reserved[8];
	pg_crc32c	crc;
} UmbraMapSuperblockData;

typedef union UmbraMapSuperblock
{
	UmbraMapSuperblockData data;
	char		padding[UMMAP_SUPERBLOCK_SIZE];
} UmbraMapSuperblock;

StaticAssertDecl(sizeof(UmbraMapSuperblockData) == UMMAP_SUPERBLOCK_PAYLOAD_SIZE,
				 "Umbra MAP superblock payload size is wrong");
StaticAssertDecl(offsetof(UmbraMapSuperblockData, crc) == 60,
				 "Umbra MAP superblock CRC offset is wrong");
StaticAssertDecl(sizeof(UmbraMapSuperblock) == UMMAP_SUPERBLOCK_SIZE,
				 "Umbra MAP superblock size is wrong");

static void ummap_superblock_init(UmbraMapSuperblock *super);
static void ummap_superblock_refresh_crc(UmbraMapSuperblock *super);
static bool ummap_superblock_check_crc(const UmbraMapSuperblock *super);
static bool ummap_superblock_has_valid_identity(const UmbraMapSuperblock *super);
static bool ummap_superblock_is_valid(const UmbraMapSuperblock *super);
static void ummap_read_superblock(UmbraFileContext *ctx,
								  UmbraMapSuperblock *super);
static void ummap_write_superblock(UmbraFileContext *ctx,
								   const UmbraMapSuperblock *super,
								   bool skipFsync, bool isNew);
static BlockNumber ummap_superblock_get_logical_nblocks(
													const UmbraMapSuperblock *super,
													ForkNumber forknum);
static void ummap_superblock_set_logical_nblocks(UmbraMapSuperblock *super,
												 ForkNumber forknum,
												 BlockNumber nblocks);
static void ummap_superblock_set_identity_physical(UmbraMapSuperblock *super,
												   ForkNumber forknum,
												   BlockNumber nblocks);
static bool ummap_fork_uses_absent_sentinel(ForkNumber forknum);
static BlockNumber ummap_normalize_fork_nblocks(ForkNumber forknum,
												BlockNumber raw);
static void ummap_init_page(char *page);
static void ummap_read_page(UmbraFileContext *ctx, BlockNumber map_blkno,
							char *page);
static void ummap_write_page(UmbraFileContext *ctx, BlockNumber map_blkno,
							 char *page, bool skipFsync);
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
	UmbraMapSuperblock super;

	umfile_create(ctx, UMBRA_MAP_FORKNUM, isRedo);

	if (umfile_nblocks(ctx, UMBRA_MAP_FORKNUM) > 0)
		return;

	ummap_superblock_init(&super);
	ummap_write_superblock(ctx, &super, false, true);
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
	umfile_unlink(rlocator, UMBRA_MAP_FORKNUM, isRedo);
}

void
ummap_init_fork(UmbraFileContext *ctx, ForkNumber forknum, bool skipFsync)
{
	(void) ummap_set_nblocks(ctx, forknum, 0, skipFsync);
}

bool
ummap_fork_exists(UmbraFileContext *ctx, ForkNumber forknum)
{
	UmbraMapSuperblock super;

	if (!ummap_tracks_fork(forknum))
		return umfile_exists(ctx, forknum);

	ummap_read_superblock(ctx, &super);
	if (!ummap_fork_uses_absent_sentinel(forknum))
		return true;

	return BlockNumberIsValid(
		ummap_superblock_get_logical_nblocks(&super, forknum));
}

BlockNumber
ummap_nblocks(UmbraFileContext *ctx, ForkNumber forknum)
{
	BlockNumber nblocks;
	UmbraMapSuperblock super;

	if (!ummap_tracks_fork(forknum))
		return umfile_nblocks(ctx, forknum);

	ummap_read_superblock(ctx, &super);
	nblocks = ummap_superblock_get_logical_nblocks(&super, forknum);
	nblocks = ummap_normalize_fork_nblocks(forknum, nblocks);
	if (!BlockNumberIsValid(nblocks))
		elog(ERROR, "missing Umbra map fork state for fork %d", (int) forknum);

	return nblocks;
}

bool
ummap_set_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
				  BlockNumber nblocks, bool skipFsync)
{
	UmbraMapSuperblock super;

	if (!ummap_tracks_fork(forknum))
		return true;

	ummap_read_superblock(ctx, &super);

	ummap_superblock_set_logical_nblocks(&super, forknum, nblocks);
	ummap_superblock_set_identity_physical(&super, forknum, nblocks);
	ummap_write_superblock(ctx, &super, skipFsync, false);

	return true;
}

BlockNumber
ummap_lookup_block(UmbraFileContext *ctx, ForkNumber forknum,
				   BlockNumber lblkno)
{
	BlockNumber pblkno;

	(void) ummap_lookup_run(ctx, forknum, lblkno, 1, &pblkno);
	return pblkno;
}

BlockNumber
ummap_lookup_run(UmbraFileContext *ctx, ForkNumber forknum,
				 BlockNumber lblkno, BlockNumber maxblocks,
				 BlockNumber *pblkno)
{
	PGIOAlignedBlock page;
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
		int			entry_idx;

		map_blkno = ummap_map_blkno(forknum, page_lblkno);
		if (map_blkno >= map_nblocks)
		{
			if (InRecovery)
			{
				if (run_blocks > 0)
					return run_blocks;
				return ummap_set_identity_run(ctx, forknum, page_lblkno,
										  maxblocks, pblkno, false);
			}
			elog(ERROR,
				 "missing Umbra map page %u for fork %d block %u",
				 map_blkno, (int) forknum, page_lblkno);
		}

		ummap_read_page(ctx, map_blkno, page.data);
		entry_idx = ummap_entry_index(page_lblkno);
		page_limit = ummap_page_run_limit(page_lblkno,
										  maxblocks - run_blocks);

		for (BlockNumber i = 0; i < page_limit; i++)
		{
			BlockNumber entry_lblkno = page_lblkno + i;
			BlockNumber entry_pblkno;

			entry_pblkno = ummap_page_get_entry(page.data, entry_idx + i);
			if (!BlockNumberIsValid(entry_pblkno))
			{
				if (InRecovery)
				{
					if (run_blocks > 0)
						return run_blocks;
					return ummap_set_identity_run(ctx, forknum, entry_lblkno,
											  maxblocks, pblkno, false);
				}
				elog(ERROR, "missing Umbra map entry for fork %d block %u",
						 (int) forknum, entry_lblkno);
			}

			if (run_blocks == 0)
				*pblkno = entry_pblkno;
			else if ((uint64) entry_pblkno !=
					 (uint64) *pblkno + run_blocks)
				return run_blocks;

			run_blocks++;
		}
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
ummap_set_identity_block(UmbraFileContext *ctx, ForkNumber forknum,
						 BlockNumber lblkno, bool skipFsync)
{
	BlockNumber pblkno;

	(void) ummap_set_identity_run(ctx, forknum, lblkno, 1, &pblkno,
								  skipFsync);
	return pblkno;
}

BlockNumber
ummap_set_identity_run(UmbraFileContext *ctx, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber maxblocks,
					   BlockNumber *pblkno, bool skipFsync)
{
	PGIOAlignedBlock page;
	BlockNumber map_blkno;
	BlockNumber nblocks;
	BlockNumber run_limit;
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
	nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	entry_idx = ummap_entry_index(lblkno);
	run_limit = ummap_identity_run_limit(forknum, lblkno, maxblocks, pblkno);

	while (nblocks < map_blkno)
	{
		ummap_init_page(page.data);
		umfile_extend(ctx, UMBRA_MAP_FORKNUM, nblocks, page.data, skipFsync);
		nblocks++;
	}

	if (nblocks == map_blkno)
	{
		ummap_init_page(page.data);

		for (BlockNumber i = 0; i < run_limit; i++)
			ummap_page_set_entry(page.data, entry_idx + i, lblkno + i);

		umfile_extend(ctx, UMBRA_MAP_FORKNUM, map_blkno, page.data,
					  skipFsync);
		return run_limit;
	}

	ummap_read_page(ctx, map_blkno, page.data);

	for (BlockNumber i = 0; i < run_limit; i++)
	{
		BlockNumber entry_lblkno = lblkno + i;
		BlockNumber entry_pblkno;

		entry_pblkno = ummap_page_get_entry(page.data, entry_idx + i);
		if (BlockNumberIsValid(entry_pblkno))
		{
			if (entry_pblkno != entry_lblkno)
				elog(ERROR,
					 "non-identity Umbra map entry for fork %d block %u points to %u",
					 (int) forknum, entry_lblkno, entry_pblkno);
			continue;
		}

		ummap_page_set_entry(page.data, entry_idx + i, entry_lblkno);
		dirty = true;
	}

	if (dirty)
		ummap_write_page(ctx, map_blkno, page.data, skipFsync);

	return run_limit;
}

bool
ummap_tracks_fork(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM ||
		forknum == FSM_FORKNUM ||
		forknum == VISIBILITYMAP_FORKNUM;
}

static bool
ummap_fork_uses_absent_sentinel(ForkNumber forknum)
{
	switch (forknum)
	{
		case FSM_FORKNUM:
		case VISIBILITYMAP_FORKNUM:
			return true;
		default:
			return false;
	}
}

static BlockNumber
ummap_normalize_fork_nblocks(ForkNumber forknum, BlockNumber raw)
{
	if (ummap_fork_uses_absent_sentinel(forknum) &&
		raw == InvalidBlockNumber)
		return 0;

	return raw;
}

static void
ummap_superblock_init(UmbraMapSuperblock *super)
{
	Assert(super != NULL);

	MemSet(super, 0, sizeof(*super));

	super->data.magic = UMMAP_SUPERBLOCK_MAGIC;
	super->data.version = UMMAP_SUPERBLOCK_VERSION;
	super->data.blcksz = BLCKSZ;
	super->data.flags = 0;

	super->data.next_free_phys_block_main = 0;
	super->data.phys_capacity_main = 0;
	super->data.logical_nblocks_main = 0;

	super->data.next_free_phys_block_fsm = InvalidBlockNumber;
	super->data.phys_capacity_fsm = InvalidBlockNumber;
	super->data.logical_nblocks_fsm = InvalidBlockNumber;

	super->data.next_free_phys_block_vm = InvalidBlockNumber;
	super->data.phys_capacity_vm = InvalidBlockNumber;
	super->data.logical_nblocks_vm = InvalidBlockNumber;

	super->data.crc = 0;
}

static void
ummap_superblock_refresh_crc(UmbraMapSuperblock *super)
{
	pg_crc32c	crc;

	Assert(super != NULL);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &super->data, offsetof(UmbraMapSuperblockData, crc));
	FIN_CRC32C(crc);
	super->data.crc = crc;
}

static bool
ummap_superblock_check_crc(const UmbraMapSuperblock *super)
{
	pg_crc32c	crc;

	Assert(super != NULL);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &super->data, offsetof(UmbraMapSuperblockData, crc));
	FIN_CRC32C(crc);

	return crc == super->data.crc;
}

static bool
ummap_superblock_has_valid_identity(const UmbraMapSuperblock *super)
{
	Assert(super != NULL);

	if (super->data.magic != UMMAP_SUPERBLOCK_MAGIC)
		return false;
	if (super->data.version != UMMAP_SUPERBLOCK_VERSION)
		return false;
	if (super->data.blcksz != BLCKSZ)
		return false;

	return true;
}

static bool
ummap_superblock_is_valid(const UmbraMapSuperblock *super)
{
	Assert(super != NULL);

	if (!ummap_superblock_has_valid_identity(super))
		return false;

	return ummap_superblock_check_crc(super);
}

static void
ummap_read_superblock(UmbraFileContext *ctx, UmbraMapSuperblock *super)
{
	Assert(super != NULL);

	umfile_read_bytes(ctx, UMBRA_MAP_FORKNUM, UMMAP_BLOCK_SUPER,
					  super->padding, UMMAP_SUPERBLOCK_SIZE);

	if (!ummap_superblock_is_valid(super))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Umbra map superblock is corrupted")));
}

static void
ummap_write_superblock(UmbraFileContext *ctx, const UmbraMapSuperblock *super,
					   bool skipFsync, bool isNew)
{
	PGIOAlignedBlock page;
	UmbraMapSuperblock write_super;

	Assert(super != NULL);

	write_super = *super;
	ummap_superblock_refresh_crc(&write_super);

	if (isNew)
	{
		MemSet(page.data, 0, BLCKSZ);
		memcpy(page.data, write_super.padding, UMMAP_SUPERBLOCK_SIZE);
		umfile_extend(ctx, UMBRA_MAP_FORKNUM, UMMAP_BLOCK_SUPER, page.data,
					  skipFsync);
		return;
	}

	umfile_write_bytes(ctx, UMBRA_MAP_FORKNUM, UMMAP_BLOCK_SUPER,
					   write_super.padding, UMMAP_SUPERBLOCK_SIZE, skipFsync);
}

static BlockNumber
ummap_superblock_get_logical_nblocks(const UmbraMapSuperblock *super,
									 ForkNumber forknum)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return super->data.logical_nblocks_main;
		case FSM_FORKNUM:
			return super->data.logical_nblocks_fsm;
		case VISIBILITYMAP_FORKNUM:
			return super->data.logical_nblocks_vm;
		default:
			elog(ERROR, "unsupported fork number for Umbra map superblock: %d",
				 (int) forknum);
	}

	pg_unreachable();
}

static void
ummap_superblock_set_logical_nblocks(UmbraMapSuperblock *super,
									 ForkNumber forknum, BlockNumber nblocks)
{
	Assert(super != NULL);
	Assert(BlockNumberIsValid(nblocks));

	switch (forknum)
	{
		case MAIN_FORKNUM:
			super->data.logical_nblocks_main = nblocks;
			break;
		case FSM_FORKNUM:
			super->data.logical_nblocks_fsm = nblocks;
			break;
		case VISIBILITYMAP_FORKNUM:
			super->data.logical_nblocks_vm = nblocks;
			break;
		default:
			elog(ERROR, "unsupported fork number for Umbra map superblock: %d",
				 (int) forknum);
	}
}

static void
ummap_superblock_set_identity_physical(UmbraMapSuperblock *super,
									   ForkNumber forknum, BlockNumber nblocks)
{
	Assert(super != NULL);
	Assert(BlockNumberIsValid(nblocks));

	switch (forknum)
	{
		case MAIN_FORKNUM:
			super->data.next_free_phys_block_main = nblocks;
			super->data.phys_capacity_main = nblocks;
			break;
		case FSM_FORKNUM:
			super->data.next_free_phys_block_fsm = nblocks;
			super->data.phys_capacity_fsm = nblocks;
			break;
		case VISIBILITYMAP_FORKNUM:
			super->data.next_free_phys_block_vm = nblocks;
			super->data.phys_capacity_vm = nblocks;
			break;
		default:
			elog(ERROR, "unsupported fork number for Umbra map superblock: %d",
				 (int) forknum);
	}
}

static void
ummap_init_page(char *page)
{
	BlockNumber *entries = (BlockNumber *) page;

	for (int i = 0; i < UMMAP_ENTRIES_PER_PAGE; i++)
		entries[i] = InvalidBlockNumber;
}

static void
ummap_read_page(UmbraFileContext *ctx, BlockNumber map_blkno, char *page)
{
	void	   *buffers[1];

	buffers[0] = page;
	umfile_readv(ctx, UMBRA_MAP_FORKNUM, map_blkno, buffers, 1);
}

static void
ummap_write_page(UmbraFileContext *ctx, BlockNumber map_blkno, char *page,
				 bool skipFsync)
{
	const void *buffers[1];

	buffers[0] = page;
	umfile_writev(ctx, UMBRA_MAP_FORKNUM, map_blkno, buffers, 1, skipFsync);
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
			blkno64 = (uint64) UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = (uint64) UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
				(uint64) UMMAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
			group_no = (uint64) fork_page_idx /
				(uint64) UMMAP_GROUP_MAIN_PAGES;
			blkno64 = (uint64) UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
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
