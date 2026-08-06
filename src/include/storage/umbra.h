/*-------------------------------------------------------------------------
 *
 * umbra.h
 *	  Umbra storage manager public interface declarations.
 *
 * This header declares the Umbra smgr callback surface used by smgr.c when
 * the build is configured with Umbra support.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/umbra.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMBRA_H
#define UMBRA_H

#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/* Stored in every Umbra metadata root, including before formula activation. */
#ifndef UMBRA_CHUNK_PAIRED_PAGES
#define UMBRA_CHUNK_PAIRED_PAGES 32U
#endif
#if UMBRA_CHUNK_PAIRED_PAGES <= 0
#error "UMBRA_CHUNK_PAIRED_PAGES must be greater than zero"
#endif

#define UMBRA_CHUNK_ACTIVE_SLOTS 3U

/* MAIN pages are born in slot 0 until a later patch adds selector pages. */
static inline bool
UmbraMainSlot0PhysicalBlock(BlockNumber logical_block,
						BlockNumber *physical_block)
{
	uint64		chunk;
	uint64		offset;
	uint64		physical;

	chunk = (uint64) logical_block / UMBRA_CHUNK_PAIRED_PAGES;
	offset = (uint64) logical_block % UMBRA_CHUNK_PAIRED_PAGES;
	physical = chunk *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES) +
		offset;
	if (physical > (uint64) MaxBlockNumber)
		return false;

	*physical_block = (BlockNumber) physical;
	return true;
}

static inline bool
UmbraMainSlot0PhysicalCapacity(BlockNumber logical_eof,
						   BlockNumber *physical_capacity)
{
	uint64		chunks;
	uint64		capacity;

	if (logical_eof == 0)
	{
		*physical_capacity = 0;
		return true;
	}

	chunks = ((uint64) logical_eof + UMBRA_CHUNK_PAIRED_PAGES - 1) /
		UMBRA_CHUNK_PAIRED_PAGES;
	capacity = chunks *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES);
	if (capacity >= (uint64) InvalidBlockNumber)
		return false;

	*physical_capacity = (BlockNumber) capacity;
	return true;
}

extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
extern void umfinishcreate(SMgrRelation reln, ForkNumber forknum);
/* Flush Umbra's private metadata-root cache at the checkpoint boundary. */
extern void umcheckpoint(void);
/* Flush Umbra's metadata-root cache before copying a database tablespace. */
extern void umflushdatabasetablespacecache(Oid dbid, Oid spcOid);
/* Forget a database's metadata-root cache entries without touching files. */
extern void uminvalidatedatabasecache(Oid dbid);
/*
 * Forget one tablespace's metadata-root cache entries without touching files.
 */
extern void uminvalidatedatabasetablespacecache(Oid dbid, Oid spcOid);
extern bool umexists(SMgrRelation reln, ForkNumber forknum);
extern void umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
					 bool isRedo);
extern void umextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void umzeroextend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool umprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, int nblocks);
extern uint32 ummaxcombine(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum);
extern void umreadv(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, void **buffers, BlockNumber nblocks);
extern void umstartreadv(PgAioHandle *ioh,
						 SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks);
extern void umwritev(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void **buffers,
					 BlockNumber nblocks, bool skipFsync);
extern void umwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber umnblocks(SMgrRelation reln, ForkNumber forknum);
extern void umpreparetruncate(SMgrRelation reln, ForkNumber *forknum,
						 int nforks, BlockNumber *old_blocks,
						 BlockNumber *nblocks);
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern bool umpreparependingsync(SMgrRelation reln);
extern int	umfd(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, uint32 *off);

#endif							/* UMBRA_H */
