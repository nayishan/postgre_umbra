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

/* Every logical page owns three physical slots. */
#define UMBRA_ACTIVE_SLOT_COUNT 3U

static inline bool
UmbraSlotZeroPhysicalBlock(BlockNumber logical_block,
						   BlockNumber *physical_block)
{
	uint64		physical = (uint64) logical_block * UMBRA_ACTIVE_SLOT_COUNT;

	if (physical >= (uint64) InvalidBlockNumber)
		return false;
	*physical_block = (BlockNumber) physical;
	return true;
}

static inline bool
UmbraThreeBucketPhysicalCapacity(BlockNumber logical_eof,
							 BlockNumber *physical_capacity)
{
	uint64		capacity = (uint64) logical_eof * UMBRA_ACTIVE_SLOT_COUNT;

	if (capacity >= (uint64) InvalidBlockNumber)
		return false;
	*physical_capacity = (BlockNumber) capacity;
	return true;
}

/* Relation state decides whether this eligible fork uses three buckets. */
static inline bool
UmbraForkUsesThreeBuckets(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM;
}

extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
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
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern int	umfd(SMgrRelation reln, ForkNumber forknum,
				 BlockNumber blocknum, uint32 *off);

#endif							/* UMBRA_H */
