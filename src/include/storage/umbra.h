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

typedef struct UmbraSlotShift
{
	SMgrRelation reln;
	ForkNumber	forknum;
	BlockNumber logical_block;
	uint8		source_slot;
	uint8		target_slot;
	bool		selected;
} UmbraSlotShift;

/* A zero selector chooses slot 0; WAL-backed shifts can choose slots 1 or 2. */
static inline bool
UmbraActiveSlotIsValid(uint8 active_slot)
{
	return active_slot < UMBRA_CHUNK_ACTIVE_SLOTS;
}

static inline uint8
UmbraPreviousActiveSlot(uint8 active_slot)
{
	Assert(UmbraActiveSlotIsValid(active_slot));
	return active_slot == 0 ? UMBRA_CHUNK_ACTIVE_SLOTS - 1 : active_slot - 1;
}

static inline bool
UmbraActiveSlotPhysicalBlock(BlockNumber logical_block, uint8 active_slot,
							BlockNumber *physical_block)
{
	uint64		chunk;
	uint64		offset;
	uint64		physical;

	Assert(UmbraActiveSlotIsValid(active_slot));
	chunk = (uint64) logical_block / UMBRA_CHUNK_PAIRED_PAGES;
	offset = (uint64) logical_block % UMBRA_CHUNK_PAIRED_PAGES;
	physical = chunk *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES) +
		(uint64) active_slot * UMBRA_CHUNK_PAIRED_PAGES + offset;
	if (physical > (uint64) MaxBlockNumber)
		return false;

	*physical_block = (BlockNumber) physical;
	return true;
}

/* Reserve all three physical slots for every logical chunk. */
static inline bool
UmbraMappedPhysicalCapacity(BlockNumber logical_eof,
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

static inline bool
UmbraIsMappedAuxiliaryFork(ForkNumber forknum)
{
	return forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM;
}

static inline bool
UmbraForkUsesActiveSlots(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM || UmbraIsMappedAuxiliaryFork(forknum);
}

extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
extern void umfinishcreate(SMgrRelation reln, ForkNumber forknum,
						   XLogRecPtr create_lsn);
extern void umcheckpoint(void);
extern void umflushdatabasetablespace(Oid dbid, Oid spcOid);
extern void uminvalidatedatabase(Oid dbid);
extern void uminvalidatedatabasetablespace(Oid dbid, Oid spcOid);
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
extern bool UmChooseSlotShift(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber logical_block,
								  UmbraSlotShift *shift);
extern void UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn);
extern bool UmCheckpointWritePredecessorSlot(SMgrRelation reln,
										 ForkNumber forknum,
										 BlockNumber lblkno,
										 const void *buffer,
										 uint8 *source_slot);
extern void UmCheckpointWritebackSourceSlot(SMgrRelation reln,
											ForkNumber forknum,
											BlockNumber lblkno,
											uint8 source_slot);
extern bool UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber logical_block, uint8 active_slot);
extern bool UmRedoSlotShift(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_block, uint8 source_slot,
							uint8 target_slot, XLogRecPtr shift_lsn);

#endif							/* UMBRA_H */
