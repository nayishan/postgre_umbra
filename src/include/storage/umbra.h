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

#include "access/xlogdefs.h"
#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/* Every logical page owns three physical slots. */
#define UMBRA_ACTIVE_SLOT_COUNT 3U
#define UMBRA_ACTIVE_SLOT_INVALID UINT8_MAX

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
	return active_slot < UMBRA_ACTIVE_SLOT_COUNT;
}

static inline uint8
UmbraNextActiveSlot(uint8 active_slot)
{
	Assert(UmbraActiveSlotIsValid(active_slot));
	return active_slot == UMBRA_ACTIVE_SLOT_COUNT - 1 ? 0 : active_slot + 1;
}

static inline uint8
UmbraPreviousActiveSlot(uint8 active_slot)
{
	Assert(UmbraActiveSlotIsValid(active_slot));
	return active_slot == 0 ? UMBRA_ACTIVE_SLOT_COUNT - 1 : active_slot - 1;
}

static inline bool
UmbraActiveSlotPhysicalBlock(BlockNumber logical_block, uint8 active_slot,
							 BlockNumber *physical_block)
{
	uint64		physical;

	Assert(UmbraActiveSlotIsValid(active_slot));
	physical = (uint64) logical_block * UMBRA_ACTIVE_SLOT_COUNT + active_slot;
	if (physical >= (uint64) InvalidBlockNumber)
		return false;
	*physical_block = (BlockNumber) physical;
	return true;
}

/* Recover the fixed-layout selector from a logical/physical block pair. */
static inline bool
UmbraPhysicalBlockActiveSlot(BlockNumber logical_block,
							 BlockNumber physical_block, uint8 *active_slot)
{
	BlockNumber candidate;

	Assert(active_slot != NULL);
	*active_slot = UMBRA_ACTIVE_SLOT_INVALID;
	for (uint8 slot = 0; slot < UMBRA_ACTIVE_SLOT_COUNT; slot++)
	{
		if (UmbraActiveSlotPhysicalBlock(logical_block, slot, &candidate) &&
			candidate == physical_block)
		{
			*active_slot = slot;
			return true;
		}
	}
	return false;
}

static inline bool
UmbraSlotZeroPhysicalBlock(BlockNumber logical_block,
						   BlockNumber *physical_block)
{
	return UmbraActiveSlotPhysicalBlock(logical_block, 0, physical_block);
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

static inline bool
UmbraIsMappedAuxiliaryFork(ForkNumber forknum)
{
	return forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM;
}

/* INIT uses direct layout so startup can copy it directly to MAIN. */
static inline bool
UmbraForkUsesThreeBuckets(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM || UmbraIsMappedAuxiliaryFork(forknum);
}

static inline bool
UmbraForkUsesActiveSlots(ForkNumber forknum)
{
	/* Auxiliary forks adopt selector rotation in a later mechanism patch. */
	return forknum == MAIN_FORKNUM;
}

extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
/* Synchronize selector MAP pages after ordinary relation forks are durable. */
extern void umsyncrelationmetadata(SMgrRelation reln);
/* Flush checkpoint selector MAP pages after ordinary shared buffers. */
extern void umcheckpoint(void);
extern bool umexists(SMgrRelation reln, ForkNumber forknum);
extern void umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
					 bool isRedo);
extern void umextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void *buffer, bool skipFsync,
					 BlockNumber *physical_block);
extern void umzeroextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, int nblocks, bool skipFsync,
					 BlockNumber *physical_blocks);
extern bool umprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, int nblocks);
extern uint32 ummaxcombine(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum);
extern void umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					void **buffers, BlockNumber nblocks);
extern void umstartreadv(PgAioHandle *ioh, SMgrRelation reln,
						 ForkNumber forknum, BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks);
extern void umwritev(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void **buffers,
					 BlockNumber nblocks, bool skipFsync,
					 BlockNumber *physical_blocks);
/* Three-bucket logical writeback is deferred until a physical target is known. */
extern void umwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
/* This path receives the resolved physical range from the scheduler. */
extern void umwritebackphysical(SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber umnblocks(SMgrRelation reln, ForkNumber forknum);
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern void umflushdatabasetablespacecache(Oid dbid, Oid spcOid);
extern void uminvalidatedatabasecache(Oid dbid);
extern void uminvalidatedatabasetablespacecache(Oid dbid, Oid spcOid);
extern int	umfd(SMgrRelation reln, ForkNumber forknum,
				 BlockNumber blocknum, uint32 *off);
extern bool UmGetActiveSlot(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber logical_block, uint8 *active_slot,
							 bool *selector_page_present);
/* Select a shift source from BufferMgr state without reading a selector. */
extern bool UmChooseSlotShift(SMgrRelation reln, ForkNumber forknum,
							  BlockNumber logical_block,
							  uint8 cached_active_slot,
							  bool selector_page_present,
							  UmbraSlotShift *shift);
/* Publish a selected target after WAL insertion has assigned its LSN. */
extern void UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn);
extern bool UmCheckpointWriteSourceSlot(SMgrRelation reln, ForkNumber forknum,
										BlockNumber lblkno, uint8 source_slot,
										const void *buffer,
										BlockNumber *physical_block);
extern bool UmUsesMappedSlots(SMgrRelation reln, ForkNumber forknum);
/* Write an existing mapped page to a caller-selected slot and report its pblk. */
extern bool UmWriteSlot(SMgrRelation reln, ForkNumber forknum,
						BlockNumber logical_block, const void *buffer,
						uint8 slot, BlockNumber *physical_block);
extern bool UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber logical_block,
									 uint8 active_slot);
extern bool UmRedoSlotShift(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_block, uint8 source_slot,
							uint8 target_slot, XLogRecPtr shift_lsn);

#endif							/* UMBRA_H */
