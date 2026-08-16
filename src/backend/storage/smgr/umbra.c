/*-------------------------------------------------------------------------
 *
 * umbra.c
 *    Umbra storage manager.
 *
 * Non-temporary MAIN, FSM, VM, and INIT forks use a fixed three-bucket
 * physical layout.  Relation extent is the physical data-fork length divided
 * by three; selector MAP pages only choose the active slot for MAIN, FSM, and
 * VM.  No relation-level metadata page is layout authority.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/map.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	UmbraFileContext *filectx;
} UmbraSmgrRelationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static bool um_fork_uses_three_buckets(SMgrRelation reln, ForkNumber forknum);
static bool um_fork_uses_selector_slots(SMgrRelation reln, ForkNumber forknum);
static BlockNumber um_active_pblk(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber logical_block,
								  uint8 *active_slot);
static BlockNumber um_mapped_range_end(SMgrRelation reln, ForkNumber forknum,
										  BlockNumber blocknum,
										  BlockNumber nblocks);
static BlockNumber um_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									   BlockNumber logical_eof);
static BlockNumber um_mapped_nblocks(SMgrRelation reln, ForkNumber forknum);
static void um_require_mapped_range(SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum, BlockNumber nblocks);
static void um_ensure_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber physical_capacity,
									  bool skipFsync);
static void um_reset_selector_range(SMgrRelation reln, ForkNumber forknum,
									BlockNumber first_block, BlockNumber nblocks,
									bool skipFsync);
static void um_ensure_selector_pages(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber first_block, BlockNumber nblocks,
									 bool skipFsync);
static bool um_resolve_slot_physical_block(SMgrRelation reln,
											ForkNumber forknum,
											BlockNumber logical_block, uint8 slot,
											BlockNumber *physical_block);

void
uminit(void)
{
	umfile_init();
}

void
umopen(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	/* Keep initialization retryable after a caught allocation ERROR. */
	if (state == NULL)
	{
		state = MemoryContextAllocZero(TopMemoryContext, sizeof(*state));
		reln->smgr_private = state;
	}
	if (state->filectx == NULL)
		state->filectx = umfile_open(reln->smgr_rlocator);
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL || state->filectx == NULL)
		return;
	umfile_close(state->filectx, forknum);
	/* MAIN close also releases this relation's selector metadata descriptors. */
	if (forknum == MAIN_FORKNUM)
		umfile_close(state->filectx, UMBRA_METADATA_FORKNUM);
}

void
umdestroy(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL)
		return;
	if (state->filectx != NULL)
		umfile_destroy(state->filectx);
	pfree(state);
	reln->smgr_private = NULL;
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	umfile_create(um_get_filectx(reln), forknum, isRedo);
}

void
umcheckpoint(void)
{
	ummap_checkpoint();
}

void
umflushdatabasetablespacecache(Oid dbid, Oid spcOid)
{
	ummap_flush_database_tablespace_cache(dbid, spcOid);
}

void
uminvalidatedatabasecache(Oid dbid)
{
	ummap_invalidate_database_cache(dbid);
}

void
uminvalidatedatabasetablespacecache(Oid dbid, Oid spcOid)
{
	ummap_invalidate_database_tablespace_cache(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	return umfile_exists(um_get_filectx(reln), forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	/* MAIN deletion destroys the relation's selector metadata as well. */
	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
		ummap_unlink(rlocator, isRedo);
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 const void *buffer, bool skipFsync, BlockNumber *physical_block)
{
	UmbraFileContext *ctx;
	BlockNumber	logical_eof;
	BlockNumber	logical_end;
	BlockNumber	physical_capacity;
	BlockNumber	physical_target;
	const void *buffers[1];

	if (physical_block != NULL)
		*physical_block = InvalidBlockNumber;
	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer,
					  skipFsync);
		if (physical_block != NULL)
			*physical_block = blocknum;
		return;
	}

	logical_eof = um_mapped_nblocks(reln, forknum);
	if (blocknum < logical_eof)
		ereport(ERROR,
				(errmsg("cannot extend Umbra three-bucket fork below logical EOF")));
	logical_end = um_mapped_range_end(reln, forknum, blocknum, 1);
	physical_capacity = um_mapped_capacity(reln, forknum, logical_end);

	/* A truncated tail can retain selector entries, but not physical slots. */
	um_reset_selector_range(reln, forknum, logical_eof,
						logical_end - logical_eof, skipFsync);
	um_ensure_mapped_capacity(reln, forknum, physical_capacity, skipFsync);
	physical_target = um_active_pblk(reln, forknum, blocknum, NULL);
	ctx = um_get_filectx(reln);
	buffers[0] = buffer;
	umfile_writev(ctx, forknum, physical_target, buffers, 1, skipFsync);

	if (physical_block != NULL)
		*physical_block = physical_target;
	um_ensure_selector_pages(reln, forknum, logical_eof,
						 logical_end - logical_eof, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 int nblocks, bool skipFsync, BlockNumber *physical_blocks)
{
	BlockNumber	logical_eof;
	BlockNumber	logical_end;
	BlockNumber	physical_capacity;

	if (physical_blocks != NULL)
	{
		for (int i = 0; i < nblocks; i++)
			physical_blocks[i] = InvalidBlockNumber;
	}
	if (nblocks <= 0)
		return;
	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
						  skipFsync);
		if (physical_blocks != NULL)
		{
			for (int i = 0; i < nblocks; i++)
				physical_blocks[i] = blocknum + i;
		}
		return;
	}

	logical_end = um_mapped_range_end(reln, forknum, blocknum,
									 (BlockNumber) nblocks);
	logical_eof = um_mapped_nblocks(reln, forknum);
	if (logical_end > logical_eof)
	{
		physical_capacity = um_mapped_capacity(reln, forknum, logical_end);
		um_reset_selector_range(reln, forknum, logical_eof,
								logical_end - logical_eof, skipFsync);
		um_ensure_mapped_capacity(reln, forknum, physical_capacity, skipFsync);
		um_ensure_selector_pages(reln, forknum, logical_eof,
								 logical_end - logical_eof, skipFsync);
	}

	if (physical_blocks != NULL)
	{
		for (int i = 0; i < nblocks; i++)
			physical_blocks[i] = um_active_pblk(reln, forknum, blocknum + i,
													NULL);
	}
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   int nblocks)
{
	if (!um_fork_uses_three_buckets(reln, forknum))
		return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
	if (nblocks <= 0)
		return true;
	um_require_mapped_range(reln, forknum, blocknum, (BlockNumber) nblocks);
	for (int i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, blocknum + i, NULL);

		if (!umfile_prefetch(um_get_filectx(reln), forknum, physical_block, 1))
			return false;
	}
	return true;
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	if (um_fork_uses_three_buckets(reln, forknum))
	{
		/* Adjacent logical blocks are normally three physical blocks apart. */
		return 1;
	}
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			void **buffers, BlockNumber nblocks)
{
	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
		return;
	}
	if (nblocks == 0)
		return;
	um_require_mapped_range(reln, forknum, blocknum, nblocks);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, blocknum + i, NULL);

		umfile_readv(um_get_filectx(reln), forknum, physical_block,
						 &buffers[i], 1);
	}
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
				 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	PgAioTargetData *target;
	BlockNumber	physical_block;
	uint8		active_slot;
	bool		selector_page_present;

	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
		pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
		umfile_startreadv(ioh, um_get_filectx(reln), forknum, blocknum,
						  buffers, nblocks);
		return;
	}
	if (nblocks != 1)
		elog(ERROR, "Umbra three-bucket AIO read crosses a physical range");
	um_require_mapped_range(reln, forknum, blocknum, 1);
	physical_block = um_active_pblk(reln, forknum, blocknum, &active_slot);
	selector_page_present = false;
	if (um_fork_uses_selector_slots(reln, forknum))
		(void) UmGetActiveSlot(reln, forknum, blocknum, &active_slot,
								   &selector_page_present);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	target = pgaio_io_get_target_data(ioh);
	target->smgr.physicalBlockNum = physical_block;
	target->smgr.umbraActiveSlot = active_slot;
	target->smgr.umbraSelectorPagePresent = selector_page_present;
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), forknum, physical_block,
					  buffers, nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 const void **buffers, BlockNumber nblocks, bool skipFsync,
			 BlockNumber *physical_blocks)
{
	UmbraFileContext *ctx;

	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
					  skipFsync);
		if (physical_blocks != NULL)
		{
			for (BlockNumber i = 0; i < nblocks; i++)
				physical_blocks[i] = blocknum + i;
		}
		return;
	}
	if (nblocks == 0)
		return;
	um_require_mapped_range(reln, forknum, blocknum, nblocks);
	ctx = um_get_filectx(reln);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber	physical_block =
			um_active_pblk(reln, forknum, blocknum + i, NULL);

		umfile_writev(ctx, forknum, physical_block, &buffers[i], 1,
						  skipFsync);
		if (physical_blocks != NULL)
			physical_blocks[i] = physical_block;
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
				BlockNumber blocknum, BlockNumber nblocks)
{
	/* Logical ranges have no single physical writeback range. */
	if (um_fork_uses_three_buckets(reln, forknum))
		return;
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

void
umwritebackphysical(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, BlockNumber nblocks)
{
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (um_fork_uses_three_buckets(reln, forknum))
		return um_mapped_nblocks(reln, forknum);
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber old_blocks, BlockNumber nblocks)
{
	BlockNumber	old_physical;
	BlockNumber	new_physical;

	if (!um_fork_uses_three_buckets(reln, forknum))
	{
		umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
		return;
	}
	old_physical = um_mapped_capacity(reln, forknum, old_blocks);
	new_physical = um_mapped_capacity(reln, forknum, nblocks);
	/* smgrnblocks() opened all physical segments before this critical section. */
	umfile_truncate(um_get_filectx(reln), forknum, old_physical, new_physical);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	umfile_immedsync(um_get_filectx(reln), forknum);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	umfile_registersync(um_get_filectx(reln), forknum);
}

void
umsyncrelationmetadata(SMgrRelation reln)
{
	if (!RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		ummap_sync_relation_metadata(um_get_filectx(reln), reln->smgr_rlocator);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/* AIO reopen receives the resolved physical block from target data. */
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

bool
UmGetActiveSlot(SMgrRelation reln, ForkNumber forknum,
					BlockNumber logical_block, uint8 *active_slot,
					bool *selector_page_present)
{
	if (active_slot != NULL)
		*active_slot = UMBRA_ACTIVE_SLOT_INVALID;
	if (selector_page_present != NULL)
		*selector_page_present = false;
	if (reln == NULL || active_slot == NULL ||
		!um_fork_uses_three_buckets(reln, forknum))
		return false;
	if (!um_fork_uses_selector_slots(reln, forknum))
	{
		*active_slot = 0;
		return true;
	}
	*active_slot = MapGetActiveSlotWithPresence(um_get_filectx(reln),
											reln->smgr_rlocator, forknum, logical_block,
											selector_page_present);
	return true;
}

bool
UmUsesMappedSlots(SMgrRelation reln, ForkNumber forknum)
{
	return um_fork_uses_three_buckets(reln, forknum);
}

bool
UmChooseSlotShift(SMgrRelation reln, ForkNumber forknum,
				  BlockNumber logical_block, uint8 cached_active_slot,
				  bool selector_page_present, UmbraSlotShift *shift)
{
	if (shift == NULL)
		elog(ERROR, "Umbra slot-shift state is required");
	MemSet(shift, 0, sizeof(*shift));
	if (reln == NULL || InRecovery ||
		!um_fork_uses_selector_slots(reln, forknum) ||
		IsBootstrapProcessingMode() || IsInitProcessingMode())
		return false;
	if (!UmbraActiveSlotIsValid(cached_active_slot))
		elog(PANIC, "Umbra mapped buffer has no cached active slot");
	if (!selector_page_present && cached_active_slot != 0)
		elog(PANIC, "Umbra mapped buffer has a nonzero slot without a selector");

	shift->reln = reln;
	shift->forknum = forknum;
	shift->logical_block = logical_block;
	shift->source_slot = cached_active_slot;
	shift->target_slot = UmbraNextActiveSlot(cached_active_slot);
	shift->selected = true;
	return true;
}

void
UmPublishSlotShift(UmbraSlotShift *shift, XLogRecPtr lsn)
{
	Assert(shift != NULL);
	Assert(shift->selected);
	Assert(shift->reln != NULL);
	Assert(XLogRecPtrIsValid(lsn));
	MapPublishSlotShift(um_get_filectx(shift->reln),
						shift->reln->smgr_rlocator, shift->forknum,
						shift->logical_block, shift->source_slot,
						shift->target_slot, lsn);
	shift->selected = false;
	shift->reln = NULL;
}

bool
UmCheckpointWriteSourceSlot(SMgrRelation reln, ForkNumber forknum,
							BlockNumber logical_block, uint8 source_slot,
							const void *buffer, BlockNumber *physical_block)
{
	const void *buffers[1] = {buffer};

	if (physical_block != NULL)
		*physical_block = InvalidBlockNumber;
	if (reln == NULL || buffer == NULL || physical_block == NULL ||
		!UmbraActiveSlotIsValid(source_slot) ||
		!um_fork_uses_selector_slots(reln, forknum))
		return false;
	um_require_mapped_range(reln, forknum, logical_block, 1);
	if (!UmbraActiveSlotPhysicalBlock(logical_block, source_slot, physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot block mapping overflow")));
	umfile_writev(um_get_filectx(reln), forknum, *physical_block,
				  buffers, 1, false);
	return true;
}

bool
UmWriteSlot(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, const void *buffer, uint8 slot,
				BlockNumber *physical_block)
{
	const void *buffers[1] = {buffer};

	if (physical_block != NULL)
		*physical_block = InvalidBlockNumber;
	if (buffer == NULL || physical_block == NULL)
		return false;
	HOLD_INTERRUPTS();
	if (!um_resolve_slot_physical_block(reln, forknum, logical_block, slot,
									 physical_block))
	{
		RESUME_INTERRUPTS();
		return false;
	}
	um_require_mapped_range(reln, forknum, logical_block, 1);
	umfile_writev(um_get_filectx(reln), forknum, *physical_block,
				  buffers, 1, false);
	RESUME_INTERRUPTS();
	return true;
}

bool
UmRedoSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
					BlockNumber logical_block, uint8 active_slot)
{
	UmbraFileContext *ctx;

	if (!InRecovery || reln == NULL ||
		!um_fork_uses_selector_slots(reln, forknum) ||
		!UmbraActiveSlotIsValid(active_slot))
		elog(PANIC, "Umbra redo targets an invalid active-slot mapping");
	ctx = um_get_filectx(reln);
	/* A normal redo record cannot fabricate an absent or zero logical page. */
	if (!umfile_exists(ctx, forknum) ||
		logical_block >= um_mapped_nblocks(reln, forknum))
		return false;
	MapRedoSetActiveSlot(ctx, reln->smgr_rlocator, forknum, logical_block,
					 active_slot);
	return true;
}

bool
UmRedoSlotShift(SMgrRelation reln, ForkNumber forknum,
				BlockNumber logical_block, uint8 source_slot,
				uint8 target_slot, XLogRecPtr shift_lsn)
{
	UmbraFileContext *ctx;

	if (!InRecovery || reln == NULL ||
		!um_fork_uses_selector_slots(reln, forknum) ||
		!UmbraActiveSlotIsValid(source_slot) ||
		!UmbraActiveSlotIsValid(target_slot) ||
		target_slot != UmbraNextActiveSlot(source_slot) ||
		!XLogRecPtrIsValid(shift_lsn))
		elog(PANIC, "Umbra redo targets an invalid slot shift");
	ctx = um_get_filectx(reln);
	/* The following FPI/WILL_INIT redo materializes any missing physical slots. */
	umfile_create(ctx, forknum, true);
	MapRedoSlotShift(ctx, reln->smgr_rlocator, forknum, logical_block,
					 source_slot, target_slot);
	return true;
}

static UmbraFileContext *
um_get_filectx(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state == NULL || state->filectx == NULL)
	{
		umopen(reln);
		state = reln->smgr_private;
	}
	return state->filectx;
}

static bool
um_fork_uses_three_buckets(SMgrRelation reln, ForkNumber forknum)
{
	return reln != NULL && !RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
		UmbraForkUsesThreeBuckets(forknum);
}

static bool
um_fork_uses_selector_slots(SMgrRelation reln, ForkNumber forknum)
{
	return reln != NULL && !RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
		UmbraForkUsesActiveSlots(forknum);
}

static BlockNumber
um_active_pblk(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber logical_block, uint8 *active_slot)
{
	BlockNumber	physical_block;
	uint8		slot;

	if (!UmGetActiveSlot(reln, forknum, logical_block, &slot, NULL))
		elog(ERROR, "could not resolve Umbra active slot");
	if (!UmbraActiveSlotPhysicalBlock(logical_block, slot, &physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra active-slot block mapping overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	if (active_slot != NULL)
		*active_slot = slot;
	return physical_block;
}

static BlockNumber
um_mapped_range_end(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, BlockNumber nblocks)
{
	uint64		end = (uint64) blocknum + nblocks;

	if (end >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra logical block range overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return (BlockNumber) end;
}

static BlockNumber
um_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber logical_eof)
{
	BlockNumber	physical_capacity;

	if (!UmbraThreeBucketPhysicalCapacity(logical_eof, &physical_capacity))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra three-bucket capacity overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber, (int) forknum)));
	return physical_capacity;
}

static BlockNumber
um_mapped_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber	physical_nblocks = umfile_nblocks(um_get_filectx(reln),
													  forknum);

	/*
	 * FileZero() can expose one or two zeroed slots while an extension is in
	 * progress.  Only a complete three-slot group represents a logical page;
	 * rounding down keeps an old logical page writable during that interval.
	 */
	return physical_nblocks / UMBRA_ACTIVE_SLOT_COUNT;
}

static void
um_require_mapped_range(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber	logical_end;
	BlockNumber	logical_eof;

	logical_end = um_mapped_range_end(reln, forknum, blocknum, nblocks);
	logical_eof = um_mapped_nblocks(reln, forknum);
	if (logical_end > logical_eof)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not access logical block %u beyond Umbra EOF %u for fork %d",
						blocknum, logical_eof, (int) forknum)));
}

static void
um_ensure_mapped_capacity(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber physical_capacity, bool skipFsync)
{
	UmbraFileContext *ctx = um_get_filectx(reln);
	BlockNumber	current_physical = umfile_nblocks(ctx, forknum);

	/*
	 * Resume at an incomplete tail left by FileZero() rather than treating it
	 * as a logical page.  This can be observed concurrently and can remain
	 * after an interrupted extension; the unwritten slots are zero only.
	 */
	while (current_physical < physical_capacity)
	{
		uint64		remaining = (uint64) physical_capacity - current_physical;
		int			extend_by = (int) Min(remaining, (uint64) INT_MAX);

		umfile_zeroextend(ctx, forknum, current_physical, extend_by, skipFsync);
		current_physical += (BlockNumber) extend_by;
	}
}

static void
um_reset_selector_range(SMgrRelation reln, ForkNumber forknum,
					BlockNumber first_block, BlockNumber nblocks, bool skipFsync)
{
	if (nblocks == 0 || !um_fork_uses_selector_slots(reln, forknum))
		return;
	Assert(CritSectionCount == 0);
	MapResetActiveSlots(um_get_filectx(reln), reln->smgr_rlocator, forknum,
					  first_block, nblocks, skipFsync);
}

static void
um_ensure_selector_pages(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber first_block, BlockNumber nblocks,
					 bool skipFsync)
{
	/*
	 * A later WAL insertion can publish a slot shift in a critical section.
	 * Seed its default-zero selector while this extension still runs outside
	 * that section, including initdb's single-user post-bootstrap phase.
	 */
	if (nblocks == 0 || !um_fork_uses_selector_slots(reln, forknum) ||
		CritSectionCount != 0)
		return;
	MapEnsureActiveSlotPages(um_get_filectx(reln), reln->smgr_rlocator,
						  forknum, first_block, nblocks, skipFsync);
}

static bool
um_resolve_slot_physical_block(SMgrRelation reln, ForkNumber forknum,
									BlockNumber logical_block, uint8 slot,
									BlockNumber *physical_block)
{
	if (physical_block != NULL)
		*physical_block = InvalidBlockNumber;
	if (reln == NULL || physical_block == NULL ||
		!UmbraActiveSlotIsValid(slot) ||
		!um_fork_uses_selector_slots(reln, forknum))
		return false;
	if (!UmbraActiveSlotPhysicalBlock(logical_block, slot, physical_block))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Umbra slot block mapping overflow")));
	return true;
}
