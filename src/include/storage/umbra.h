/*-------------------------------------------------------------------------
 *
 * umbra.h
 *    Umbra storage manager public interface declarations.
 *
 * This header declares the Umbra smgr callback surface used by smgr.c when
 * the build is configured with --with-umbra.
 *
 *-------------------------------------------------------------------------
 */

#ifndef UMBRA_H
#define UMBRA_H

#include "storage/aio_types.h"
#include "storage/block.h"
#include "common/relpath.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "storage/um_defs.h"

extern PGDLLIMPORT bool umbra_chunk_zero_fill_all_slots;
extern PGDLLIMPORT bool umbra_exp2_c3_pause;

/*
 * Umbra MAP policy.
 *
 * This is the handle-local Umbra access state. Mapped forks must not silently
 * fall back to direct physical addressing unless upper layers explicitly allow
 * it.
 */
typedef enum UmbraMapPolicy
{
	UMBRA_MAP_POLICY_UNKNOWN = 0,
	UMBRA_MAP_POLICY_BYPASS_MAP,
	UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP,
	UMBRA_MAP_POLICY_REQUIRE_MAP,
} UmbraMapPolicy;

/*
 * Umbra keeps MAIN/FSM/VM under chunk-paired translation. New logical pages are
 * born on their formula-derived slot 0; later rewrites can switch the active
 * slot through the shift metadata.
 */
static inline bool
UmbraForkUsesMapTranslation(ForkNumber forknum)
{
	return (forknum == MAIN_FORKNUM ||
			forknum == FSM_FORKNUM ||
			forknum == VISIBILITYMAP_FORKNUM);
}

static inline bool
UmbraForkIsAuxiliaryMapped(ForkNumber forknum)
{
	return (forknum == FSM_FORKNUM ||
			forknum == VISIBILITYMAP_FORKNUM);
}

/*
 * Chunk-paired physical layout.
 *
 * Each logical chunk owns three physical slots in the same data fork:
 *
 *   [slot 0 chunk][slot 1 chunk][slot 2 chunk]
 *
 * Each logical page is born on slot 0.  Later rewrites rotate the active slot
 * through the metadata stored for that logical block.
 */
#ifndef UMBRA_CHUNK_PAIRED_PAGES
#define UMBRA_CHUNK_PAIRED_PAGES 1U
#endif
#if UMBRA_CHUNK_PAIRED_PAGES <= 0
#error "UMBRA_CHUNK_PAIRED_PAGES must be greater than zero"
#endif

#define UMBRA_CHUNK_ACTIVE_SLOTS 3U

static inline bool
UmbraChunkActiveSlotIsValid(uint8 active_slot)
{
	return active_slot < UMBRA_CHUNK_ACTIVE_SLOTS;
}

static inline uint8
UmbraChunkNextActiveSlot(uint8 active_slot)
{
	Assert(UmbraChunkActiveSlotIsValid(active_slot));
	return (uint8) ((active_slot + 1) % UMBRA_CHUNK_ACTIVE_SLOTS);
}

static inline bool
UmbraChunkPairedSlotPblk(BlockNumber lblkno, uint8 active_slot,
						BlockNumber *pblkno)
{
	uint64		chunk_id;
	uint64		offset;
	uint64		pblk;

	Assert(pblkno != NULL);
	Assert(UmbraChunkActiveSlotIsValid(active_slot));

	chunk_id = (uint64) lblkno / UMBRA_CHUNK_PAIRED_PAGES;
	offset = (uint64) lblkno % UMBRA_CHUNK_PAIRED_PAGES;
	pblk = chunk_id *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES) +
		(uint64) active_slot * (uint64) UMBRA_CHUNK_PAIRED_PAGES +
		offset;

	if (pblk > (uint64) MaxBlockNumber)
		return false;

	*pblkno = (BlockNumber) pblk;
	return true;
}

static inline bool
UmbraChunkPairedBasePblk(BlockNumber lblkno, BlockNumber *pblkno)
{
	return UmbraChunkPairedSlotPblk(lblkno, 0, pblkno);
}

static inline bool
UmbraChunkPairedPhysicalCapacity(BlockNumber logical_nblocks,
								 BlockNumber *physical_nblocks)
{
	uint64		chunks;
	uint64		capacity;

	Assert(physical_nblocks != NULL);

	if (logical_nblocks == 0)
	{
		*physical_nblocks = 0;
		return true;
	}

	chunks = ((uint64) logical_nblocks + UMBRA_CHUNK_PAIRED_PAGES - 1) /
		UMBRA_CHUNK_PAIRED_PAGES;
	capacity = chunks *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES);

	if (capacity > (uint64) MaxBlockNumber + 1)
		return false;

	*physical_nblocks = (BlockNumber) capacity;
	return true;
}

static inline bool
UmbraChunkPairedCapacityForPblk(BlockNumber pblkno,
							   BlockNumber *physical_nblocks)
{
	uint64		chunk_id;
	uint64		capacity;

	Assert(physical_nblocks != NULL);

	chunk_id = (uint64) pblkno /
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES);
	capacity = (chunk_id + 1) *
		(UMBRA_CHUNK_ACTIVE_SLOTS * (uint64) UMBRA_CHUNK_PAIRED_PAGES);

	if (capacity > (uint64) MaxBlockNumber + 1)
		return false;

	*physical_nblocks = (BlockNumber) capacity;
	return true;
}

extern bool UmMetadataExists(SMgrRelation reln);
extern bool UmMetadataOpenOrCreate(SMgrRelation reln, bool isRedo, bool *created);
extern BlockNumber UmMetadataNblocks(SMgrRelation reln);
extern void UmMetadataRead(SMgrRelation reln, BlockNumber blkno, void *buffer);
extern void UmMetadataWrite(SMgrRelation reln, BlockNumber blkno,
							const void *buffer, bool skipFsync);
extern void UmMetadataWriteSuperblock(RelFileLocatorBackend rlocator,
									  const void *sector, bool skipFsync);
extern void UmMetadataExtend(SMgrRelation reln, BlockNumber blkno,
							 const void *buffer, bool skipFsync);
extern void UmMetadataImmediateSync(SMgrRelation reln);
extern void UmMetadataRegisterSync(SMgrRelation reln);
extern void UmMetadataUnlink(RelFileLocatorBackend rlocator, bool isRedo);

/* Umbra storage manager functionality (smgr callbacks). */
extern void uminit(void);
extern void umbeforeshmemexitcleanup(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern bool umcreatedballowswallog(void);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
extern void umsetmapstate(SMgrRelation reln, uint8 map_state);
extern void ummarkskipwalpending(SMgrRelation reln);
extern void umclearskipwalpending(SMgrRelation reln);
extern bool umisinternalfork(ForkNumber forknum);
extern void umcreaterelationmetadata(SMgrRelation reln);
extern void umredocreatefork(SMgrRelation reln, ForkNumber forknum,
							 XLogRecPtr lsn);
extern void umcheckpointdatabasetablespaces(Oid dbid, int ntablespaces,
											const Oid *tablespace_ids);
extern void uminvalidatedatabasetablespaces(Oid dbid, int ntablespaces,
											 const Oid *tablespace_ids);
extern void umcopyrelationmetadata(SMgrRelation src, SMgrRelation dst,
								   char relpersistence);
extern void umsyncrelationmetadata(SMgrRelation reln);
extern void umunlinkrelationmetadata(RelFileLocatorBackend rlocator,
									 bool isRedo);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool umexists(SMgrRelation reln, ForkNumber forknum);
extern void umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
extern void umextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void umzeroextend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, int nblocks, bool skipFsync);
extern void UmRebuildMapAndSuperblockForSkipWAL(SMgrRelation reln);
extern bool umprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, int nblocks);
extern uint32 ummaxcombine(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum);
extern void umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					void **buffers, BlockNumber nblocks);
extern void umstartreadv(PgAioHandle *ioh,
						 SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks);
extern void umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					 const void **buffers, BlockNumber nblocks, bool skipFsync);
extern void umwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern void umpretruncate(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber old_blocks, BlockNumber nblocks,
						  XLogRecPtr truncate_lsn);
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern bool umpreparependingsync(SMgrRelation reln);
extern bool umneedsrecoveryfsmvacuum(SMgrRelation reln);
extern int umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off);
extern int umsyncfiletag(const FileTag *ftag, char *path);
extern int umunlinkfiletag(const FileTag *ftag, char *path);
extern bool umfiletagmatches(const FileTag *ftag, const FileTag *candidate);

/*
 * Runtime semantic helpers.
 *
 * These consume Umbra access semantics (bypass/require-map/skip-pending) and
 * expose the runtime answers upper layers use directly.
 */
extern BlockNumber umphysicalblock(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber lblkno);
extern BlockNumber umnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber umnblocks_cached(SMgrRelation reln, ForkNumber forknum);

/*
 * Translation helpers used by WAL and replay code.
 *
 * These expose chunk-paired translation and active-slot updates only. Runtime
 * read-miss interpretation stays in umbra.c.
 */
extern bool UmUsesChunkPairedTranslation(SMgrRelation reln, ForkNumber forknum);
extern bool UmTranslationTryLookupPblkno(SMgrRelation reln, ForkNumber forknum,
										BlockNumber lblkno, BlockNumber *pblkno);
extern bool UmTranslationPhysicalBlockExists(SMgrRelation reln,
											 ForkNumber forknum,
											 BlockNumber lblkno);
extern bool UmTranslationSlotPhysicalBlockExists(SMgrRelation reln,
												 ForkNumber forknum,
												 BlockNumber lblkno,
												 uint8 slot);
extern bool UmCheckpointCaptureSlot(SMgrRelation reln, ForkNumber forknum,
									BlockNumber lblkno, uint8 *checkpoint_slot);
extern void UmCheckpointWriteSlot(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber lblkno, const void *buffer,
								  uint8 checkpoint_slot);
extern void UmCheckpointWritebackSlot(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber lblkno,
									  uint8 checkpoint_slot);
extern uint8 UmShiftChooseTargetSlot(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber lblkno, uint8 *source_slot);
extern void UmShiftSetActiveSlot(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber lblkno, uint8 active_slot,
								 XLogRecPtr map_lsn);
extern void UmRedoBeginShiftSourceSide(SMgrRelation reln, ForkNumber forknum,
									   BlockNumber lblkno,
									   uint8 source_slot);
extern void UmRedoEndShiftSourceSide(void);

#endif							/* UMBRA_H */
