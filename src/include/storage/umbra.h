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
 * Umbra keeps MAIN/FSM/VM under mapping translation, but only MAIN uses the
 * more involved page-WAL-owned first-born protocol. FSM/VM are auxiliary
 * mapped forks with a more explicit producer set and a simpler traced-extend
 * model.
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
 * Each logical chunk owns a base half and a shadow half in the same data fork:
 *
 *   [base chunk][shadow chunk]
 *
 * The current transition keeps using MAP entries, but all first materialization
 * in skip-WAL/base state must use this formula instead of lblk == pblk.
 */
#define UMBRA_CHUNK_PAIRED_PAGES 32U

static inline bool
UmbraChunkPairedBasePblk(BlockNumber lblkno, BlockNumber *pblkno)
{
	uint64		chunk_id;
	uint64		offset;
	uint64		pblk;

	Assert(pblkno != NULL);

	chunk_id = (uint64) lblkno / UMBRA_CHUNK_PAIRED_PAGES;
	offset = (uint64) lblkno % UMBRA_CHUNK_PAIRED_PAGES;
	pblk = chunk_id * (2 * (uint64) UMBRA_CHUNK_PAIRED_PAGES) + offset;

	if (pblk > (uint64) MaxBlockNumber)
		return false;

	*pblkno = (BlockNumber) pblk;
	return true;
}

static inline bool
UmbraChunkPairedShadowPblk(BlockNumber lblkno, BlockNumber *pblkno)
{
	uint64		chunk_id;
	uint64		offset;
	uint64		pblk;

	Assert(pblkno != NULL);

	chunk_id = (uint64) lblkno / UMBRA_CHUNK_PAIRED_PAGES;
	offset = (uint64) lblkno % UMBRA_CHUNK_PAIRED_PAGES;
	pblk = chunk_id * (2 * (uint64) UMBRA_CHUNK_PAIRED_PAGES) +
		(uint64) UMBRA_CHUNK_PAIRED_PAGES + offset;

	if (pblk > (uint64) MaxBlockNumber)
		return false;

	*pblkno = (BlockNumber) pblk;
	return true;
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
	capacity = chunks * (2 * (uint64) UMBRA_CHUNK_PAIRED_PAGES);

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

	chunk_id = (uint64) pblkno / (2 * (uint64) UMBRA_CHUNK_PAIRED_PAGES);
	capacity = (chunk_id + 1) * (2 * (uint64) UMBRA_CHUNK_PAIRED_PAGES);

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
extern bool umapplyreservedrange(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber firstblock, BlockNumber nblocks,
								 const BlockNumber *pblknos,
								 XLogRecPtr lsn, bool skipFsync);
extern void umzeroextend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, int nblocks, bool skipFsync);
extern void UmApplyReservedRangeRemap(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber firstblock, BlockNumber nblocks,
									  const BlockNumber *pblknos,
									  XLogRecPtr lsn, bool skipFsync);
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
 * MAP fact / mutation helpers used by WAL and replay code.
 *
 * These expose mapping facts and mapping-state updates only. Runtime read-miss
 * interpretation stays in umbra.c.
 */
extern void UmMapGetNewPbkno(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno, BlockNumber *new_pblkno,
							 BlockNumber *old_pblkno);
extern void UmMapReserveFreshPbkno(SMgrRelation reln, ForkNumber forknum,
								   BlockNumber lblkno,
								   BlockNumber *new_pblkno);
extern bool UmMapAccessAvailable(SMgrRelation reln, ForkNumber forknum);
extern bool UmWalOwnedRemapAvailable(SMgrRelation reln, ForkNumber forknum);
extern bool UmWalOwnedFirstbornAvailable(SMgrRelation reln, ForkNumber forknum,
										 BlockNumber lblkno);
extern bool UmMapUsesChunkPaired(SMgrRelation reln, ForkNumber forknum);
extern bool UmMapTryLookupPblkno(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber lblkno, BlockNumber *pblkno);
extern bool UmMapIsLogicalUnmaterialized(SMgrRelation reln, ForkNumber forknum,
										 BlockNumber lblkno);
extern void UmMapSetMapping(SMgrRelation reln, ForkNumber forknum,
							BlockNumber lblkno, BlockNumber new_pblkno,
							XLogRecPtr map_lsn);

#endif							/* UMBRA_H */
