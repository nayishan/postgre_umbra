/*-------------------------------------------------------------------------
 *
 * umbra.c
 *    Umbra storage manager: MAP translation + physical segment manager.
 *
 * Umbra implements a separate smgr that translates logical block numbers to
 * physical block numbers for mapped data forks.
 *
 * The mapping is stored in Umbra's internal metadata fork and cached in
 * shared memory by the MAP subsystem (src/backend/storage/map/).
 *
 * Layering in this file is intentionally split:
 *   1. access semantics: classify relation/fork access state
 *   2. mapping facts: consume MAP lookups/logical EOF/frontier facts
 *   3. execution: issue physical file I/O
 *
 * map.c owns facts and metadata storage actions. umbra.c owns runtime
 * interpretation of those facts for reads, writes, and publication.
 *
 * For correctness create/open establishes a steady-state base policy once:
 *   - permanent mapped relations: REQUIRE_MAP
 *   - temp/unlogged/direct relations: BYPASS_MAP
 *
 * INIT and MAP forks always use direct physical addressing.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/umbra_xlog.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/map.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "storage/umfile.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

typedef struct UmbraAccessState
{
	UmbraMapPolicy policy;
	bool		map_available;
} UmbraAccessState;

typedef struct UmbraRelationState
{
	UmbraFileContext *file_ctx;	/* cached borrow from umfile registry */
	uint8		map_state;
} UmbraRelationState;

typedef struct UmbraAccessLookupState
{
	bool		have_logical_nblocks;
	BlockNumber logical_nblocks;
} UmbraAccessLookupState;

typedef enum UmbraAccessResolveMode
{
	UMBRA_ACCESS_RESOLVE_READ,
	UMBRA_ACCESS_RESOLVE_WRITE,
	UMBRA_ACCESS_RESOLVE_WRITEBACK
} UmbraAccessResolveMode;

typedef enum UmbraAccessResolveResult
{
	UMBRA_ACCESS_RESOLVED_PBLK,
	UMBRA_ACCESS_RESOLVED_ZERO,
	UMBRA_ACCESS_RESOLVED_SKIP
} UmbraAccessResolveResult;

typedef struct UmbraMappedBirthResult
{
	BlockNumber pblkno;
	bool		mapping_published;
} UmbraMappedBirthResult;

#define UMBRA_WRITE_BARRIER_WAIT_RETRIES 10000
#define UMBRA_WRITE_BARRIER_WAIT_USEC 1000

static inline UmbraRelationState *
um_state_lookup(SMgrRelation reln)
{
	return (UmbraRelationState *) reln->smgr_private;
}

static inline UmbraRelationState *
um_state_acquire(SMgrRelation reln)
{
	UmbraRelationState *state = (UmbraRelationState *) reln->smgr_private;

	if (state == NULL)
	{
		state = MemoryContextAllocZero(TopMemoryContext, sizeof(*state));
		state->file_ctx = umfile_ctx_acquire(reln->smgr_rlocator);
		state->map_state = UMBRA_MAP_POLICY_UNKNOWN;
		reln->smgr_private = state;
	}
	else if (state->file_ctx == NULL)
		state->file_ctx = umfile_ctx_acquire(reln->smgr_rlocator);

	return state;
}

static inline UmbraFileContext *
um_ctx_acquire(SMgrRelation reln)
{
	return um_state_acquire(reln)->file_ctx;
}

static inline UmbraFileContext *
um_ctx_lookup(SMgrRelation reln)
{
	UmbraRelationState *state = um_state_lookup(reln);

	return state != NULL ? state->file_ctx : NULL;
}

static inline UmbraMapPolicy
um_map_state_cached(SMgrRelation reln)
{
	UmbraRelationState *state = um_state_lookup(reln);

	if (state == NULL)
		return UMBRA_MAP_POLICY_UNKNOWN;

	return (UmbraMapPolicy) state->map_state;
}

static inline void
um_set_cached_map_state(SMgrRelation reln, UmbraMapPolicy map_state)
{
	UmbraRelationState *state;

	Assert(map_state != UMBRA_MAP_POLICY_UNKNOWN);
	state = um_state_acquire(reln);
	state->map_state = (uint8) map_state;
}

static inline void
um_state_destroy(SMgrRelation reln)
{
	UmbraRelationState *state = um_state_lookup(reln);

	if (state == NULL)
		return;

	reln->smgr_private = NULL;
	pfree(state);
}

/* Runtime access semantics. */
static UmbraMapPolicy um_map_policy_for_access(SMgrRelation reln,
												   ForkNumber forknum);
static UmbraAccessState um_classify_access(SMgrRelation reln,
										   ForkNumber forknum);
static void um_report_unmapped_map_entry(SMgrRelation reln,
										 ForkNumber forknum,
										 const UmbraAccessState *access,
										 BlockNumber lblkno);
static BlockNumber umnblocks_for_access(SMgrRelation reln, ForkNumber forknum,
										const UmbraAccessState *access);
static bool um_lblk_precedes_logical_eof_for_access(SMgrRelation reln,
													ForkNumber forknum,
													const UmbraAccessState *access,
													UmbraAccessLookupState *lookup_state,
													BlockNumber lblkno);
static bool um_is_logical_unmaterialized_for_access(SMgrRelation reln,
													ForkNumber forknum,
													const UmbraAccessState *access,
													BlockNumber lblkno);
static UmbraAccessResolveResult um_resolve_lblk_for_access(SMgrRelation reln,
														   ForkNumber forknum,
														   const UmbraAccessState *access,
														   UmbraAccessLookupState *lookup_state,
														   BlockNumber lblkno,
														   UmbraAccessResolveMode mode,
														   BlockNumber *pblkno);
static BlockNumber um_resolve_mapped_read_run(SMgrRelation reln,
											  ForkNumber forknum,
											  const UmbraAccessState *access,
											  UmbraAccessLookupState *lookup_state,
											  BlockNumber blocknum,
											  BlockNumber maxblocks,
											  BlockNumber *start_pblk);
static void um_complete_zero_readv(PgAioHandle *ioh, SMgrRelation reln,
								   ForkNumber forknum, BlockNumber blocknum,
								   void *buffer);
static bool um_is_stale_post_truncate_lblk_for_access(SMgrRelation reln,
													  ForkNumber forknum,
													  const UmbraAccessState *access,
													  UmbraAccessLookupState *lookup_state,
													  BlockNumber lblkno);
static bool um_is_stale_post_truncate_lblk_with_eof(ForkNumber forknum,
											BlockNumber logical_nblocks,
											BlockNumber lblkno);

/* MAP facts and storage actions consumed by Umbra semantics. */
static void um_ensure_datafork_batch_ready_for_access(SMgrRelation reln,
													  ForkNumber forknum,
													  const UmbraAccessState *access,
													  BlockNumber pblkno,
													  bool skipFsync);
static void um_reserve_fresh_pblkno_for_access(SMgrRelation reln,
											   ForkNumber forknum,
											   const UmbraAccessState *access,
											   BlockNumber lblkno,
											   BlockNumber *new_pblkno);
static bool um_fork_uses_map_translation(ForkNumber forknum);
static bool um_fork_uses_wal_owned_firstborn(ForkNumber forknum);
static bool um_mapped_exists_from_super(SMgrRelation reln, ForkNumber forknum);
static UmbraMapPolicy um_open_map_state(SMgrRelation reln);
static bool um_state_uses_map(UmbraMapPolicy state);
static bool um_state_requires_durable_sync(UmbraMapPolicy state);
static bool um_relation_requires_durable_sync(SMgrRelation reln);
static UmbraMappedBirthResult um_publish_mapped_birth(SMgrRelation reln,
													  ForkNumber forknum,
													  const UmbraAccessState *access,
													  BlockNumber lblkno,
													  bool allow_wal_owned_firstborn);
static void um_filetag_path(const FileTag *ftag, char *path);

bool
UmMetadataExists(SMgrRelation reln)
{
	return umfile_exists(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM,
						 UMFILE_EXISTS_DENSE);
}

bool
UmMetadataOpenOrCreate(SMgrRelation reln, bool isRedo, bool *created)
{
	return umfile_open_or_create(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM,
								 isRedo, created);
}

BlockNumber
UmMetadataNblocks(SMgrRelation reln)
{
	return umfile_nblocks(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM,
						  UMFILE_NBLOCKS_DENSE);
}

void
UmMetadataRead(SMgrRelation reln, BlockNumber blkno, void *buffer)
{
	umfile_readv(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM, blkno,
				 &buffer, 1);
}

void
UmMetadataWrite(SMgrRelation reln, BlockNumber blkno, const void *buffer,
				bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	umfile_ctx_write(ctx, UMBRA_METADATA_FORKNUM, blkno,
					 buffer, BLCKSZ, skipFsync);
	umfile_ctx_register_dirty(ctx, UMBRA_METADATA_FORKNUM, blkno,
							  skipFsync,
							  RelFileLocatorBackendIsTemp(reln->smgr_rlocator));
}

void
UmMetadataWriteSuperblock(RelFileLocatorBackend rlocator, const void *sector,
						  bool skipFsync)
{
	UmbraFileContext *ctx = umfile_ctx_acquire(rlocator);

	/*
	 * Superblock checkpoint flush can run while holding MapSuperEntry->lock,
	 * so it must not reopen the relation via smgr/umopen and recurse into MAP
	 * state lookup.
	 */
	umfile_ctx_write(ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
					 sector, MAP_SUPERBLOCK_SIZE, skipFsync);
	umfile_ctx_register_dirty(ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
							  skipFsync,
							  RelFileLocatorBackendIsTemp(rlocator));
}

void
UmMetadataExtend(SMgrRelation reln, BlockNumber blkno, const void *buffer,
				 bool skipFsync)
{
	umfile_extend(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM, blkno,
				  buffer, skipFsync);
}

void
UmMetadataImmediateSync(SMgrRelation reln)
{
	umimmedsync(reln, UMBRA_METADATA_FORKNUM);
}

void
UmMetadataRegisterSync(SMgrRelation reln)
{
	umimmedsync(reln, UMBRA_METADATA_FORKNUM);
}

void
UmMetadataUnlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

static void
um_ensure_datafork_batch_ready_for_access(SMgrRelation reln,
										  ForkNumber forknum,
										  const UmbraAccessState *access,
										  BlockNumber pblkno,
										  bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (!access->map_available)
		return;

	if (pblkno == InvalidBlockNumber)
		return;

	if (pblkno + 1 == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("physical block overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum)));

	(void) MapSBlockEnsurePhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										  forknum, pblkno + 1, skipFsync);
}

void
UmApplyReservedRangeRemap(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber firstblock, BlockNumber nblocks,
						  const BlockNumber *pblknos,
						  XLogRecPtr lsn, bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber max_lblkno;
	BlockNumber max_pblkno = InvalidBlockNumber;

	Assert(nblocks > 0);
	Assert(pblknos != NULL);

	max_lblkno = firstblock + nblocks - 1;
	if (max_lblkno < firstblock)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("logical range overflow for relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum)));

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber pblk = pblknos[i];

		if (max_pblkno == InvalidBlockNumber || pblk > max_pblkno)
			max_pblkno = pblk;
	}

	if (max_pblkno != InvalidBlockNumber)
	{
		(void) MapSBlockEnsurePhysicalNblocks(ctx, reln->smgr_rlocator.locator,
											  forknum, max_pblkno + 1, skipFsync);
	}

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = firstblock + i;
		BlockNumber pblk = pblknos[i];

		MapSetMapping(ctx, reln->smgr_rlocator.locator, forknum, lblk, pblk, lsn);
	}

	if (max_pblkno != InvalidBlockNumber)
	{
		MapSBlockBumpNextFreePhysBlock(ctx, reln->smgr_rlocator.locator,
									   forknum, max_pblkno + 1, lsn);
		MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
									 forknum, max_pblkno + 1, lsn);
		for (BlockNumber i = 0; i < nblocks; i++)
			MapInflightRelease(reln->smgr_rlocator.locator, forknum,
							   firstblock + i);
	}
	MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									forknum, max_lblkno + 1, lsn);
}

bool
umapplyreservedrange(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber firstblock, BlockNumber nblocks,
					 const BlockNumber *pblknos,
					 XLogRecPtr lsn, bool skipFsync)
{
	UmApplyReservedRangeRemap(reln, forknum, firstblock, nblocks,
							  pblknos, lsn, skipFsync);
	return true;
}

/*
 * Create and initialize MAP fork for a relation.
 *
 * Keep creation O(1): create/open the MAP fork and write only the superblock
 * sector. Regular MAP pages are synthesized on first access and written
 * lazily by the MAP layer.
 */

static void
ummapcreate(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	bool		newly_created;

	/*
	 * Open existing MAP fork or create new one. During redo, EEXIST is
	 * acceptable and we reuse the existing file.
	 */
	UmMetadataOpenOrCreate(reln, true /* isRedo */, &newly_created);

	/* Existing MAP fork does not need re-initialization. */
	if (!newly_created)
		return;

	/*
	 * Superblock identity is established locally here and reconstructed during
	 * redo from relation creation, so no separate Umbra rmgr record is needed.
	 */
	Assert(um_map_state_cached(reln) != UMBRA_MAP_POLICY_UNKNOWN);
	MapSBlockInit(ctx, reln->smgr_rlocator.locator, InvalidXLogRecPtr);

	/*
	 * Keep metadata fork durability aligned with main-fork create semantics:
	 * the file is created now, while checkpoint/restartpoint owns syncing it.
	 */
	if (!SmgrIsTemp(reln))
		UmMetadataRegisterSync(reln);
}

bool
umisinternalfork(ForkNumber forknum)
{
	return forknum == UMBRA_METADATA_FORKNUM;
}

bool
umcreatedballowswallog(void)
{
	return false;
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	umsetmapstate(reln, needs_wal ?
				  UMBRA_MAP_POLICY_REQUIRE_MAP :
				  UMBRA_MAP_POLICY_BYPASS_MAP);
	if (needs_wal)
		umcreaterelationmetadata(reln);
}

void
umcreaterelationmetadata(SMgrRelation reln)
{
	ummapcreate(reln);
}

void
umredocreatefork(SMgrRelation reln, ForkNumber forknum, XLogRecPtr lsn)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (forknum == MAIN_FORKNUM)
	{
		umsetmapstate(reln, UMBRA_MAP_POLICY_REQUIRE_MAP);
		umcreaterelationmetadata(reln);
		return;
	}

	if (!UmbraForkUsesMapTranslation(forknum))
	{
		umsetmapstate(reln, UMBRA_MAP_POLICY_BYPASS_MAP);
		return;
	}

	umsetmapstate(reln, UMBRA_MAP_POLICY_REQUIRE_MAP);
	if (!UmMetadataExists(reln))
		umcreaterelationmetadata(reln);

	if (UmbraForkIsAuxiliaryMapped(forknum))
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, 0, lsn);
}

void
umcheckpointdatabasetablespaces(Oid dbid, int ntablespaces,
								const Oid *tablespace_ids)
{
	MapCheckpointDatabaseTablespaces(dbid, ntablespaces, tablespace_ids);
}

void
uminvalidatedatabasetablespaces(Oid dbid, int ntablespaces,
								const Oid *tablespace_ids)
{
	MapInvalidateDatabaseTablespaces(dbid, ntablespaces, tablespace_ids);
}

void
umcopyrelationmetadata(SMgrRelation src, SMgrRelation dst, char relpersistence)
{
	BlockNumber src_nblocks;
	BlockNumber dst_nblocks;
	PGIOAlignedBlock pagebuf;

	if (relpersistence != RELPERSISTENCE_PERMANENT)
		return;

	if (!UmMetadataExists(src))
		return;

	ummapcreate(dst);

	src_nblocks = UmMetadataNblocks(src);
	dst_nblocks = UmMetadataNblocks(dst);

	for (BlockNumber blkno = 0; blkno < src_nblocks; blkno++)
	{
		UmMetadataRead(src, blkno, pagebuf.data);
		if (blkno < dst_nblocks)
			UmMetadataWrite(dst, blkno, pagebuf.data, true);
		else
			UmMetadataExtend(dst, blkno, pagebuf.data, true);
	}

	UmMetadataImmediateSync(dst);
}

void
umsyncrelationmetadata(SMgrRelation reln)
{
	if (!UmMetadataExists(reln))
		return;

	UmMetadataImmediateSync(reln);
}

void
umunlinkrelationmetadata(RelFileLocatorBackend rlocator, bool isRedo)
{
	MapInvalidateRelation(rlocator.locator);
	UmMetadataUnlink(rlocator, isRedo);
}

void
uminit(void)
{
	umfile_init();
	MapBackendInit();
}

void
umbeforeshmemexitcleanup(void)
{
	MapBackendExitCleanup();
}

void
umopen(SMgrRelation reln)
{
	(void) um_ctx_acquire(reln);

	if (um_map_state_cached(reln) != UMBRA_MAP_POLICY_UNKNOWN)
		return;

	um_set_cached_map_state(reln, um_open_map_state(reln));
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_ctx_lookup(reln);

	if (ctx != NULL)
		umfile_ctx_close_fork(ctx, forknum);
}

void
umdestroy(SMgrRelation reln)
{
	umfile_ctx_forget(reln->smgr_rlocator);
	um_state_destroy(reln);
}

void
umsetmapstate(SMgrRelation reln, uint8 map_state)
{
	Assert(map_state != UMBRA_MAP_POLICY_UNKNOWN);
	Assert(map_state <= UMBRA_MAP_POLICY_REQUIRE_MAP);
	um_set_cached_map_state(reln, (UmbraMapPolicy) map_state);
}

static bool
um_fork_uses_map_translation(ForkNumber forknum)
{
	return UmbraForkUsesMapTranslation(forknum);
}

/*
 * MAIN fork is the only one that uses page-WAL-owned first-born remap.
 *
 * FSM/VM growth is much more structured: their extend/truncate producers are
 * concentrated in a few helper paths, so we keep them on explicit mapping
 * publication rather than tying first-born ownership to arbitrary page WAL.
 */
static bool
um_fork_uses_wal_owned_firstborn(ForkNumber forknum)
{
	return UmbraForkUsesMapTranslation(forknum) &&
		!UmbraForkIsAuxiliaryMapped(forknum);
}

static bool
um_mapped_exists_from_super(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	Assert(um_fork_uses_map_translation(forknum));

	if (!UmMetadataExists(reln))
		return false;

	return MapSBlockForkExists(ctx, reln->smgr_rlocator.locator, forknum);
}

static UmbraMapPolicy
um_open_map_state(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (RelFileLocatorBackendIsTemp(reln->smgr_rlocator) ||
		!UmMetadataExists(reln))
		return UMBRA_MAP_POLICY_BYPASS_MAP;

	if (MapSBlockIsSkipWalPending(ctx,
								  reln->smgr_rlocator.locator))
		return UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP;

	return UMBRA_MAP_POLICY_REQUIRE_MAP;
}

static bool
um_state_uses_map(UmbraMapPolicy state)
{
	return state == UMBRA_MAP_POLICY_REQUIRE_MAP;
}

static bool
um_state_requires_durable_sync(UmbraMapPolicy state)
{
	return state == UMBRA_MAP_POLICY_REQUIRE_MAP ||
		state == UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP;
}

/*
 * Commit-time durability is a separate question from ordinary access state.
 *
 * log_newpage_range() can describe only physical page images.  Once a relation
 * either actively uses MAP translation or already owns a MAP fork on disk, its
 * durable transition must go through the Umbra-aware flush+sync path instead
 * of plain FPI-range logging.
 */
static bool
um_relation_requires_durable_sync(SMgrRelation reln)
{
	UmbraMapPolicy state;

	state = um_map_state_cached(reln);
	Assert(state != UMBRA_MAP_POLICY_UNKNOWN);

	return um_state_requires_durable_sync(state) || UmMetadataExists(reln);
}

static UmbraMapPolicy
um_map_policy_for_access(SMgrRelation reln, ForkNumber forknum)
{
	if (!um_fork_uses_map_translation(forknum))
		return UMBRA_MAP_POLICY_BYPASS_MAP;

	Assert(um_map_state_cached(reln) != UMBRA_MAP_POLICY_UNKNOWN);
	return um_map_state_cached(reln);
}

/*
 * um_map_fork_available() -- whether MAP fork can be used for this access.
 *
 * If MAP fork is absent:
 * - optional relations fall back to direct mapping (lblkno==pblkno)
 * - required relations throw ERROR
 */
static UmbraAccessState
um_classify_access(SMgrRelation reln, ForkNumber forknum)
{
	UmbraAccessState state;

	state.policy = um_map_policy_for_access(reln, forknum);
	state.map_available = um_state_uses_map(state.policy);

	return state;
}

static void
um_report_unmapped_map_entry(SMgrRelation reln, ForkNumber forknum,
							 const UmbraAccessState *access,
							 BlockNumber lblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber logical_nblocks = InvalidBlockNumber;
	BlockNumber map_blkno;
	BlockNumber fork_page_idx;
	int			entry_idx;
	uint64		blkno64;

	Assert(access->map_available);

	(void) MapSBlockTryGetLogicalNblocks(ctx,
										 reln->smgr_rlocator.locator,
										 forknum, &logical_nblocks);

	fork_page_idx = lblkno / MAP_ENTRIES_PER_PAGE;
	entry_idx = lblkno % MAP_ENTRIES_PER_PAGE;

	switch (forknum)
	{
		case FSM_FORKNUM:
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				(uint64) fork_page_idx * (uint64) MAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				(uint64) fork_page_idx * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
		{
			uint64 group_page_idx = (uint64) fork_page_idx;
			uint64 group_no = group_page_idx / (uint64) MAP_GROUP_MAIN_PAGES;

			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) MAP_GROUP_FSM_PAGES +
				(uint64) MAP_GROUP_VM_PAGES +
				(group_page_idx % (uint64) MAP_GROUP_MAIN_PAGES);
			break;
		}

		default:
			elog(ERROR, "unsupported fork number %d in map miss report",
				 (int) forknum);
			pg_unreachable();
	}

	map_blkno = (BlockNumber) blkno64;

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("MAP entry is unmapped: rel=%u/%u/%u fork=%d lblk=%u logical_nblocks=%u map_page=%u entry_idx=%d",
					reln->smgr_rlocator.locator.spcOid,
					reln->smgr_rlocator.locator.dbOid,
					reln->smgr_rlocator.locator.relNumber,
					forknum,
					lblkno,
					logical_nblocks,
					map_blkno,
					entry_idx)));
}

/*
 * Build identity MAP metadata for relations that stayed on direct lblk==pblk
 * access during a skip-WAL window and now need durable mapped state.
 */
void
UmRebuildMapAndSuperblockForSkipWAL(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	xl_umbra_skip_wal_dense_map_entry apply_entries[MAX_FORKNUM + 1];
	xl_umbra_skip_wal_dense_map_entry wal_entries[MAX_FORKNUM + 1];
	uint16		apply_count = 0;
	uint16		wal_count = 0;
	XLogRecPtr	map_lsn = InvalidXLogRecPtr;
	bool		wal_insert_enabled;

	/*
	 * Rebuild assumes the relation stayed on direct lblk==pblk access during
	 * the skip-WAL window. No running path may consume MAP state before this
	 * durable-transition rebuild runs.
	 */
	Assert(RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator));

	MapInvalidateRelation(reln->smgr_rlocator.locator);
	Assert(UmMetadataExists(reln));

	for (ForkNumber forknum = MAIN_FORKNUM; forknum <= VISIBILITYMAP_FORKNUM; forknum++)
	{
		BlockNumber nblocks;

		if (!UmbraForkUsesMapTranslation(forknum))
			continue;

		if (!umfile_exists(ctx, forknum, UMFILE_EXISTS_DENSE))
			continue;

		nblocks = umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);
		apply_entries[apply_count].forknum = forknum;
		apply_entries[apply_count].nblocks = nblocks;
		apply_count++;

		/*
		 * The redo anchor records dense [0, nblocks) mapping. Empty forks
		 * don't need an anchor and may correspond to zero-length metadata left
		 * by aborted storage operations.
		 */
		if (nblocks > 0)
		{
			wal_entries[wal_count].forknum = forknum;
			wal_entries[wal_count].nblocks = nblocks;
			wal_count++;
		}
	}

	wal_insert_enabled =
		XLogInsertAllowed() &&
		!IsBootstrapProcessingMode() &&
		!IsInitProcessingMode();

	if (wal_count > 0 && wal_insert_enabled)
		map_lsn = log_umbra_skip_wal_dense_map(reln->smgr_rlocator.locator,
											   wal_count, wal_entries);

	for (uint16 i = 0; i < apply_count; i++)
	{
		ForkNumber	forknum = apply_entries[i].forknum;
		BlockNumber nblocks = apply_entries[i].nblocks;
		XLogRecPtr	fork_lsn = nblocks > 0 ? map_lsn : InvalidXLogRecPtr;

		for (BlockNumber lblk = 0; lblk < nblocks; lblk++)
			MapSetMapping(ctx, reln->smgr_rlocator.locator, forknum,
						  lblk, lblk, fork_lsn);

		if (nblocks > 0)
		{
			MapSBlockBumpNextFreePhysBlock(ctx, reln->smgr_rlocator.locator,
										   forknum, nblocks, fork_lsn);
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum, nblocks, fork_lsn);
		}
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, nblocks, fork_lsn);
	}
}

void
ummarkskipwalpending(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (!UmMetadataExists(reln))
		ummapcreate(reln);

	MapSBlockSetSkipWalPending(ctx, reln->smgr_rlocator.locator,
							   true, InvalidXLogRecPtr);
	um_set_cached_map_state(reln, UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP);
}

void
umclearskipwalpending(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (!UmMetadataExists(reln))
		return;

	MapSBlockSetSkipWalPending(ctx, reln->smgr_rlocator.locator,
							   false, InvalidXLogRecPtr);
	um_set_cached_map_state(reln, UMBRA_MAP_POLICY_REQUIRE_MAP);
	/*
	 * Clearing the durable-transition flag must itself be durable before the
	 * transaction can be considered to have left the skip-WAL state.  If we
	 * only dirty the shared superblock copy here, a crash before checkpoint
	 * would resurrect SKIP_WAL_PENDING from disk on restart.
	 */
	umimmedsync(reln, UMBRA_METADATA_FORKNUM);
}

static bool
um_lblk_precedes_logical_eof_for_access(SMgrRelation reln, ForkNumber forknum,
										const UmbraAccessState *access,
										UmbraAccessLookupState *lookup_state,
										BlockNumber lblkno)
{
	BlockNumber logical_nblocks;

	if (!access->map_available)
		return false;

	if (!um_fork_uses_map_translation(forknum))
		return false;

	if (lookup_state != NULL && lookup_state->have_logical_nblocks)
		logical_nblocks = lookup_state->logical_nblocks;
	else
	{
		logical_nblocks = umnblocks_for_access(reln, forknum, access);
		if (lookup_state != NULL)
		{
			lookup_state->logical_nblocks = logical_nblocks;
			lookup_state->have_logical_nblocks = true;
		}
	}

	return lblkno < logical_nblocks;
}

static bool
um_is_logical_unmaterialized_for_access(SMgrRelation reln, ForkNumber forknum,
										const UmbraAccessState *access,
										BlockNumber lblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber pblkno;

	if (!um_lblk_precedes_logical_eof_for_access(reln, forknum, access, NULL,
												 lblkno))
		return false;

	return !MapTryLookup(ctx, reln->smgr_rlocator.locator,
						 forknum, lblkno, &pblkno);
}

static void
um_reserve_fresh_pblkno_for_access(SMgrRelation reln, ForkNumber forknum,
								   const UmbraAccessState *access,
								   BlockNumber lblkno,
								   BlockNumber *new_pblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	Assert(new_pblkno != NULL);

	if (!access->map_available)
	{
		*new_pblkno = lblkno;
		return;
	}

	if (!MapReserveFreshPblkno(ctx, reln->smgr_rlocator.locator,
							   forknum, lblkno, new_pblkno))
		elog(ERROR,
			 "failed to reserve fresh physical block for relation %u/%u/%u fork %d blk %u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber,
				 forknum, lblkno);
}

static UmbraAccessResolveResult
um_resolve_lblk_for_read(SMgrRelation reln, ForkNumber forknum,
						 const UmbraAccessState *access,
						 UmbraAccessLookupState *lookup_state,
						 BlockNumber lblkno)
{
	Assert(lookup_state != NULL);

	if (!lookup_state->have_logical_nblocks)
	{
		lookup_state->logical_nblocks =
			umnblocks_for_access(reln, forknum, access);
		lookup_state->have_logical_nblocks = true;
	}

	if (InRecovery && UmbraForkIsAuxiliaryMapped(forknum) &&
		lookup_state->logical_nblocks != InvalidBlockNumber &&
		um_is_stale_post_truncate_lblk_with_eof(forknum,
												lookup_state->logical_nblocks,
												lblkno))
	{
		return UMBRA_ACCESS_RESOLVED_ZERO;
	}

	if (lookup_state->logical_nblocks != InvalidBlockNumber &&
		lblkno < lookup_state->logical_nblocks)
	{
		return UMBRA_ACCESS_RESOLVED_ZERO;
	}

	{
		um_report_unmapped_map_entry(reln, forknum, access, lblkno);
	}
	pg_unreachable();
}

static UmbraAccessResolveResult
um_resolve_lblk_for_write(SMgrRelation reln, ForkNumber forknum,
						  const UmbraAccessState *access,
						  UmbraAccessLookupState *lookup_state,
						  BlockNumber lblkno, BlockNumber *pblkno)
{
	(void) lookup_state;
	(void) pblkno;

	um_report_unmapped_map_entry(reln, forknum, access, lblkno);
	pg_unreachable();
}

static UmbraAccessResolveResult
um_resolve_lblk_for_writeback(SMgrRelation reln, ForkNumber forknum,
							  const UmbraAccessState *access,
							  UmbraAccessLookupState *lookup_state,
							  BlockNumber lblkno, BlockNumber *pblkno)
{
	if (um_lblk_precedes_logical_eof_for_access(reln, forknum, access,
												lookup_state, lblkno))
	{
		if (MapInflightLookupOwnedPblk(reln->smgr_rlocator.locator,
									   forknum, lblkno, pblkno))
			return UMBRA_ACCESS_RESOLVED_PBLK;
		return UMBRA_ACCESS_RESOLVED_SKIP;
	}

	if (um_is_stale_post_truncate_lblk_for_access(reln, forknum, access,
												  lookup_state, lblkno))
		return UMBRA_ACCESS_RESOLVED_SKIP;

	um_report_unmapped_map_entry(reln, forknum, access, lblkno);
	pg_unreachable();
}

static UmbraAccessResolveResult
um_resolve_lblk_for_access(SMgrRelation reln, ForkNumber forknum,
						   const UmbraAccessState *access,
						   UmbraAccessLookupState *lookup_state,
						   BlockNumber lblkno,
						   UmbraAccessResolveMode mode,
						   BlockNumber *pblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	bool		found;

	Assert(pblkno != NULL);

	if (!access->map_available)
	{
		*pblkno = lblkno;
		return UMBRA_ACCESS_RESOLVED_PBLK;
	}

	if (mode == UMBRA_ACCESS_RESOLVE_READ)
	{
		found = MapTryLookup(ctx, reln->smgr_rlocator.locator,
							 forknum, lblkno, pblkno);
	}
	else
	{
		found = MapTryLookup(ctx, reln->smgr_rlocator.locator,
							 forknum, lblkno, pblkno);
	}

	if (found)
		return UMBRA_ACCESS_RESOLVED_PBLK;

	switch (mode)
	{
			case UMBRA_ACCESS_RESOLVE_READ:
				return um_resolve_lblk_for_read(reln, forknum, access,
												 lookup_state, lblkno);
			case UMBRA_ACCESS_RESOLVE_WRITE:
				return um_resolve_lblk_for_write(reln, forknum, access,
												  lookup_state, lblkno, pblkno);
			case UMBRA_ACCESS_RESOLVE_WRITEBACK:
				return um_resolve_lblk_for_writeback(reln, forknum, access,
													  lookup_state, lblkno, pblkno);
		}

	pg_unreachable();
}

/*
 * Resolve the longest read prefix beginning at blocknum whose translated
 * physical blocks form one contiguous run within a single segment.
 */
static BlockNumber
um_resolve_mapped_read_run(SMgrRelation reln, ForkNumber forknum,
						   const UmbraAccessState *access,
						   UmbraAccessLookupState *lookup_state,
						   BlockNumber blocknum, BlockNumber maxblocks,
						   BlockNumber *start_pblk)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	run_blocks;
	BlockNumber	pblk;

	Assert(access != NULL);
	Assert(start_pblk != NULL);
	Assert(maxblocks > 0);

	run_blocks = MapTryLookupPblkRun(ctx, reln->smgr_rlocator.locator,
									 forknum, blocknum, maxblocks,
									 start_pblk);
	if (run_blocks > 0)
		return run_blocks;

	if (um_resolve_lblk_for_access(reln, forknum, access, lookup_state,
								   blocknum, UMBRA_ACCESS_RESOLVE_READ,
								   &pblk) == UMBRA_ACCESS_RESOLVED_PBLK)
	{
		*start_pblk = pblk;
		return 1;
	}

	return 0;
}

static void
um_materialize_pblk_zero_runs(UmbraFileContext *ctx, ForkNumber forknum,
							  const BlockNumber *pblknos, BlockNumber nblocks,
							  bool skipFsync)
{
	BlockNumber	run_start_pblk = InvalidBlockNumber;
	BlockNumber	run_blocks = 0;

	Assert(ctx != NULL);
	Assert(pblknos != NULL);

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber pblk = pblknos[i];

		if (run_blocks == 0)
		{
			run_start_pblk = pblk;
			run_blocks = 1;
		}
		else if (pblk == run_start_pblk + run_blocks)
		{
			run_blocks++;
		}
		else
		{
			umfile_zeroextend(ctx, forknum, run_start_pblk,
							  (int) run_blocks, skipFsync);
			run_start_pblk = pblk;
			run_blocks = 1;
		}
	}

	if (run_blocks > 0)
		umfile_zeroextend(ctx, forknum, run_start_pblk,
						  (int) run_blocks, skipFsync);
}

static bool
um_pblk_run_is_contiguous(const BlockNumber *pblknos, BlockNumber nblocks)
{
	Assert(pblknos != NULL);
	Assert(nblocks > 0);

	for (BlockNumber i = 1; i < nblocks; i++)
	{
		if (pblknos[i] != pblknos[0] + i)
			return false;
	}

	return true;
}

static bool
um_try_pure_firstborn_range_remap_zeroextend(SMgrRelation reln, ForkNumber forknum,
											 const UmbraAccessState *access,
											 BlockNumber blocknum,
											 BlockNumber nblocks,
											 bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber *pblknos;
	xl_umbra_range_remap_entry *entries;
	bool		wal_insert_enabled;
	bool		applied = false;

	Assert(access != NULL);
	Assert(access->map_available);
	Assert(nblocks > 0);

	/*
	 * Try to collapse an EOF zeroextend range into one or more RANGE_REMAP
	 * records. RANGE_REMAP carries only new pblk ownership, so this helper is
	 * deliberately all-or-nothing and only accepts pure first-born ranges.
	 * Recovery consumes authoritative remap WAL instead of synthesizing new
	 * range ownership locally.
	 */
	if (InRecovery || nblocks < 2)
		return false;

	pblknos = palloc(sizeof(BlockNumber) * nblocks);
	entries = palloc(sizeof(xl_umbra_range_remap_entry) * nblocks);

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = blocknum + i;
		BlockNumber pblk;

		if (MapTryLookup(ctx, reln->smgr_rlocator.locator, forknum, lblk, &pblk) ||
			MapInflightLookupOwnedPblk(reln->smgr_rlocator.locator,
									   forknum, lblk, &pblk))
		{
			/*
			 * Normal EOF extension should not get here.  Treat existing or
			 * in-flight ownership as a compatibility fallback condition, not as
			 * a mixed-range batching opportunity.
			 */
			pfree(entries);
			pfree(pblknos);
			return false;
		}
	}

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = blocknum + i;

		if (!MapReserveFreshPblkno(ctx, reln->smgr_rlocator.locator,
								   forknum, lblk, &pblknos[i]))
		{
			for (BlockNumber j = 0; j < i; j++)
				MapInflightRelease(reln->smgr_rlocator.locator,
								   forknum, blocknum + j);
			pfree(entries);
			pfree(pblknos);
			return false;
		}
		entries[i].lblkno = lblk;
		entries[i].new_pblkno = pblknos[i];
	}

	wal_insert_enabled =
		XLogInsertAllowed() &&
		!IsBootstrapProcessingMode() &&
		!IsInitProcessingMode();

	PG_TRY();
	{
		BlockNumber done = 0;

		while (done < nblocks)
		{
			BlockNumber chunk_blocks = Min(nblocks - done,
										   (BlockNumber) UINT16_MAX);
			XLogRecPtr	map_lsn = InvalidXLogRecPtr;

			if (wal_insert_enabled)
			{
				if (um_pblk_run_is_contiguous(pblknos + done, chunk_blocks))
					map_lsn = log_umbra_range_remap_compact(
						reln->smgr_rlocator.locator, forknum,
						blocknum + done, pblknos[done], (uint16) chunk_blocks);
				else
					map_lsn = log_umbra_range_remap(
						reln->smgr_rlocator.locator, forknum,
						(uint16) chunk_blocks, entries + done);
			}

			um_materialize_pblk_zero_runs(ctx, forknum, pblknos + done,
										  chunk_blocks, skipFsync);
			UmApplyReservedRangeRemap(reln, forknum, blocknum + done,
									  chunk_blocks, pblknos + done,
									  map_lsn, skipFsync);
			done += chunk_blocks;
		}

		applied = true;
	}
	PG_CATCH();
	{
		if (!applied)
		{
			for (BlockNumber i = 0; i < nblocks; i++)
				MapInflightRelease(reln->smgr_rlocator.locator,
								   forknum, blocknum + i);
		}

		pfree(entries);
		pfree(pblknos);
		PG_RE_THROW();
	}
	PG_END_TRY();

	pfree(entries);
	pfree(pblknos);
	return true;
}

static UmbraMappedBirthResult
um_publish_mapped_birth(SMgrRelation reln, ForkNumber forknum,
						const UmbraAccessState *access,
						BlockNumber lblkno, bool allow_wal_owned_firstborn)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	UmbraMappedBirthResult result;
	BlockNumber old_pblkno;
	XLogRecPtr	map_lsn;
	bool		wal_insert_enabled;
	bool		wal_owns_firstborn;
	bool		emit_map_set;

	Assert(access->map_available);

	result.mapping_published = false;

	if (InRecovery && um_fork_uses_wal_owned_firstborn(forknum))
		elog(PANIC,
			 "missing WAL mapping during recovery for relation %u/%u/%u fork %d blk %u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber,
			 forknum, lblkno);

	MapGetNewPbkno(ctx, reln->smgr_rlocator.locator, forknum, lblkno,
				   &result.pblkno, &old_pblkno);
	Assert(old_pblkno == InvalidBlockNumber);

	wal_owns_firstborn = false;

	if (InRecovery)
	{
		map_lsn = GetXLogReplayRecPtr(NULL);
		MapSetMapping(ctx, reln->smgr_rlocator.locator, forknum, lblkno,
					  result.pblkno, map_lsn);
		result.mapping_published = true;
	}
	else
	{
		/*
		 * Birth ownership needs crash-recovery WAL even at wal_level=minimal.
		 * XLogIsNeeded() is too weak here because it suppresses WAL that is
		 * still required to recover eager MAP_SET publication after a crash.
		 */
		wal_insert_enabled =
			XLogInsertAllowed() &&
			!IsBootstrapProcessingMode() &&
			!IsInitProcessingMode();

		wal_owns_firstborn =
			allow_wal_owned_firstborn &&
			wal_insert_enabled &&
			UmWalOwnedFirstbornAvailable(reln, forknum, lblkno);

		emit_map_set = !wal_owns_firstborn;

		if (emit_map_set && wal_insert_enabled)
			map_lsn = log_umbra_map_set(reln->smgr_rlocator.locator, forknum,
										lblkno, old_pblkno, result.pblkno);
		else
			map_lsn = InvalidXLogRecPtr;

		if (emit_map_set)
		{
			MapSetMapping(ctx, reln->smgr_rlocator.locator, forknum, lblkno,
						  result.pblkno, map_lsn);
			result.mapping_published = true;
		}
	}

	if (result.mapping_published)
		MapSBlockBumpNextFreePhysBlock(ctx, reln->smgr_rlocator.locator,
									   forknum, result.pblkno + 1,
									   map_lsn);

	/*
	 * WAL-owned first-born pages keep their in-flight claim private until WAL
	 * insertion succeeds and XLogCommitBlockRemapsUmbra() publishes the mapping.
	 * Advancing the physical frontier or releasing the claim here would
	 * let xloginsert reserve a second pblk for the same logical birth, leaving
	 * alternating holes in the initial physical layout.
	 */
	if (!wal_owns_firstborn)
		MapInflightRelease(reln->smgr_rlocator.locator, forknum, lblkno);
	return result;
}

/*
 * Auxiliary mapped forks (FSM/VM) can observe stale logical blocks during
 * replay after truncate/VACUUM maintenance has already shrunk the
 * authoritative logical EOF.  Those callers historically expect EOF-like
 * semantics, not a hard mapped-fork corruption error.
 *
 * Recovery reads are synchronous, but they still come through the AIO/bufmgr
 * pipeline.  Complete the read locally as a zero page so the normal shared
 * buffer completion callbacks still run and mark the buffer valid, without
 * issuing any physical I/O against an unmapped/stale block.
 */
static void
um_complete_zero_readv(PgAioHandle *ioh, SMgrRelation reln,
					   ForkNumber forknum, BlockNumber blocknum,
					   void *buffer)
{
	Assert(ioh != NULL);
	Assert(buffer != NULL);
	Assert(!INTERRUPTS_CAN_BE_PROCESSED());
	Assert(ioh->state == PGAIO_HS_HANDED_OUT);
	Assert(pgaio_my_backend->handed_out_io == ioh);

	memset(buffer, 0, BLCKSZ);

	pgaio_io_set_target_smgr(ioh, reln, forknum,
							 blocknum,
							 blocknum,
							 1, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);

	/*
	 * Mirror the minimal pgaio_io_stage() state transitions needed to invoke
	 * the normal smgr + buffer read completion callbacks, but do not start an
	 * actual readv against any file.
	 */
	ioh->op = PGAIO_OP_READV;
	ioh->result = 0;
	ioh->state = PGAIO_HS_DEFINED;
	pgaio_my_backend->handed_out_io = NULL;

	pgaio_io_call_stage(ioh);

	ioh->state = PGAIO_HS_STAGED;
	pgaio_io_prepare_submit(ioh);

	START_CRIT_SECTION();
	pgaio_io_process_completion(ioh, BLCKSZ);
	END_CRIT_SECTION();
}

/*
 * PG18 truncation order is:
 *   1. RelationTruncate() emits truncate WAL
 *   2. smgrpretruncate() lets Umbra preload MAP pages before entering the
 *      critical section
 *   3. smgrtruncate() later drops old shared buffers and performs the
 *      truncate-time metadata update
 *
 * That means a backend can briefly attempt to flush a stale dirty buffer for
 * a block that has already been truncated away logically, but has not yet
 * been removed from shared buffers.  Such a write must be ignored, not turned
 * into a new mapping owner.  Only blocks still inside the authoritative
 * logical EOF are allowed to demand a mapping.
 */
static bool
um_is_stale_post_truncate_lblk_for_access(SMgrRelation reln,
										  ForkNumber forknum,
										  const UmbraAccessState *access,
										  UmbraAccessLookupState *lookup_state,
										  BlockNumber lblkno)
{
	BlockNumber logical_nblocks;

	if (!access->map_available)
		return false;

	if (!um_fork_uses_map_translation(forknum))
		return false;

	/*
	 * MAIN fork remains strict during recovery: missing mappings there are
	 * corruption signals. Auxiliary mapped forks (FSM/VM) can legitimately
	 * walk just beyond logical EOF during truncate/vacuum maintenance, both
	 * in normal execution and during replay.
	 */
	if (InRecovery && !UmbraForkIsAuxiliaryMapped(forknum))
		return false;

	/*
	 * Use the same authoritative logical EOF that the rest of the system sees.
	 * Umbra should have only one logical-size source of truth; this stale
	 * post-truncate path must not invent a second one by scanning MAP pages.
	 */
	if (lookup_state != NULL && lookup_state->have_logical_nblocks)
		logical_nblocks = lookup_state->logical_nblocks;
	else
	{
		logical_nblocks = umnblocks_for_access(reln, forknum, access);
		if (lookup_state != NULL)
		{
			lookup_state->logical_nblocks = logical_nblocks;
			lookup_state->have_logical_nblocks = true;
		}
	}

	return um_is_stale_post_truncate_lblk_with_eof(forknum,
												   logical_nblocks,
												   lblkno);
}

static bool
um_is_stale_post_truncate_lblk_with_eof(ForkNumber forknum,
										BlockNumber logical_nblocks,
										BlockNumber lblkno)
{
	if (!um_fork_uses_map_translation(forknum))
		return false;

	if (logical_nblocks == InvalidBlockNumber)
		return false;

	return lblkno >= logical_nblocks;
}

BlockNumber
umphysicalblock(SMgrRelation reln, ForkNumber forknum, BlockNumber lblkno)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber pblkno;

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
		return lblkno;

	if (MapTryLookup(ctx, reln->smgr_rlocator.locator,
					 forknum, lblkno, &pblkno))
		return pblkno;

	um_report_unmapped_map_entry(reln, forknum, &access, lblkno);
	pg_unreachable();
}

void
UmMapGetNewPbkno(SMgrRelation reln, ForkNumber forknum,
				 BlockNumber lblkno, BlockNumber *new_pblkno,
				 BlockNumber *old_pblkno)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	Assert(new_pblkno != NULL);
	Assert(old_pblkno != NULL);

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
	{
		*old_pblkno = lblkno;
		*new_pblkno = lblkno;
		return;
	}

	MapGetNewPbkno(ctx, reln->smgr_rlocator.locator, forknum,
				   lblkno, new_pblkno, old_pblkno);
}

void
UmMapReserveFreshPbkno(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber *new_pblkno)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	um_reserve_fresh_pblkno_for_access(reln, forknum, &access,
									   lblkno, new_pblkno);
}

bool
UmMapAccessAvailable(SMgrRelation reln, ForkNumber forknum)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	return access.map_available;
}

bool
UmWalOwnedRemapAvailable(SMgrRelation reln, ForkNumber forknum)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	return access.policy == UMBRA_MAP_POLICY_REQUIRE_MAP;
}

bool
UmWalOwnedFirstbornAvailable(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno)
{
	UmbraAccessState access;

	(void) lblkno;
	access = um_classify_access(reln, forknum);
	return access.policy == UMBRA_MAP_POLICY_REQUIRE_MAP &&
		XLogInsertAllowed() &&
		!IsBootstrapProcessingMode() &&
		!IsInitProcessingMode() &&
		um_fork_uses_wal_owned_firstborn(forknum);
}

bool
UmMapTryLookupPblkno(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber lblkno, BlockNumber *pblkno)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
	{
		*pblkno = lblkno;
		return true;
	}

	return MapTryLookup(ctx, reln->smgr_rlocator.locator,
						forknum, lblkno, pblkno);
}

bool
UmMapIsLogicalUnmaterialized(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	return um_is_logical_unmaterialized_for_access(reln, forknum, &access,
												   lblkno);
}

void
UmMapSetMapping(SMgrRelation reln, ForkNumber forknum,
				BlockNumber lblkno, BlockNumber new_pblkno,
				XLogRecPtr map_lsn)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
		return;

	MapSetMapping(ctx, reln->smgr_rlocator.locator,
				  forknum, lblkno, new_pblkno, map_lsn);
}

static void
um_filetag_path(const FileTag *ftag, char *path)
{
	RelPathStr	base;

	if (ftag->forknum == UMBRA_METADATA_FORKNUM)
		base = UmMetadataRelPathPerm(ftag->rlocator);
	else
		base = relpathperm(ftag->rlocator, ftag->forknum);

	if (ftag->segno == 0)
		strlcpy(path, base.str, MAXPGPATH);
	else
		snprintf(path, MAXPGPATH, "%s.%llu",
				 base.str, (unsigned long long) ftag->segno);
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	bool		created = false;

	if (!umfile_open_or_create(ctx, forknum, isRedo, &created))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create or open relation %u/%u/%u fork %d",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum)));

	if (created &&
		UmbraForkIsAuxiliaryMapped(forknum) &&
		UmMetadataExists(reln))
	{
		XLogRecPtr	map_lsn;

		map_lsn = InRecovery ? GetXLogReplayRecPtr(NULL) : GetXLogWriteRecPtr();
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, 0, map_lsn);
	}
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	UmbraMapPolicy policy;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (!um_fork_uses_map_translation(forknum))
		return umfile_exists(ctx, forknum, UMFILE_EXISTS_DENSE);

	policy = um_map_policy_for_access(reln, forknum);
	if (policy != UMBRA_MAP_POLICY_REQUIRE_MAP)
		return umfile_exists(ctx, forknum, UMFILE_EXISTS_DENSE);

	return um_mapped_exists_from_super(reln, forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	/*
	 * Keep MAIN and MAP fork deletion ordered so the mapping lifecycle tracks
	 * the data fork during DROP processing and relfilenode reuse.
	 */
	if (forknum == InvalidForkNumber)
	{
		MapInvalidateRelation(rlocator.locator);

		umfile_unlink(rlocator, MAIN_FORKNUM, isRedo);
		UmMetadataUnlink(rlocator, isRedo);

		for (ForkNumber other = FSM_FORKNUM; other <= INIT_FORKNUM; other++)
			umfile_unlink(rlocator, other, isRedo);
		return;
	}

	if (forknum == UMBRA_METADATA_FORKNUM)
		MapInvalidateRelation(rlocator.locator);
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	UmbraAccessState access;
	BlockNumber	pblkno;
	BlockNumber	logical_nblocks;
	BlockNumber materialized_nblocks = 0;
	BlockNumber	max_pblkno = InvalidBlockNumber;
	bool		mapping_committed = false;

	access = um_classify_access(reln, forknum);

	/* Only MAP fork itself uses direct physical extend. */
	if (!access.map_available)
	{
		pblkno = blocknum;
		umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
		return;
	}

	/*
	 * smgrextend() contract allows blocknum beyond current EOF and requires
	 * intervening space to read as zeros.  For mapped forks, we must explicitly
	 * create mappings and materialize zero pages for the gap.
	 */
	logical_nblocks = umnblocks_for_access(reln, forknum, &access);
	(void) MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										  forknum, &materialized_nblocks);
	if (blocknum > logical_nblocks)
	{
		umzeroextend(reln, forknum, logical_nblocks,
					 (int) (blocknum - logical_nblocks),
					 skipFsync);
	}

	/*
	 * For mapped data forks, smgrextend is the point where an unmapped logical
	 * block gets its first physical block. Existing mappings can appear during
	 * redo/replay and should be reused.
	 */
	if (!MapTryLookup(ctx, reln->smgr_rlocator.locator, forknum, blocknum, &pblkno))
	{
		UmbraMappedBirthResult birth;

		birth = um_publish_mapped_birth(reln, forknum, &access, blocknum, true);
		pblkno = birth.pblkno;
		mapping_committed = birth.mapping_published;

		/*
		 * Reservation/publication can make the mapping visible before the data
		 * fork is physically materialized. Extend when the chosen pblk is still
		 * beyond the materialized physical EOF; otherwise just write the page.
		 */
		/*
		 * Page checksum stays keyed by the logical block identity; callers
		 * reaching smgrextend() have already set it using the logical blkno.
		 */
		if (pblkno >= materialized_nblocks)
		{
			umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
			max_pblkno = pblkno;
		}
		else
		{
			const void *single_buffer[1];

			single_buffer[0] = buffer;
			umfile_writev(ctx, forknum, pblkno, single_buffer, 1, skipFsync);
		}
	}
	else
	{
		if (pblkno >= materialized_nblocks)
		{
			umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
			max_pblkno = pblkno;
		}
		else
		{
			const void *single_buffer[1];

			single_buffer[0] = buffer;
			umfile_writev(ctx, forknum, pblkno, single_buffer, 1, skipFsync);
		}
	}

	if (mapping_committed)
		MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
										forknum, blocknum + 1,
										InvalidXLogRecPtr);
	if (max_pblkno != InvalidBlockNumber)
		MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum, max_pblkno + 1,
										 InvalidXLogRecPtr);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	UmbraAccessState access;
	BlockNumber	run_start_pblk;
	BlockNumber	max_pblkno;
	int			run_len;

	if (nblocks <= 0)
		return;

	access = um_classify_access(reln, forknum);

	/* Direct physical path for non-mapped forks. */
	if (!access.map_available)
	{
		umfile_zeroextend(ctx, forknum, blocknum, nblocks,
						  skipFsync);
		return;
	}

	if (um_try_pure_firstborn_range_remap_zeroextend(reln, forknum, &access,
													blocknum,
													(BlockNumber) nblocks,
													skipFsync))
		return;

	/*
	 * Per-block path for single-block, recovery, or callers that encountered
	 * pre-existing/pending MAP ownership in the requested range.
	 *
	 * For mapped forks we must materialize each newly-mapped physical page as
	 * a zero page, otherwise a later read through MAP would hit EOF/short read.
	 *
	 * Map allocator hands out sequential pblknos, so we can batch contiguous
	 * physical ranges with umfile_zeroextend().
	 */
	run_start_pblk = InvalidBlockNumber;
	max_pblkno = InvalidBlockNumber;
	run_len = 0;

	for (int i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = blocknum + (BlockNumber) i;
		BlockNumber pblk;

		if (!MapTryLookup(ctx, reln->smgr_rlocator.locator, forknum, lblk, &pblk))
		{
			UmbraMappedBirthResult birth;

			birth = um_publish_mapped_birth(reln, forknum, &access, lblk, false);
			pblk = birth.pblkno;
		}

		if (max_pblkno == InvalidBlockNumber || pblk > max_pblkno)
			max_pblkno = pblk;

		if (run_len == 0)
		{
			run_start_pblk = pblk;
			run_len = 1;
		}
		else if (pblk == run_start_pblk + (BlockNumber) run_len)
		{
			run_len++;
		}
		else
		{
			umfile_zeroextend(ctx, forknum, run_start_pblk,
							  run_len, skipFsync);
			run_start_pblk = pblk;
			run_len = 1;
		}
	}

	if (run_len > 0)
		umfile_zeroextend(ctx, forknum, run_start_pblk, run_len,
						  skipFsync);

	if (max_pblkno != InvalidBlockNumber)
		MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
									 forknum, max_pblkno + 1,
									 InvalidXLogRecPtr);

	MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								forknum, blocknum + (BlockNumber) nblocks,
								InvalidXLogRecPtr);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);

	if (!access.map_available)
		return umfile_prefetch(ctx, forknum, blocknum, nblocks);

	for (int i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = blocknum + (BlockNumber) i;
		BlockNumber pblk;

		if (um_resolve_lblk_for_access(reln, forknum, &access, &lookup_state,
									   lblk, UMBRA_ACCESS_RESOLVE_READ,
									   &pblk) != UMBRA_ACCESS_RESOLVED_PBLK)
			continue;

		if (!umfile_prefetch(ctx, forknum, pblk, 1))
			return false;
	}
	return true;
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	BlockNumber	pblk;
	BlockNumber	run_blocks;

	access = um_classify_access(reln, forknum);

	/*
	 * For mapped forks we can only combine a read while the translated physical
	 * blocks remain contiguous and stay inside one segment.
	 */
	if (access.map_available)
	{
		if (InRecovery && UmbraForkIsAuxiliaryMapped(forknum))
			return 1;

		run_blocks = um_resolve_mapped_read_run(reln, forknum, &access,
												 &lookup_state, blocknum,
												 Min((BlockNumber) io_max_combine_limit,
													 (BlockNumber) umfile_maxcombine(forknum,
																			 blocknum)),
												 &pblk);
		return Max((BlockNumber) 1, run_blocks);
	}
	return umfile_maxcombine(forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	bool		aux_recovery_read;

	access = um_classify_access(reln, forknum);

	if (!access.map_available)
	{
		umfile_readv(ctx, forknum, blocknum, buffers, nblocks);
		return;
	}

	aux_recovery_read = InRecovery && UmbraForkIsAuxiliaryMapped(forknum);

	for (BlockNumber i = 0; i < nblocks;)
	{
		BlockNumber	pblk;
		BlockNumber	run_blocks;

		run_blocks = um_resolve_mapped_read_run(reln, forknum, &access,
												 &lookup_state, blocknum + i,
												 aux_recovery_read ? 1 :
												 (nblocks - i),
												 &pblk);
		if (run_blocks == 0)
		{
			memset(buffers[i], 0, BLCKSZ);
			i++;
			continue;
		}

		/*
		 * Preserve md-style sync-read semantics for mapped forks by routing the
		 * translated physical block through umfile_readv().
		 *
		 * Callers such as VM/FSM redo use RBM_ZERO_ON_ERROR and expect
		 * InRecovery/zero_damaged_pages handling on short reads.  A direct
		 * physical read would bypass that behavior and fail before bufmgr gets a
		 * chance to zero the page.
		 */
		if (aux_recovery_read)
			um_ensure_datafork_batch_ready_for_access(reln, forknum, &access,
													  pblk, true /* skipFsync */ );

		umfile_readv(ctx, forknum, pblk, &buffers[i], run_blocks);
		i += run_blocks;
	}
}

static void
um_startreadv_direct_physical(PgAioHandle *ioh, SMgrRelation reln,
							  UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers,
							  BlockNumber nblocks)
{
	pgaio_io_set_target_smgr(ioh, reln, forknum,
							 blocknum /* logical */,
							 blocknum /* physical */,
							 nblocks, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, ctx, forknum, blocknum, buffers, nblocks);
}

static BlockNumber
um_startreadv_lookup_mapped(PgAioHandle *ioh, SMgrRelation reln,
							ForkNumber forknum, BlockNumber blocknum,
							void **buffers, BlockNumber nblocks,
							const UmbraAccessState *access,
							UmbraAccessLookupState *lookup_state,
							bool aux_recovery_read,
							BlockNumber *pblk)
{
	BlockNumber	run_blocks;

	Assert(pblk != NULL);

	run_blocks = um_resolve_mapped_read_run(reln, forknum, access, lookup_state,
											 blocknum,
											 aux_recovery_read ? 1 : nblocks,
											 pblk);
	if (run_blocks > 0)
		return run_blocks;

	ioh->handle_data_len = 1;
	um_complete_zero_readv(ioh, reln, forknum, blocknum, buffers[0]);
	return 0;
}

static void
um_startreadv_mapped_physical(PgAioHandle *ioh, SMgrRelation reln,
							  UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers,
							  BlockNumber nblocks, BlockNumber pblk,
							  bool aux_recovery_read,
							  const UmbraAccessState *access)
{
	if (aux_recovery_read)
	{
		if (!umfile_ctx_block_exists(ctx, forknum, pblk))
			um_ensure_datafork_batch_ready_for_access(reln, forknum, access,
													  pblk, true /* skipFsync */ );
	}

	pgaio_io_set_target_smgr(ioh, reln, forknum,
							 blocknum /* logical */,
							 pblk /* physical */,
							 nblocks, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv_physical(ioh, ctx, forknum,
							   blocknum /* logical */,
							   pblk /* physical */,
							   buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	pblk;
	bool		aux_recovery_read;
	access = um_classify_access(reln, forknum);

	if (!access.map_available)
	{
		um_startreadv_direct_physical(ioh, reln, ctx, forknum,
									  blocknum, buffers, nblocks);
		return;
	}

	/*
	 * See ummaxcombine(): callers may only combine a prefix whose translated
	 * physical blocks form one contiguous run.  If the mapping changed since
	 * that check, shrink this I/O to the currently valid prefix and let
	 * WaitReadBuffers() retry the remainder.
	 */
	aux_recovery_read = InRecovery && UmbraForkIsAuxiliaryMapped(forknum);

	{
		BlockNumber	run_blocks;

		run_blocks = um_startreadv_lookup_mapped(ioh, reln, forknum, blocknum,
												 buffers, nblocks, &access,
												 &lookup_state,
												 aux_recovery_read, &pblk);
		if (run_blocks == 0)
		{
			return;
		}

		if (run_blocks < nblocks)
			ioh->handle_data_len = run_blocks;
		nblocks = run_blocks;
	}

	/*
	 * Start I/O using physical addressing but preserve logical identity for
	 * error reporting and reopen semantics.
	 */
	um_startreadv_mapped_physical(ioh, reln, ctx, forknum, blocknum,
								  buffers, nblocks, pblk,
								  aux_recovery_read, &access);
}

static void
um_claim_write_barrier(SMgrRelation reln, ForkNumber forknum,
					   UmbraFileContext *ctx, BlockNumber lblkno,
					   MapInflightBarrier *barrier)
{
	int			wait_retries = 0;

	Assert(barrier != NULL);
	Assert(!barrier->valid);

	for (;;)
	{
		if (MapInflightTryClaimBarrier(ctx, reln->smgr_rlocator.locator,
									   forknum, lblkno, barrier))
		{
			if (wait_retries > 0)
				elog(LOG,
					 "storage write waited for in-flight remap on relation %u/%u/%u fork %d block %u (%d retries, %d usec)",
					 reln->smgr_rlocator.locator.spcOid,
					 reln->smgr_rlocator.locator.dbOid,
					 reln->smgr_rlocator.locator.relNumber,
					 forknum, lblkno,
					 wait_retries,
					 wait_retries * UMBRA_WRITE_BARRIER_WAIT_USEC);
			return;
		}

		CHECK_FOR_INTERRUPTS();
		pg_usleep(UMBRA_WRITE_BARRIER_WAIT_USEC);
		wait_retries++;

		if (wait_retries >= UMBRA_WRITE_BARRIER_WAIT_RETRIES)
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("timed out waiting for in-flight remap of relation %u/%u/%u fork %d block %u",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum, lblkno)));
	}
}

static void
um_release_write_barriers(MapInflightBarrier *barriers, BlockNumber nbarriers)
{
	for (BlockNumber i = 0; i < nbarriers; i++)
		MapInflightReleaseBarrier(&barriers[i]);
}

static void
um_flush_write_barrier_run(UmbraFileContext *ctx, ForkNumber forknum,
						   BlockNumber run_start_pblk,
						   const void **buffers,
						   BlockNumber run_start_idx,
						   BlockNumber *run_blocks,
						   bool skipFsync,
						   MapInflightBarrier *barriers)
{
	if (*run_blocks == 0)
		return;

	umfile_writev(ctx, forknum, run_start_pblk, &buffers[run_start_idx],
				  *run_blocks, skipFsync);
	um_release_write_barriers(barriers, *run_blocks);
	*run_blocks = 0;
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber		materialized_nblocks = 0;
	BlockNumber		max_extended_pblk = InvalidBlockNumber;
	BlockNumber		run_start_pblk = InvalidBlockNumber;
	BlockNumber		run_start_idx = 0;
	BlockNumber		run_blocks = 0;
	MapInflightBarrier run_barriers[PG_IOV_MAX];
	MapInflightBarrier pending_barrier = {0};

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
	{
		umfile_writev(ctx, forknum, blocknum, buffers, nblocks,
					  skipFsync);
		return;
	}

	(void) MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										  forknum, &materialized_nblocks);

	PG_TRY();
	{
		for (BlockNumber i = 0; i < nblocks; i++)
		{
			BlockNumber lblk = blocknum + i;
			BlockNumber pblk;
			UmbraAccessResolveResult resolve;
			bool		can_extend_run;

			pending_barrier.valid = false;
			pending_barrier.slot_id = -1;
			pending_barrier.entry_idx = -1;

			/*
			 * The barrier serializes physical writes with any foreign in-flight
			 * remap for the same logical block.  Claim before lookup so a later remap
			 * cannot publish a new mapping while this write is still targeting the
			 * old physical page.
			 */
			um_claim_write_barrier(reln, forknum, ctx, lblk, &pending_barrier);

			resolve = um_resolve_lblk_for_access(reln, forknum, &access,
												 &lookup_state,
												 lblk,
												 UMBRA_ACCESS_RESOLVE_WRITE,
												 &pblk);
			Assert(resolve == UMBRA_ACCESS_RESOLVED_PBLK);

			/*
			 * Checksum identity stays logical (lblk); callers reaching smgrwritev()
			 * have already set it before Umbra translates to physical blocks.
			 */

			can_extend_run =
				(run_blocks > 0) &&
				(pblk == run_start_pblk + run_blocks) &&
				(run_blocks < (BlockNumber) lengthof(run_barriers)) &&
				((run_start_pblk % ((BlockNumber) RELSEG_SIZE)) + run_blocks <
				 ((BlockNumber) RELSEG_SIZE));

			if (pblk < materialized_nblocks)
			{
				if (run_blocks == 0)
				{
					run_start_pblk = pblk;
					run_start_idx = i;
				}
				else if (!can_extend_run)
				{
					um_flush_write_barrier_run(ctx, forknum, run_start_pblk,
											   buffers, run_start_idx,
											   &run_blocks, skipFsync,
											   run_barriers);
					run_start_pblk = pblk;
					run_start_idx = i;
				}

				Assert(run_blocks < (BlockNumber) lengthof(run_barriers));
				run_barriers[run_blocks] = pending_barrier;
				pending_barrier.valid = false;
				run_blocks++;
				continue;
			}

			um_flush_write_barrier_run(ctx, forknum, run_start_pblk,
									   buffers, run_start_idx,
									   &run_blocks, skipFsync, run_barriers);
			run_start_pblk = InvalidBlockNumber;

			umfile_extend(ctx, forknum, pblk, buffers[i], skipFsync);
			MapInflightReleaseBarrier(&pending_barrier);

			if (max_extended_pblk == InvalidBlockNumber || pblk > max_extended_pblk)
				max_extended_pblk = pblk;
			if (materialized_nblocks < pblk + 1)
				materialized_nblocks = pblk + 1;
		}

		um_flush_write_barrier_run(ctx, forknum, run_start_pblk,
								   buffers, run_start_idx,
								   &run_blocks, skipFsync, run_barriers);
	}
	PG_CATCH();
	{
		MapInflightReleaseBarrier(&pending_barrier);
		um_release_write_barriers(run_barriers, run_blocks);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (max_extended_pblk != InvalidBlockNumber)
		MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
									 forknum, max_extended_pblk + 1,
									 InvalidXLogRecPtr);

	if (nblocks > 0)
	{
		BlockNumber logical_nblocks;

		logical_nblocks = umnblocks_for_access(reln, forknum, &access);
		Assert(logical_nblocks >= blocknum + nblocks);
	}
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			BlockNumber nblocks)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber		run_start_pblk = InvalidBlockNumber;
	BlockNumber		run_blocks = 0;

	access = um_classify_access(reln, forknum);
	if (!access.map_available)
	{
		umfile_writeback(ctx, forknum, blocknum, nblocks);
		return;
	}

	for (BlockNumber i = 0; i < nblocks; i++)
	{
		BlockNumber lblk = blocknum + i;
		BlockNumber pblk;
		UmbraAccessResolveResult resolve;
		bool		can_extend_run;

		resolve = um_resolve_lblk_for_access(reln, forknum, &access,
											 &lookup_state,
											 lblk,
											 UMBRA_ACCESS_RESOLVE_WRITEBACK,
											 &pblk);
		if (resolve == UMBRA_ACCESS_RESOLVED_SKIP)
		{
			if (run_blocks > 0)
			{
				umfile_writeback(ctx, forknum, run_start_pblk, run_blocks);
				run_start_pblk = InvalidBlockNumber;
				run_blocks = 0;
			}
			continue;
		}

		Assert(resolve == UMBRA_ACCESS_RESOLVED_PBLK);

		can_extend_run =
			(run_blocks > 0) &&
			(pblk == run_start_pblk + run_blocks);

		if (run_blocks == 0)
		{
			run_start_pblk = pblk;
			run_blocks = 1;
		}
		else if (can_extend_run)
		{
			run_blocks++;
		}
		else
		{
			umfile_writeback(ctx, forknum, run_start_pblk, run_blocks);
			run_start_pblk = pblk;
			run_blocks = 1;
		}
	}

	if (run_blocks > 0)
		umfile_writeback(ctx, forknum, run_start_pblk, run_blocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	return umnblocks_for_access(reln, forknum, &access);
}

BlockNumber
umnblocks_cached(SMgrRelation reln, ForkNumber forknum)
{
	return reln->smgr_cached_nblocks[forknum];
}

static BlockNumber
umnblocks_for_access(SMgrRelation reln, ForkNumber forknum,
					 const UmbraAccessState *access)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber nblocks;

	if (!access->map_available)
		return umfile_nblocks(ctx, forknum, UMFILE_NBLOCKS_DENSE);

	if (MapSBlockTryGetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									  forknum, &nblocks))
		return nblocks;

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("missing or invalid MAP superblock for relation %u/%u/%u fork %d",
					reln->smgr_rlocator.locator.spcOid,
					reln->smgr_rlocator.locator.dbOid,
					reln->smgr_rlocator.locator.relNumber,
					forknum)));
}

void
umpretruncate(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber old_blocks, BlockNumber nblocks,
			  XLogRecPtr truncate_lsn)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	(void) old_blocks;
	(void) truncate_lsn;
	access = um_classify_access(reln, forknum);

	if (um_fork_uses_map_translation(forknum) &&
		(access.policy == UMBRA_MAP_POLICY_REQUIRE_MAP ||
		 access.map_available))
		MapPreloadTruncatePages(ctx, reln->smgr_rlocator.locator,
								forknum, nblocks);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (um_fork_uses_map_translation(forknum) &&
		(access.policy == UMBRA_MAP_POLICY_REQUIRE_MAP ||
		 access.map_available))
	{
		XLogRecPtr	map_lsn;

		map_lsn = InRecovery ?
			GetXLogReplayRecPtr(NULL) : GetXLogWriteRecPtr();

		MapTruncate(ctx, reln->smgr_rlocator.locator,
					forknum, nblocks, map_lsn);
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, nblocks, map_lsn);
		MapReleasePreloadedTruncatePages(reln->smgr_rlocator.locator, forknum);
		return;
	}

	/* Non-mapped forks (and MAP fork itself) truncate physically. */
	umfile_truncate(ctx, forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	if (forknum == UMBRA_METADATA_FORKNUM)
		MapCheckpointRelation(reln->smgr_rlocator.locator);

	umfile_immedsync(ctx, forknum);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	umimmedsync(reln, forknum);
}

bool
umpreparependingsync(SMgrRelation reln)
{
	if (RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator))
		UmRebuildMapAndSuperblockForSkipWAL(reln);

	return um_relation_requires_durable_sync(reln);
}

bool
umneedsrecoveryfsmvacuum(SMgrRelation reln)
{
	(void) reln;

	/*
	 * FSM is not WAL-logged. During replay, Umbra already publishes the
	 * truncate result through mapped metadata, and auxiliary stale reads are
	 * handled with EOF-like semantics. Re-running the generic post-truncate
	 * FSM vacuum step provides only tidy-up value, while forcing recovery to
	 * walk stale upper-tree pages through the MAIN-oriented buffer model.
	 *
	 * Skip that replay-only cleanup and let later foreground FSM maintenance
	 * refresh upper-level slots naturally.
	 */
	return false;
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/*
	 * smgrfd() is only used by the AIO reopen path, after the issuer has
	 * already resolved logical identity to a concrete physical target.
	 * Interpret blocknum here as a physical block number and reopen the
	 * corresponding segment/offset directly.
	 */
	return umfile_fd(um_ctx_acquire(reln), forknum, blocknum, off);
}

int
umsyncfiletag(const FileTag *ftag, char *path)
{
	File		fd;
	int			ret;
	int			save_errno;

	um_filetag_path(ftag, path);

	fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return -1;

	ret = FileSync(fd, WAIT_EVENT_DATA_FILE_SYNC);
	save_errno = errno;

	FileClose(fd);
	errno = save_errno;
	return ret;
}

int
umunlinkfiletag(const FileTag *ftag, char *path)
{
	um_filetag_path(ftag, path);
	return unlink(path);
}

bool
umfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
	/*
	 * Database-scope filter (DROP DATABASE / MOVE DATABASE paths).
	 */
	if (ftag->forknum == InvalidForkNumber &&
		ftag->segno == InvalidBlockNumber &&
		ftag->rlocator.spcOid == 0 &&
		ftag->rlocator.relNumber == 0)
		return ftag->rlocator.dbOid == candidate->rlocator.dbOid;

	/*
	 * Relation-scope filter: wildcard fork/segment.
	 */
	if (ftag->forknum == InvalidForkNumber &&
		ftag->segno == InvalidBlockNumber)
		return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator);

	/*
	 * Fork-scope filter: wildcard segment.
	 */
	if (ftag->segno == InvalidBlockNumber)
		return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator) &&
			ftag->forknum == candidate->forknum;

	/* Exact file match. */
	return RelFileLocatorEquals(ftag->rlocator, candidate->rlocator) &&
		ftag->forknum == candidate->forknum &&
		ftag->segno == candidate->segno;
}
