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
#include "storage/map_internal.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "storage/umfile.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

bool		umbra_chunk_zero_fill_all_slots = true;
bool		umbra_exp2_c3_pause = false;

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

typedef struct UmbraRedoShiftSourceState
{
	bool		active;
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber lblkno;
	uint8		source_slot;
} UmbraRedoShiftSourceState;

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
static void um_report_legacy_entry_map_path(SMgrRelation reln,
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
static bool um_redo_shift_source_pblk(SMgrRelation reln, ForkNumber forknum,
									  BlockNumber lblkno, BlockNumber *pblkno);
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
static void um_zeroextend_flush_run(UmbraFileContext *ctx, ForkNumber forknum,
									BlockNumber *run_start_pblk,
									int *run_len, bool skipFsync);
static bool um_fork_uses_map_translation(ForkNumber forknum);
static bool um_mapped_exists_from_super(SMgrRelation reln, ForkNumber forknum);
static UmbraMapPolicy um_open_map_state(SMgrRelation reln);
static bool um_state_uses_map(UmbraMapPolicy state);
static bool um_state_uses_chunk_base(UmbraMapPolicy state);
static bool um_state_uses_chunk_direct_base(UmbraMapPolicy state);
static bool um_state_requires_durable_sync(UmbraMapPolicy state);
static bool um_relation_requires_durable_sync(SMgrRelation reln);
static BlockNumber um_prepare_mapped_birth(SMgrRelation reln,
										   ForkNumber forknum,
										   const UmbraAccessState *access,
										   BlockNumber lblkno);
static void um_filetag_path(const FileTag *ftag, char *path);
static BlockNumber um_chunk_slot_pblk_checked(SMgrRelation reln,
											  ForkNumber forknum,
											  BlockNumber lblkno,
											  uint8 active_slot);
static BlockNumber um_chunk_base_pblk_checked(SMgrRelation reln,
											  ForkNumber forknum,
											  BlockNumber lblkno);
static BlockNumber um_chunk_active_pblk_checked(SMgrRelation reln,
												ForkNumber forknum,
												BlockNumber lblkno);
static BlockNumber um_chunk_active_pblk_run_checked(SMgrRelation reln,
													ForkNumber forknum,
													BlockNumber lblkno,
													BlockNumber maxblocks,
													BlockNumber *start_pblk);
static BlockNumber um_chunk_physical_capacity_checked(SMgrRelation reln,
													  ForkNumber forknum,
													  BlockNumber logical_nblocks);
static void um_ensure_chunk_physical_capacity(SMgrRelation reln,
											  ForkNumber forknum,
											  UmbraFileContext *ctx,
											  BlockNumber logical_nblocks,
											  bool skipFsync);

static UmbraRedoShiftSourceState um_redo_shift_source_state = {0};

bool
UmMetadataExists(SMgrRelation reln)
{
	return umfile_exists(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM);
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
	return umfile_nblocks(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM);
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
	umfile_registersync(um_ctx_acquire(reln), UMBRA_METADATA_FORKNUM);
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

	if (um_state_uses_chunk_base(access->policy))
		umfile_ctx_ensure_block_exists(ctx, forknum, pblkno);
	else
		(void) MapSBlockEnsurePhysicalNblocks(ctx,
											  reln->smgr_rlocator.locator,
											  forknum, pblkno + 1,
											  skipFsync);
}

static void
um_zeroextend_flush_run(UmbraFileContext *ctx, ForkNumber forknum,
						BlockNumber *run_start_pblk,
						int *run_len, bool skipFsync)
{
	Assert(run_start_pblk != NULL);
	Assert(run_len != NULL);

	if (*run_len <= 0)
		return;

	umfile_zeroextend(ctx, forknum, *run_start_pblk, *run_len, skipFsync);
	*run_start_pblk = InvalidBlockNumber;
	*run_len = 0;
}

/*
 * Create and initialize the Umbra metadata fork for a relation.
 *
 * Keep creation O(1): create/open the metadata fork and write only the
 * superblock sector. Shift bitmap pages are synthesized on first access and
 * written lazily by the MAP layer.
 */

static void
ummapcreate(SMgrRelation reln)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	bool		newly_created;

	/*
	 * Open existing metadata fork or create a new one. During redo, EEXIST is
	 * acceptable and we reuse the existing file.
	 */
	UmMetadataOpenOrCreate(reln, true /* isRedo */, &newly_created);

	/* Existing metadata fork does not need re-initialization. */
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
um_state_uses_chunk_base(UmbraMapPolicy state)
{
	return state == UMBRA_MAP_POLICY_REQUIRE_MAP ||
		state == UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP;
}

static bool
um_state_uses_chunk_direct_base(UmbraMapPolicy state)
{
	return state == UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP;
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

static BlockNumber
um_chunk_slot_pblk_checked(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber lblkno, uint8 active_slot)
{
	BlockNumber pblkno;

	if (!UmbraChunkActiveSlotIsValid(active_slot))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid Umbra active slot %u for relation %u/%u/%u fork %d logical block %u",
						active_slot,
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum, lblkno)));

	if (!UmbraChunkPairedSlotPblk(lblkno, active_slot, &pblkno))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("chunk-paired slot physical block overflow for relation %u/%u/%u fork %d logical block %u slot %u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum, lblkno, active_slot)));

	return pblkno;
}

static BlockNumber
um_chunk_base_pblk_checked(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber lblkno)
{
	return um_chunk_slot_pblk_checked(reln, forknum, lblkno, 0);
}

static BlockNumber
um_chunk_active_pblk_checked(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	uint8		active_slot = 0;

	(void) UmbraShiftGet(ctx, reln->smgr_rlocator.locator,
						  forknum, lblkno, &active_slot);

	return um_chunk_slot_pblk_checked(reln, forknum, lblkno, active_slot);
}

static BlockNumber
um_chunk_active_pblk_run_checked(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber lblkno, BlockNumber maxblocks,
								 BlockNumber *start_pblk)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	chunk_blocks;
	BlockNumber	run_blocks;
	uint8		active_slot = 0;

	Assert(maxblocks > 0);
	Assert(start_pblk != NULL);

	chunk_blocks = Min(maxblocks,
					   (BlockNumber) UMBRA_CHUNK_PAIRED_PAGES -
					   (lblkno % (BlockNumber) UMBRA_CHUNK_PAIRED_PAGES));
	run_blocks = UmbraShiftGetRun(ctx, reln->smgr_rlocator.locator,
								  forknum, lblkno, chunk_blocks,
								  &active_slot);

	Assert(run_blocks > 0);
	Assert(run_blocks <= chunk_blocks);

	*start_pblk = um_chunk_slot_pblk_checked(reln, forknum, lblkno,
											 active_slot);

	return run_blocks;
}

static bool
um_redo_shift_source_pblk(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber lblkno, BlockNumber *pblkno)
{
	UmbraRedoShiftSourceState *state = &um_redo_shift_source_state;

	Assert(pblkno != NULL);

	if (!state->active)
		return false;
	if (forknum != state->forknum || lblkno != state->lblkno)
		return false;
	if (!RelFileLocatorEquals(state->rlocator, reln->smgr_rlocator.locator))
		return false;

	*pblkno = um_chunk_slot_pblk_checked(reln, forknum, lblkno,
										 state->source_slot);
	return true;
}

static BlockNumber
um_chunk_physical_capacity_checked(SMgrRelation reln, ForkNumber forknum,
								   BlockNumber logical_nblocks)
{
	BlockNumber physical_nblocks;

	if (!UmbraChunkPairedPhysicalCapacity(logical_nblocks, &physical_nblocks))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("chunk-paired physical capacity overflow for relation %u/%u/%u fork %d logical blocks %u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum, logical_nblocks)));

	return physical_nblocks;
}

static void
um_ensure_chunk_physical_capacity(SMgrRelation reln, ForkNumber forknum,
								  UmbraFileContext *ctx,
								  BlockNumber logical_nblocks,
								  bool skipFsync)
{
	BlockNumber physical_nblocks;
	bool		materialized;

	physical_nblocks =
		um_chunk_physical_capacity_checked(reln, forknum, logical_nblocks);
	if (physical_nblocks == 0)
		return;

	if (umbra_chunk_zero_fill_all_slots)
		materialized = MapSBlockEnsurePhysicalNblocksZeroFill(ctx,
															  reln->smgr_rlocator.locator,
															  forknum,
															  physical_nblocks,
															  skipFsync);
	else
		materialized = MapSBlockEnsurePhysicalNblocks(ctx,
													  reln->smgr_rlocator.locator,
													  forknum,
													  physical_nblocks,
													  skipFsync);
	if (materialized)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("could not materialize chunk-paired physical capacity for relation %u/%u/%u fork %d",
					reln->smgr_rlocator.locator.spcOid,
					reln->smgr_rlocator.locator.dbOid,
					reln->smgr_rlocator.locator.relNumber,
					forknum)));
}

static void
um_report_legacy_entry_map_path(SMgrRelation reln, ForkNumber forknum,
								const UmbraAccessState *access,
								BlockNumber lblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber logical_nblocks = InvalidBlockNumber;

	Assert(access->map_available);

	(void) MapSBlockTryGetLogicalNblocks(ctx,
										 reln->smgr_rlocator.locator,
										 forknum, &logical_nblocks);

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("legacy entry-map path reached in chunk-paired Umbra relation %u/%u/%u fork %d block %u",
					reln->smgr_rlocator.locator.spcOid,
					reln->smgr_rlocator.locator.dbOid,
					reln->smgr_rlocator.locator.relNumber,
					forknum,
					lblkno),
			 errdetail("policy=%d map_available=%s logical_nblocks=%u",
					   (int) access->policy,
					   access->map_available ? "true" : "false",
					   logical_nblocks)));
}

/*
 * Build MAP metadata for relations that stayed on chunk-paired base-slot
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
	bool		delay_chkp_start_set = false;

	/*
	 * Rebuild assumes the relation stayed on chunk-paired base-slot access
	 * during the skip-WAL window. No running path may consume MAP state before
	 * this durable-transition rebuild runs.
	 */
	Assert(RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator));

	Assert(UmMetadataExists(reln));

	for (ForkNumber forknum = MAIN_FORKNUM; forknum <= VISIBILITYMAP_FORKNUM; forknum++)
	{
		BlockNumber nblocks;

		if (!UmbraForkUsesMapTranslation(forknum))
			continue;

		if (!MapSBlockTryGetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
										   forknum, &nblocks))
			nblocks = 0;
		apply_entries[apply_count].forknum = forknum;
		apply_entries[apply_count].nblocks = nblocks;
		apply_count++;

		/*
		 * Empty forks don't need an anchor and may correspond to zero-length
		 * metadata left by aborted storage operations.
		 */
		if (nblocks > 0)
		{
			wal_entries[wal_count].forknum = forknum;
			wal_entries[wal_count].nblocks = nblocks;
			wal_count++;
		}
	}

	/*
	 * Capture skip-WAL frontiers before invalidating relation metadata.  During
	 * the skip-WAL window, logical EOF exists only in the shared superblock entry
	 * until this durable-transition rebuild persists it.
	 */
	MapInvalidateRelation(reln->smgr_rlocator.locator);

	wal_insert_enabled =
		XLogInsertAllowed() &&
		!IsBootstrapProcessingMode() &&
		!IsInitProcessingMode();

	if (wal_count > 0 && wal_insert_enabled)
	{
		START_CRIT_SECTION();
		if ((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0)
		{
			MyProc->delayChkptFlags |= DELAY_CHKPT_START;
			delay_chkp_start_set = true;
		}

		map_lsn = log_umbra_skip_wal_dense_map(reln->smgr_rlocator.locator,
											   wal_count, wal_entries);
	}

	for (uint16 i = 0; i < apply_count; i++)
	{
		ForkNumber	forknum = apply_entries[i].forknum;
		BlockNumber nblocks = apply_entries[i].nblocks;
		XLogRecPtr	fork_lsn = nblocks > 0 ? map_lsn : InvalidXLogRecPtr;
		BlockNumber physical_nblocks;

		/*
		 * Chunk-paired slots are formula-derived.  Dense skip-WAL transition
		 * records only need to make the frontiers durable; active-slot
		 * metadata defaults to slot 0 until a later shift rotates it.
		 */
		physical_nblocks =
			um_chunk_physical_capacity_checked(reln, forknum, nblocks);

		if (physical_nblocks > 0)
		{
			MapSBlockBumpNextFreePhysBlock(ctx, reln->smgr_rlocator.locator,
										   forknum, physical_nblocks, fork_lsn);
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum, physical_nblocks, fork_lsn);
		}
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, nblocks, fork_lsn);
	}

	if (wal_count > 0 && wal_insert_enabled)
	{
		if (delay_chkp_start_set)
			MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
		END_CRIT_SECTION();
	}

	/*
	 * The durable transition publishes metadata just like the old entry-map
	 * path did: after rebuild, relation-local metadata must be visible through
	 * the _map fork, not only through shared MAP caches.
	 */
	MapCheckpointRelation(reln->smgr_rlocator.locator);
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
		um_report_legacy_entry_map_path(reln, forknum, access, lblkno);
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

	um_report_legacy_entry_map_path(reln, forknum, access, lblkno);
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
		return UMBRA_ACCESS_RESOLVED_SKIP;
	}

	if (um_is_stale_post_truncate_lblk_for_access(reln, forknum, access,
												  lookup_state, lblkno))
		return UMBRA_ACCESS_RESOLVED_SKIP;

	um_report_legacy_entry_map_path(reln, forknum, access, lblkno);
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
	Assert(pblkno != NULL);

	if (um_state_uses_chunk_base(access->policy))
	{
		if (mode == UMBRA_ACCESS_RESOLVE_READ &&
			!um_lblk_precedes_logical_eof_for_access(reln, forknum,
													 access, lookup_state,
													 lblkno))
			return um_resolve_lblk_for_read(reln, forknum, access,
											lookup_state, lblkno);

		if (mode == UMBRA_ACCESS_RESOLVE_WRITEBACK &&
			!um_lblk_precedes_logical_eof_for_access(reln, forknum,
													 access, lookup_state,
													 lblkno))
		{
			if (um_is_stale_post_truncate_lblk_for_access(reln, forknum,
														  access,
														  lookup_state,
														  lblkno))
				return UMBRA_ACCESS_RESOLVED_SKIP;
			um_report_legacy_entry_map_path(reln, forknum, access, lblkno);
			pg_unreachable();
		}

		if (mode == UMBRA_ACCESS_RESOLVE_WRITE &&
			!um_lblk_precedes_logical_eof_for_access(reln, forknum,
													 access, lookup_state,
													 lblkno))
		{
			BlockNumber logical_nblocks = InvalidBlockNumber;

			if (lookup_state != NULL && lookup_state->have_logical_nblocks)
				logical_nblocks = lookup_state->logical_nblocks;
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cannot write Umbra relation %u/%u/%u fork %d block %u beyond logical EOF %u",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum, lblkno, logical_nblocks),
					 errdetail("New logical pages must be born through smgrextend or smgrzeroextend before ordinary writes.")));
		}

		if (mode == UMBRA_ACCESS_RESOLVE_READ &&
			um_redo_shift_source_pblk(reln, forknum, lblkno, pblkno))
			return UMBRA_ACCESS_RESOLVED_PBLK;

		*pblkno = um_chunk_active_pblk_checked(reln, forknum, lblkno);
		return UMBRA_ACCESS_RESOLVED_PBLK;
	}

	if (!access->map_available)
	{
		*pblkno = lblkno;
		return UMBRA_ACCESS_RESOLVED_PBLK;
	}

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
	BlockNumber	run_blocks;
	BlockNumber	pblk;

	Assert(access != NULL);
	Assert(start_pblk != NULL);
	Assert(maxblocks > 0);

	if (!access->map_available &&
		!um_state_uses_chunk_base(access->policy))
	{
		*start_pblk = blocknum;
		return Min(maxblocks, (BlockNumber) umfile_maxcombine(forknum, blocknum));
	}

	if (um_state_uses_chunk_base(access->policy))
	{
		if (um_redo_shift_source_pblk(reln, forknum, blocknum, start_pblk))
			return 1;

		run_blocks = um_chunk_active_pblk_run_checked(reln, forknum,
													  blocknum, maxblocks,
													  start_pblk);
		return run_blocks;
	}

	if (um_resolve_lblk_for_access(reln, forknum, access, lookup_state,
								   blocknum, UMBRA_ACCESS_RESOLVE_READ,
								   &pblk) == UMBRA_ACCESS_RESOLVED_PBLK)
	{
		*start_pblk = pblk;
		return 1;
	}

	return 0;
}

static BlockNumber
um_prepare_mapped_birth(SMgrRelation reln, ForkNumber forknum,
						const UmbraAccessState *access, BlockNumber lblkno)
{
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber pblkno;
	BlockNumber logical_nblocks;

	Assert(access->map_available);

	if (lblkno == MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("logical block overflow for relation %u/%u/%u fork %d block %u",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum, lblkno)));

	logical_nblocks = lblkno + 1;
	pblkno = um_chunk_base_pblk_checked(reln, forknum, lblkno);
	um_ensure_chunk_physical_capacity(reln, forknum, ctx, logical_nblocks,
									  true /* skipFsync */);
	MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								forknum, logical_nblocks,
								InvalidXLogRecPtr);
	return pblkno;
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
 *   2. smgrpretruncate() runs before entering the critical section
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
	 * Umbra should have only one logical-size source of truth.
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

	access = um_classify_access(reln, forknum);
	if (um_state_uses_chunk_base(access.policy))
		return um_chunk_active_pblk_checked(reln, forknum, lblkno);

	if (!access.map_available)
		return lblkno;

	um_report_legacy_entry_map_path(reln, forknum, &access, lblkno);
	pg_unreachable();
}

bool
UmUsesChunkPairedTranslation(SMgrRelation reln, ForkNumber forknum)
{
	UmbraAccessState access;

	access = um_classify_access(reln, forknum);
	return um_state_uses_chunk_base(access.policy);
}

bool
UmTranslationTryLookupPblkno(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber lblkno, BlockNumber *pblkno)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (um_state_uses_chunk_base(access.policy))
	{
		BlockNumber logical_nblocks;

		if (!MapSBlockTryGetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
										   forknum, &logical_nblocks) ||
			lblkno >= logical_nblocks)
			return false;

		*pblkno = um_chunk_active_pblk_checked(reln, forknum, lblkno);
		return true;
	}

	if (!access.map_available)
	{
		*pblkno = lblkno;
		return true;
	}

	return false;
}

bool
UmTranslationPhysicalBlockExists(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber lblkno)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	pblk;

	access = um_classify_access(reln, forknum);
	if (um_state_uses_chunk_base(access.policy))
	{
		if (!UmTranslationTryLookupPblkno(reln, forknum, lblkno, &pblk))
			return false;
		return umfile_ctx_block_exists(ctx, forknum, pblk);
	}

	if (!access.map_available)
		return umfile_ctx_block_exists(ctx, forknum, lblkno);

	return false;
}

bool
UmTranslationSlotPhysicalBlockExists(SMgrRelation reln, ForkNumber forknum,
									 BlockNumber lblkno, uint8 slot)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	logical_nblocks;
	BlockNumber	pblk;

	if (!UmbraChunkActiveSlotIsValid(slot))
		return false;

	access = um_classify_access(reln, forknum);
	if (!um_state_uses_chunk_base(access.policy))
		return UmTranslationPhysicalBlockExists(reln, forknum, lblkno);

	if (!MapSBlockTryGetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									   forknum, &logical_nblocks) ||
		lblkno >= logical_nblocks)
		return false;

	pblk = um_chunk_slot_pblk_checked(reln, forknum, lblkno, slot);
	return umfile_ctx_block_exists(ctx, forknum, pblk);
}

bool
UmCheckpointCaptureSlot(SMgrRelation reln, ForkNumber forknum,
						BlockNumber lblkno, uint8 *checkpoint_slot)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	uint8		active_slot = 0;

	Assert(checkpoint_slot != NULL);

	access = um_classify_access(reln, forknum);
	if (!um_state_uses_chunk_base(access.policy))
		return false;

	if (lblkno >= umnblocks_for_access(reln, forknum, &access))
		return false;

	(void) UmbraShiftGet(ctx, reln->smgr_rlocator.locator,
						 forknum, lblkno, &active_slot);
	*checkpoint_slot = active_slot;
	return true;
}

void
UmCheckpointWriteSlot(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber lblkno, const void *buffer,
					  uint8 checkpoint_slot)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber		pblk;

	if (!UmbraChunkActiveSlotIsValid(checkpoint_slot))
		elog(ERROR, "invalid Umbra checkpoint slot %u", checkpoint_slot);

	access = um_classify_access(reln, forknum);
	if (!um_state_uses_chunk_base(access.policy))
		elog(ERROR,
			 "checkpoint slot write requested for non-chunk Umbra relation %u/%u/%u fork %d block %u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber,
			 forknum, lblkno);

	um_ensure_chunk_physical_capacity(reln, forknum, ctx, lblkno + 1,
									  false);
	pblk = um_chunk_slot_pblk_checked(reln, forknum, lblkno,
									  checkpoint_slot);
	umfile_ctx_ensure_block_exists(ctx, forknum, pblk);
	umfile_writev(ctx, forknum, pblk, &buffer, 1, false);
}

void
UmCheckpointWritebackSlot(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber lblkno, uint8 checkpoint_slot)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber		pblk;

	if (!UmbraChunkActiveSlotIsValid(checkpoint_slot))
		return;

	access = um_classify_access(reln, forknum);
	if (!um_state_uses_chunk_base(access.policy))
		return;

	pblk = um_chunk_slot_pblk_checked(reln, forknum, lblkno,
									  checkpoint_slot);
	umfile_writeback(ctx, forknum, pblk, 1);
}

uint8
UmShiftChooseTargetSlot(SMgrRelation reln, ForkNumber forknum, BlockNumber lblkno,
						uint8 *source_slot)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	uint8		active_slot = 0;

	Assert(source_slot != NULL);

	access = um_classify_access(reln, forknum);
	if (!um_state_uses_chunk_base(access.policy))
	{
		*source_slot = 0;
		return 0;
	}

	if (lblkno >= umnblocks_for_access(reln, forknum, &access))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot shift Umbra relation %u/%u/%u fork %d block %u beyond logical EOF",
						reln->smgr_rlocator.locator.spcOid,
						reln->smgr_rlocator.locator.dbOid,
						reln->smgr_rlocator.locator.relNumber,
						forknum, lblkno)));

	(void) UmbraShiftGet(ctx, reln->smgr_rlocator.locator,
						 forknum, lblkno, &active_slot);
	*source_slot = active_slot;
	return UmbraChunkNextActiveSlot(active_slot);
}

void
UmShiftSetActiveSlot(SMgrRelation reln, ForkNumber forknum, BlockNumber lblkno,
					 uint8 active_slot, XLogRecPtr map_lsn)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (um_state_uses_chunk_base(access.policy))
	{
		UmbraShiftSet(ctx, reln->smgr_rlocator.locator,
					  forknum, lblkno, active_slot, map_lsn);
		return;
	}

	if (!access.map_available)
		return;

	elog(ERROR,
		 "legacy entry-map publication is not supported for relation %u/%u/%u fork %d blk %u",
		 reln->smgr_rlocator.locator.spcOid,
		 reln->smgr_rlocator.locator.dbOid,
		 reln->smgr_rlocator.locator.relNumber,
		 forknum, lblkno);
}

void
UmRedoBeginShiftSourceSide(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber lblkno, uint8 source_slot)
{
	Assert(InRecovery);
	Assert(UmbraChunkActiveSlotIsValid(source_slot));

	um_redo_shift_source_state.active = true;
	um_redo_shift_source_state.rlocator = reln->smgr_rlocator.locator;
	um_redo_shift_source_state.forknum = forknum;
	um_redo_shift_source_state.lblkno = lblkno;
	um_redo_shift_source_state.source_slot = source_slot;
}

void
UmRedoEndShiftSourceSide(void)
{
	um_redo_shift_source_state.active = false;
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
		return umfile_exists(ctx, forknum);

	policy = um_map_policy_for_access(reln, forknum);
	if (policy != UMBRA_MAP_POLICY_REQUIRE_MAP)
		return umfile_exists(ctx, forknum);

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

	access = um_classify_access(reln, forknum);

	if (um_state_uses_chunk_direct_base(access.policy))
	{
		pblkno = um_chunk_base_pblk_checked(reln, forknum, blocknum);
		logical_nblocks = umnblocks_for_access(reln, forknum, &access);
		if (blocknum > logical_nblocks)
			umzeroextend(reln, forknum, logical_nblocks,
						 (int) (blocknum - logical_nblocks),
						 skipFsync);
		um_ensure_chunk_physical_capacity(reln, forknum, ctx,
										  blocknum + 1, skipFsync);
		umfile_extend(ctx, forknum, pblkno, buffer, skipFsync);
		MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									 forknum, blocknum + 1,
									 InvalidXLogRecPtr);
		return;
	}

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
	if (um_state_uses_chunk_base(access.policy))
		materialized_nblocks = umfile_nblocks(ctx, forknum);
	else
		(void) MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
											  forknum, &materialized_nblocks);
	if (blocknum > logical_nblocks)
	{
		umzeroextend(reln, forknum, logical_nblocks,
					 (int) (blocknum - logical_nblocks),
					 skipFsync);
		logical_nblocks = blocknum;
	}

	/*
	 * For mapped data forks, smgrextend is the point where an unmapped logical
	 * block gets its first physical block. New pages always start on the
	 * formula-derived slot 0; active-slot metadata remains at its default until
	 * a later shift switches the active slot.
	 */
	if (blocknum < logical_nblocks)
		pblkno = um_chunk_active_pblk_checked(reln, forknum, blocknum);
	else
		pblkno = um_prepare_mapped_birth(reln, forknum, &access, blocknum);

	/*
	 * Extend when the chosen pblk is still beyond the materialized physical
	 * EOF; otherwise just write the page.
	 *
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

	if (max_pblkno != InvalidBlockNumber)
	{
		if (um_state_uses_chunk_base(access.policy))
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum,
										 um_chunk_physical_capacity_checked(reln,
																			forknum,
																			blocknum + 1),
										 InvalidXLogRecPtr);
		else
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum, max_pblkno + 1,
										 InvalidXLogRecPtr);
	}
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

	if (um_state_uses_chunk_direct_base(access.policy))
	{
		BlockNumber logical_nblocks;
		BlockNumber logical_end = blocknum + (BlockNumber) nblocks;
		BlockNumber old_physical_nblocks = InvalidBlockNumber;
		bool		skip_new_capacity_zero;

		if (logical_end < blocknum)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("logical block range overflow for relation %u/%u/%u fork %d",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum)));

		logical_nblocks = umnblocks_for_access(reln, forknum, &access);
		if (blocknum != logical_nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cannot zero-extend Umbra relation %u/%u/%u fork %d from block %u with logical EOF %u",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum, blocknum, logical_nblocks)));

		skip_new_capacity_zero = umbra_chunk_zero_fill_all_slots;
		if (skip_new_capacity_zero &&
			!MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
											forknum, &old_physical_nblocks))
			old_physical_nblocks = 0;
		um_ensure_chunk_physical_capacity(reln, forknum, ctx,
										  logical_end, skipFsync);
		run_start_pblk = InvalidBlockNumber;
		run_len = 0;

		for (int i = 0; i < nblocks; i++)
		{
			BlockNumber pblk = um_chunk_base_pblk_checked(reln, forknum,
														 blocknum + (BlockNumber) i);

			if (skip_new_capacity_zero && pblk >= old_physical_nblocks)
			{
				um_zeroextend_flush_run(ctx, forknum, &run_start_pblk,
										&run_len, skipFsync);
				continue;
			}

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
				um_zeroextend_flush_run(ctx, forknum, &run_start_pblk,
										&run_len, skipFsync);
				run_start_pblk = pblk;
				run_len = 1;
			}
		}

		um_zeroextend_flush_run(ctx, forknum, &run_start_pblk, &run_len,
								skipFsync);

		MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									forknum, logical_end,
									InvalidXLogRecPtr);
		return;
	}

	/* Direct physical path for non-mapped forks. */
	if (!access.map_available)
	{
		umfile_zeroextend(ctx, forknum, blocknum, nblocks,
						  skipFsync);
		return;
	}

	/*
	 * For mapped forks we must materialize each newly-mapped physical page as
	 * a zero page, otherwise a later translated read would hit EOF/short read.
	 *
	 * Chunk-paired placement is contiguous inside each active slot, so
	 * batch physical zero-extension until the formula crosses a non-contiguous
	 * boundary.
	 */
	run_start_pblk = InvalidBlockNumber;
	max_pblkno = InvalidBlockNumber;
	run_len = 0;

	{
		BlockNumber logical_nblocks;
		BlockNumber logical_end = blocknum + (BlockNumber) nblocks;
		BlockNumber old_physical_nblocks = InvalidBlockNumber;
		bool		extends_logical;
		bool		skip_new_capacity_zero = false;

		if (logical_end < blocknum)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("logical block range overflow for relation %u/%u/%u fork %d",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum)));

		logical_nblocks = umnblocks_for_access(reln, forknum, &access);
		extends_logical = logical_end > logical_nblocks;
		if (blocknum != logical_nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cannot zero-extend Umbra relation %u/%u/%u fork %d from block %u with logical EOF %u",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum, blocknum, logical_nblocks)));
		if (extends_logical)
		{
			skip_new_capacity_zero = umbra_chunk_zero_fill_all_slots;
			if (skip_new_capacity_zero &&
				!MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
												forknum, &old_physical_nblocks))
				old_physical_nblocks = 0;
			um_ensure_chunk_physical_capacity(reln, forknum, ctx,
											  logical_end,
											  true /* skipFsync */);
		}

		for (int i = 0; i < nblocks; i++)
		{
			BlockNumber lblk = blocknum + (BlockNumber) i;
			BlockNumber pblk;

			if (lblk < logical_nblocks)
				pblk = um_chunk_active_pblk_checked(reln, forknum, lblk);
			else
				pblk = um_chunk_base_pblk_checked(reln, forknum, lblk);

			if (max_pblkno == InvalidBlockNumber || pblk > max_pblkno)
				max_pblkno = pblk;

			if (skip_new_capacity_zero &&
				lblk >= logical_nblocks &&
				pblk >= old_physical_nblocks)
			{
				um_zeroextend_flush_run(ctx, forknum, &run_start_pblk,
										&run_len, skipFsync);
				continue;
			}

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
				um_zeroextend_flush_run(ctx, forknum, &run_start_pblk,
										&run_len, skipFsync);
				run_start_pblk = pblk;
				run_len = 1;
			}
		}

		um_zeroextend_flush_run(ctx, forknum, &run_start_pblk, &run_len,
								skipFsync);

		if (max_pblkno != InvalidBlockNumber)
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum,
										 um_chunk_physical_capacity_checked(reln,
																			forknum,
																			blocknum + (BlockNumber) nblocks),
										 InvalidXLogRecPtr);

		if (extends_logical)
			MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
										forknum, logical_end,
										InvalidXLogRecPtr);
	}
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks)
{
	UmbraAccessState access;
	UmbraAccessLookupState lookup_state = {0};
	UmbraFileContext *ctx = um_ctx_acquire(reln);
	BlockNumber	total_blocks;

	access = um_classify_access(reln, forknum);

	if (nblocks <= 0)
		return true;

	if (!access.map_available &&
		!um_state_uses_chunk_base(access.policy))
		return umfile_prefetch(ctx, forknum, blocknum, nblocks);

	total_blocks = (BlockNumber) nblocks;
	for (BlockNumber i = 0; i < total_blocks;)
	{
		BlockNumber pblk;
		BlockNumber run_blocks;

		run_blocks = um_resolve_mapped_read_run(reln, forknum, &access,
												&lookup_state, blocknum + i,
												total_blocks - i, &pblk);
		if (run_blocks == 0)
		{
			i++;
			continue;
		}

		if (!umfile_prefetch(ctx, forknum, pblk, (int) run_blocks))
			return false;
		i += run_blocks;
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

	if (um_state_uses_chunk_base(access.policy))
	{
		run_blocks = um_resolve_mapped_read_run(reln, forknum, &access,
												 &lookup_state, blocknum,
												 Min((BlockNumber) io_max_combine_limit,
													 (BlockNumber) umfile_maxcombine(forknum,
																					 um_chunk_base_pblk_checked(reln, forknum, blocknum))),
												 &pblk);
		return Max((BlockNumber) 1, run_blocks);
	}

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
	bool		aux_zero_on_error_read;

	access = um_classify_access(reln, forknum);

	aux_zero_on_error_read = UmbraForkIsAuxiliaryMapped(forknum);

	for (BlockNumber i = 0; i < nblocks;)
	{
		BlockNumber	pblk;
		BlockNumber	run_blocks;

		run_blocks = um_resolve_mapped_read_run(reln, forknum, &access,
											 &lookup_state, blocknum + i,
												 aux_zero_on_error_read ? 1 :
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
		 * FSM/VM callers use RBM_ZERO_ON_ERROR semantics.  A direct physical
		 * read of a missing chunk-paired auxiliary fork would fail before
		 * bufmgr gets a chance to zero the page.
		 */
		if (aux_zero_on_error_read)
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
		um_ensure_datafork_batch_ready_for_access(reln, forknum, access,
												  pblk, true /* skipFsync */ );

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

	if (!access.map_available &&
		!um_state_uses_chunk_base(access.policy))
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
um_flush_write_run(UmbraFileContext *ctx, ForkNumber forknum,
				   BlockNumber run_start_pblk,
				   const void **buffers,
				   BlockNumber run_start_idx,
				   BlockNumber *run_blocks,
				   bool skipFsync)
{
	if (*run_blocks == 0)
		return;

	umfile_writev(ctx, forknum, run_start_pblk, &buffers[run_start_idx],
				  *run_blocks, skipFsync);
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

	access = um_classify_access(reln, forknum);

	if (um_state_uses_chunk_direct_base(access.policy))
	{
		BlockNumber logical_end = blocknum + nblocks;

		if (logical_end < blocknum)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("logical block range overflow for relation %u/%u/%u fork %d",
							reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid,
							reln->smgr_rlocator.locator.relNumber,
							forknum)));

		um_ensure_chunk_physical_capacity(reln, forknum, ctx,
										  logical_end, skipFsync);
		for (BlockNumber i = 0; i < nblocks;)
		{
			BlockNumber lblk = blocknum + i;
			BlockNumber pblk = um_chunk_base_pblk_checked(reln, forknum, lblk);
			BlockNumber run_blocks;

			run_blocks = Min(nblocks - i,
							 (BlockNumber) UMBRA_CHUNK_PAIRED_PAGES -
							 (lblk % (BlockNumber) UMBRA_CHUNK_PAIRED_PAGES));
			umfile_writev(ctx, forknum, pblk, &buffers[i], run_blocks,
						  skipFsync);
			i += run_blocks;
		}

		MapSBlockBumpLogicalNblocks(ctx, reln->smgr_rlocator.locator,
									forknum, logical_end,
									InvalidXLogRecPtr);
		return;
	}

	if (!access.map_available)
	{
		umfile_writev(ctx, forknum, blocknum, buffers, nblocks,
					  skipFsync);
		return;
	}

	if (um_state_uses_chunk_base(access.policy))
		materialized_nblocks = umfile_nblocks(ctx, forknum);
	else
		(void) MapSBlockTryGetPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
											  forknum, &materialized_nblocks);

	if (um_state_uses_chunk_base(access.policy))
	{
		for (BlockNumber i = 0; i < nblocks;)
		{
			BlockNumber lblk = blocknum + i;
			BlockNumber pblk;
			BlockNumber max_run = nblocks - i;
			BlockNumber active_run;
			BlockNumber segment_run;
			BlockNumber write_run;

			active_run = um_chunk_active_pblk_run_checked(reln, forknum, lblk,
														 max_run, &pblk);
			segment_run = Min(active_run,
							  (BlockNumber) RELSEG_SIZE -
							  (pblk % (BlockNumber) RELSEG_SIZE));

			if (pblk < materialized_nblocks)
			{
				write_run = Min(segment_run, materialized_nblocks - pblk);
				write_run = Min(write_run, (BlockNumber) PG_IOV_MAX);
				umfile_writev(ctx, forknum, pblk, &buffers[i], write_run,
							  skipFsync);
				i += write_run;
				continue;
			}

			umfile_extend(ctx, forknum, pblk, buffers[i], skipFsync);

			if (max_extended_pblk == InvalidBlockNumber ||
				pblk > max_extended_pblk)
				max_extended_pblk = pblk;
			if (materialized_nblocks < pblk + 1)
				materialized_nblocks = pblk + 1;
			i++;
		}

		if (max_extended_pblk != InvalidBlockNumber)
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum,
										 um_chunk_physical_capacity_checked(reln,
																			forknum,
																			blocknum + nblocks),
										 InvalidXLogRecPtr);

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
			(run_blocks < (BlockNumber) PG_IOV_MAX) &&
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
				um_flush_write_run(ctx, forknum, run_start_pblk,
								   buffers, run_start_idx,
								   &run_blocks, skipFsync);
				run_start_pblk = pblk;
				run_start_idx = i;
			}

			Assert(run_blocks < (BlockNumber) PG_IOV_MAX);
			run_blocks++;
			continue;
		}

		um_flush_write_run(ctx, forknum, run_start_pblk,
						   buffers, run_start_idx,
						   &run_blocks, skipFsync);
		run_start_pblk = InvalidBlockNumber;

		umfile_extend(ctx, forknum, pblk, buffers[i], skipFsync);

		if (max_extended_pblk == InvalidBlockNumber || pblk > max_extended_pblk)
			max_extended_pblk = pblk;
		if (materialized_nblocks < pblk + 1)
			materialized_nblocks = pblk + 1;
	}

	um_flush_write_run(ctx, forknum, run_start_pblk,
					   buffers, run_start_idx,
					   &run_blocks, skipFsync);

	if (max_extended_pblk != InvalidBlockNumber)
	{
		if (um_state_uses_chunk_base(access.policy))
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum,
										 um_chunk_physical_capacity_checked(reln,
																			forknum,
																			blocknum + nblocks),
										 InvalidXLogRecPtr);
		else
			MapSBlockBumpPhysicalNblocks(ctx, reln->smgr_rlocator.locator,
										 forknum, max_extended_pblk + 1,
										 InvalidXLogRecPtr);
	}

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

	if (um_state_uses_chunk_direct_base(access.policy))
	{
		for (BlockNumber i = 0; i < nblocks;)
		{
			BlockNumber lblk = blocknum + i;
			BlockNumber pblk = um_chunk_base_pblk_checked(reln, forknum, lblk);
			BlockNumber chunk_run;

			chunk_run = Min(nblocks - i,
							(BlockNumber) UMBRA_CHUNK_PAIRED_PAGES -
							(lblk % (BlockNumber) UMBRA_CHUNK_PAIRED_PAGES));
			umfile_writeback(ctx, forknum, pblk, chunk_run);
			i += chunk_run;
		}
		return;
	}

	if (!access.map_available)
	{
		umfile_writeback(ctx, forknum, blocknum, nblocks);
		return;
	}

	if (um_state_uses_chunk_base(access.policy))
	{
		for (BlockNumber i = 0; i < nblocks;)
		{
			BlockNumber lblk = blocknum + i;
			BlockNumber pblk;
			BlockNumber max_run = nblocks - i;
			BlockNumber active_run;
			BlockNumber writeback_run;

			active_run = um_chunk_active_pblk_run_checked(reln, forknum,
														 lblk, max_run,
														 &pblk);
			writeback_run = Min(active_run,
								(BlockNumber) RELSEG_SIZE -
								(pblk % (BlockNumber) RELSEG_SIZE));

			umfile_writeback(ctx, forknum, pblk, writeback_run);
			i += writeback_run;
		}
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

	if (um_state_uses_chunk_base(access->policy))
	{
		if (MapSBlockTryGetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
										  forknum, &nblocks))
			return nblocks;
		return 0;
	}

	if (!access->map_available)
		return umfile_nblocks(ctx, forknum);

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
	(void) reln;
	(void) forknum;
	(void) old_blocks;
	(void) nblocks;
	(void) truncate_lsn;
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	UmbraAccessState access;
	UmbraFileContext *ctx = um_ctx_acquire(reln);

	access = um_classify_access(reln, forknum);
	if (um_fork_uses_map_translation(forknum) &&
		(um_state_uses_chunk_base(access.policy) ||
		 access.map_available))
	{
		XLogRecPtr	map_lsn;

		map_lsn = InRecovery ?
			GetXLogReplayRecPtr(NULL) : GetXLogWriteRecPtr();

		UmbraShiftTruncate(ctx, reln->smgr_rlocator.locator,
						   forknum, nblocks, map_lsn);
		MapSBlockSetLogicalNblocks(ctx, reln->smgr_rlocator.locator,
								   forknum, nblocks, map_lsn);
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
	umfile_registersync(um_ctx_acquire(reln), forknum);
}

bool
umpreparependingsync(SMgrRelation reln)
{
	/*
	 * Skip-WAL relations write data directly first and publish Umbra MAP
	 * metadata only at the durable transition boundary. Rebuild MAP and
	 * superblock before the relation enters the fsync path.
	 */
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
