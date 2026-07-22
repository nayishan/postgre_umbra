/*-------------------------------------------------------------------------
 *
 * umbra.c
 *	  Umbra storage manager.
 *
 * This file owns the smgr implementation boundary for Umbra.  Ordinary
 * relation fork operations are serviced by Umbra's md-style physical segment
 * file layer in umfile.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/umbra.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "storage/aio.h"
#include "storage/smgr.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct UmbraSmgrRelationState
{
	/*
	 * State stored in SMgrRelationData.smgr_private.
	 *
	 * Umbra owns this state for the lifetime of the SMgrRelation handle.
	 * umfile.c owns the physical segment descriptors below this context.
	 */
	UmbraFileContext *filectx;
	bool		uses_map;
	XLogRecPtr	generation_lsn;
} UmbraSmgrRelationState;

typedef struct UmbraRedoGenerationState
{
	RelFileLocator rlocator;		/* hash key, must be first */
	bool		discard_precreate_redo;
} UmbraRedoGenerationState;

static UmbraFileContext *um_get_filectx(SMgrRelation reln);
static void um_reset_recovery_storage(SMgrRelation reln);
static UmbraRedoGenerationState *um_get_redo_generation_state(
	RelFileLocator rlocator, bool create);
static void um_forget_redo_generation_state(RelFileLocator rlocator);
static void um_forget_redo_generation_database(Oid dbid, Oid spcOid);

static HTAB *um_redo_generation_hash = NULL;

void
uminit(void)
{
	umfile_init();
}

void
umopen(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	/*
	 * smgropen() enters its hash table before invoking this callback.  Make
	 * initialization retryable in case an allocation ERROR is caught by the
	 * caller and leaves that SMgrRelation entry in place.
	 */
	if (state == NULL)
	{
		state = MemoryContextAllocZero(TopMemoryContext,
									   sizeof(UmbraSmgrRelationState));
		reln->smgr_private = state;
	}

	if (state->filectx == NULL)
	{
		state->filectx = umfile_open(reln->smgr_rlocator);
		state->uses_map = !RelFileLocatorBackendIsTemp(reln->smgr_rlocator) &&
			ummap_exists(state->filectx);
		state->generation_lsn = InvalidXLogRecPtr;
	}

	/* CREATE redo repairs a deterministic root before normal validation. */
	if (!InRecovery && state->uses_map && IsUnderPostmaster &&
		!IsBootstrapProcessingMode() && !IsInitProcessingMode())
	{
		ummap_validate_if_exists(state->filectx, reln->smgr_rlocator);
		state->generation_lsn = ummap_get_generation_lsn(state->filectx,
													 reln->smgr_rlocator);
	}
}

void
umclose(SMgrRelation reln, ForkNumber forknum)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL && state->filectx != NULL)
	{
		if (forknum == MAIN_FORKNUM)
			umfile_close(state->filectx, UMBRA_METADATA_FORKNUM);
		umfile_close(state->filectx, forknum);
	}
}

void
umdestroy(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	if (state != NULL)
	{
		if (state->filectx != NULL)
			umfile_destroy(state->filectx);
		pfree(state);
		reln->smgr_private = NULL;
	}
}

void
umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx = um_get_filectx(reln);

	Assert(state != NULL);
	umfile_create(ctx, forknum, isRedo);
	if (isRedo && forknum == MAIN_FORKNUM &&
		!RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		state->uses_map = true;
}

void
uminitnewrelation(SMgrRelation reln, bool needs_wal)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	state->uses_map = needs_wal && IsUnderPostmaster &&
		!IsBootstrapProcessingMode() && !IsInitProcessingMode();
	state->generation_lsn = InvalidXLogRecPtr;
	if (state->uses_map)
		ummap_create(um_get_filectx(reln), reln->smgr_rlocator, false);
}

void
umsetgeneration(SMgrRelation reln, ForkNumber forknum,
				XLogRecPtr generation_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;

	Assert(state != NULL);
	if (forknum == MAIN_FORKNUM && state->uses_map)
	{
		ummap_set_generation_lsn(state->filectx, reln->smgr_rlocator,
								 generation_lsn);
		state->generation_lsn = generation_lsn;
	}
}

void
umredocreate(SMgrRelation reln, ForkNumber forknum,
				 XLogRecPtr generation_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;
	UmbraRedoGenerationState *redo_state;
	bool		empty_storage = true;

	Assert(InRecovery);
	Assert(state != NULL);
	Assert(forknum >= 0 && forknum <= MAX_FORKNUM);
	if (forknum != MAIN_FORKNUM ||
		RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return;

	ctx = state->filectx;
	state->uses_map = true;
	if (!XLogRecPtrIsValid(state->generation_lsn))
	{
		if (ummap_exists(ctx))
			empty_storage = ummap_is_empty(ctx, reln->smgr_rlocator);
		for (int forkidx = 0; empty_storage && forkidx <= MAX_FORKNUM;
			 forkidx++)
		{
			ForkNumber	fork = (ForkNumber) forkidx;

			if (umfile_exists(ctx, fork) &&
				umfile_nblocks(ctx, fork) != 0)
				empty_storage = false;
		}
	}

	/* An authoritative CREATE never inherits another generation's files. */
	if ((XLogRecPtrIsValid(state->generation_lsn) &&
		 state->generation_lsn != generation_lsn) ||
		(!XLogRecPtrIsValid(state->generation_lsn) && !empty_storage))
		um_reset_recovery_storage(reln);

	/* CREATE redo is the one path allowed to repair a damaged root. */
	ummap_create(ctx, reln->smgr_rlocator, true);
	ummap_set_generation_lsn(ctx, reln->smgr_rlocator, generation_lsn);
	state->generation_lsn = generation_lsn;

	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 true);
	redo_state->discard_precreate_redo = false;
}

void
umprepareredo(SMgrRelation reln, XLogRecPtr replay_lsn)
{
	UmbraRedoGenerationState *redo_state;

	if (!InRecovery)
		return;
	Assert(XLogRecPtrIsValid(replay_lsn));
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	if (redo_state != NULL && redo_state->discard_precreate_redo)
		return;
	if (umredogenerationahead(reln, replay_lsn))
	{
		um_reset_recovery_storage(reln);
		/* Missing old-generation pages now wait for DROP or TRUNCATE WAL. */
		redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 true);
		redo_state->discard_precreate_redo = true;
	}
}

bool
umredogenerationahead(SMgrRelation reln, XLogRecPtr replay_lsn)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraRedoGenerationState *redo_state;
	XLogRecPtr	generation_lsn;

	if (!InRecovery || state == NULL || !state->uses_map ||
		!XLogRecPtrIsValid(replay_lsn))
		return false;
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	if (redo_state != NULL && redo_state->discard_precreate_redo)
		return true;
	if (!XLogRecPtrIsValid(state->generation_lsn))
	{
		if (!ummap_try_get_generation_lsn(state->filectx,
										reln->smgr_rlocator, &generation_lsn) ||
			!XLogRecPtrIsValid(generation_lsn))
			return false;
		state->generation_lsn = generation_lsn;
	}
	return replay_lsn < state->generation_lsn;
}

bool
UmRedoDiscardingPrecreateRecords(SMgrRelation reln)
{
	UmbraRedoGenerationState *redo_state;

	if (!InRecovery || reln == NULL)
		return false;
	redo_state = um_get_redo_generation_state(reln->smgr_rlocator.locator,
													 false);
	return redo_state != NULL && redo_state->discard_precreate_redo;
}

void
umcheckpoint(void)
{
	ummap_checkpoint();
}

void
umflushdatabasetablespace(Oid dbid, Oid spcOid)
{
	ummap_flush_database_tablespace(dbid, spcOid);
}

void
uminvalidatedatabase(Oid dbid)
{
	um_forget_redo_generation_database(dbid, InvalidOid);
	ummap_invalidate_database(dbid);
}

void
uminvalidatedatabasetablespace(Oid dbid, Oid spcOid)
{
	um_forget_redo_generation_database(dbid, spcOid);
	ummap_invalidate_database_tablespace(dbid, spcOid);
}

bool
umexists(SMgrRelation reln, ForkNumber forknum)
{
	return umfile_exists(um_get_filectx(reln), forknum);
}

void
umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	if (forknum == MAIN_FORKNUM || forknum == InvalidForkNumber)
	{
		um_forget_redo_generation_state(rlocator.locator);
		ummap_unlink(rlocator, isRedo);
	}
	umfile_unlink(rlocator, forknum, isRedo);
}

void
umextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
{
	umfile_extend(um_get_filectx(reln), forknum, blocknum, buffer, skipFsync);
}

void
umzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 int nblocks, bool skipFsync)
{
	umfile_zeroextend(um_get_filectx(reln), forknum, blocknum, nblocks,
					  skipFsync);
}

bool
umprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   int nblocks)
{
	return umfile_prefetch(um_get_filectx(reln), forknum, blocknum, nblocks);
}

uint32
ummaxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return umfile_maxcombine(um_get_filectx(reln), forknum, blocknum);
}

void
umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	umfile_readv(um_get_filectx(reln), forknum, blocknum, buffers, nblocks);
}

void
umstartreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	/* Physical and logical block identities are equal in this patch. */
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	umfile_startreadv(ioh, um_get_filectx(reln), forknum, blocknum, buffers,
					  nblocks);
}

void
umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	umfile_writev(um_get_filectx(reln), forknum, blocknum, buffers, nblocks,
				  skipFsync);
}

void
umwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	umfile_writeback(um_get_filectx(reln), forknum, blocknum, nblocks);
}

BlockNumber
umnblocks(SMgrRelation reln, ForkNumber forknum)
{
	return umfile_nblocks(um_get_filectx(reln), forknum);
}

void
umtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber old_blocks, BlockNumber nblocks)
{
	umfile_truncate(um_get_filectx(reln), forknum, old_blocks, nblocks);
}

void
umimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_immedsync(ctx, forknum);
	if (forknum == MAIN_FORKNUM)
		ummap_immedsync_if_exists(ctx, reln->smgr_rlocator);
}

void
umregistersync(SMgrRelation reln, ForkNumber forknum)
{
	UmbraFileContext *ctx = um_get_filectx(reln);

	umfile_registersync(ctx, forknum);
	if (forknum == MAIN_FORKNUM)
		ummap_registersync_if_exists(ctx, reln->smgr_rlocator);
}

int
umfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	return umfile_fd(um_get_filectx(reln), forknum, blocknum, off);
}

/*
 * A second recovery can start before an old generation's DROP while a newer
 * generation already occupies the same locator.  Replace that storage before
 * replay reads an old page through it.
 */
static void
um_reset_recovery_storage(SMgrRelation reln)
{
	UmbraSmgrRelationState *state = reln->smgr_private;
	UmbraFileContext *ctx;
	SMgrRelation rels[1] = {reln};

	Assert(InRecovery);
	Assert(state != NULL);
	ctx = state->filectx;

	/* Drop shared buffers and stale descriptors before locator reuse. */
	smgrdounlinkall(rels, lengthof(rels), true);
	if (!umfile_unlink(reln->smgr_rlocator, InvalidForkNumber, true))
		elog(PANIC,
			 "could not remove old Umbra storage generation for relation %u/%u/%u",
			 reln->smgr_rlocator.locator.spcOid,
			 reln->smgr_rlocator.locator.dbOid,
			 reln->smgr_rlocator.locator.relNumber);
	umfile_create(ctx, MAIN_FORKNUM, true);
	ummap_create(ctx, reln->smgr_rlocator, true);

	state->uses_map = true;
	state->generation_lsn = InvalidXLogRecPtr;
	for (int forkidx = 0; forkidx <= MAX_FORKNUM; forkidx++)
		reln->smgr_cached_nblocks[forkidx] = InvalidBlockNumber;
	reln->smgr_targblock = InvalidBlockNumber;
}

static UmbraRedoGenerationState *
um_get_redo_generation_state(RelFileLocator rlocator, bool create)
{
	UmbraRedoGenerationState *entry;
	bool		found;

	if (um_redo_generation_hash == NULL)
	{
		HASHCTL		ctl;

		if (!create)
			return NULL;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileLocator);
		ctl.entrysize = sizeof(UmbraRedoGenerationState);
		ctl.hcxt = TopMemoryContext;
		um_redo_generation_hash =
			hash_create("Umbra redo generation state", 128, &ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	entry = hash_search(um_redo_generation_hash, &rlocator,
						create ? HASH_ENTER : HASH_FIND,
						create ? &found : NULL);
	if (create && !found)
		entry->discard_precreate_redo = false;
	return entry;
}

static void
um_forget_redo_generation_state(RelFileLocator rlocator)
{
	if (um_redo_generation_hash != NULL)
		(void) hash_search(um_redo_generation_hash, &rlocator, HASH_REMOVE,
							   NULL);
}

static void
um_forget_redo_generation_database(Oid dbid, Oid spcOid)
{
	HASH_SEQ_STATUS status;
	UmbraRedoGenerationState *entry;

	if (um_redo_generation_hash == NULL)
		return;

	hash_seq_init(&status, um_redo_generation_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->rlocator.dbOid != dbid ||
			(OidIsValid(spcOid) && entry->rlocator.spcOid != spcOid))
			continue;
		if (hash_search(um_redo_generation_hash, &entry->rlocator,
							HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "Umbra redo generation hash table corrupted");
	}
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
