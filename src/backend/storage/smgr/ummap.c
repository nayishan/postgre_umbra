/*-------------------------------------------------------------------------
 *
 * ummap.c
 *    Umbra selector-MAP lifecycle.
 *
 * The metadata fork stores selector pages only.  It does not define the
 * physical layout, relation extent, or active slot default for a missing
 * selector page.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/um_defs.h"
#include "storage/umfile.h"
#include "storage/ummap.h"

void
ummap_sync_relation_metadata(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator)
{
	Assert(ctx != NULL);

	/* Core has already made ordinary data forks durable at this point. */
	MapFlushRelation(ctx, rlocator);
	if (umfile_exists(ctx, UMBRA_METADATA_FORKNUM))
		umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	MapInvalidateRelation(rlocator);
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

void
ummap_flush_database_tablespace_cache(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapFlushDatabaseTablespace(dbid, spcOid);
}

void
ummap_invalidate_database_cache(Oid dbid)
{
	Assert(OidIsValid(dbid));
	MapInvalidateDatabase(dbid);
}

void
ummap_invalidate_database_tablespace_cache(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapInvalidateDatabaseTablespace(dbid, spcOid);
}

void
ummap_checkpoint(void)
{
	/* CheckPointBuffers() has completed the data-before-selector phase. */
	MapCheckpoint();
}
