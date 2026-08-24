/*-------------------------------------------------------------------------
 *
 * ummap.c
 *    Umbra selector-MAP lifecycle.
 *
 * The metadata fork stores selector pages only.  It does not define relation
 * extent or layout, and a missing selector page means slot 0.
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

	/* Core has already made the relation's ordinary forks durable. */
	MapFlushRelation(ctx, rlocator);
	if (umfile_exists(ctx, UMBRA_METADATA_FORKNUM))
		umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_checkpoint(void)
{
	/* CheckPointBuffers() has completed the data-before-selector phase. */
	MapCheckpoint();
}
