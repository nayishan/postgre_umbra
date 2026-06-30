/*-------------------------------------------------------------------------
 *
 * umbra_xlog.c
 *	  WAL support for Umbra MAP lifecycle records.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/umbra_xlog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "storage/map.h"
#include "storage/smgr.h"
#include "storage/umbra.h"
#include "storage/umfile.h"

XLogRecPtr
log_umbra_skip_wal_dense_map(RelFileLocator rlocator,
							 uint16 count,
							 const xl_umbra_skip_wal_dense_map_entry *entries)
{
	xl_umbra_skip_wal_dense_map xlrec;

	Assert(count > 0);
	Assert(entries != NULL);

	xlrec.rlocator = rlocator;
	xlrec.count = count;
	xlrec.padding = 0;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec,
					 offsetof(xl_umbra_skip_wal_dense_map, entries));
	XLogRegisterData((char *) entries,
					 sizeof(xl_umbra_skip_wal_dense_map_entry) * count);

	return XLogInsert(RM_UMBRA_ID,
					  XLOG_UMBRA_SKIP_WAL_DENSE_MAP | XLR_SPECIAL_REL_UPDATE);
}

void
umbra_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in Umbra MAP records. */
	Assert(!XLogRecHasAnyBlockRefs(record));

	switch (info)
	{
		case XLOG_UMBRA_SKIP_WAL_DENSE_MAP:
			{
				xl_umbra_skip_wal_dense_map *xlrec;
				xl_umbra_skip_wal_dense_map_entry *entries;
				SMgrRelation reln;
				UmbraFileContext *ctx;

				xlrec = (xl_umbra_skip_wal_dense_map *) XLogRecGetData(record);
				entries = xlrec->entries;
				reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);
				ctx = umfile_ctx_acquire(reln->smgr_rlocator);

				if (!UmMetadataExists(reln))
					break;

				MapInvalidateRelation(xlrec->rlocator);

				for (uint16 i = 0; i < xlrec->count; i++)
				{
					ForkNumber	forknum = entries[i].forknum;
					BlockNumber nblocks = entries[i].nblocks;
					BlockNumber physical_nblocks;

					if (!UmbraForkUsesMapTranslation(forknum) ||
						!BlockNumberIsValid(nblocks))
						elog(PANIC,
							 "invalid UMBRA skip-WAL dense-map record for relation %u/%u/%u fork %d nblocks %u",
							 xlrec->rlocator.spcOid,
							 xlrec->rlocator.dbOid,
							 xlrec->rlocator.relNumber,
							 forknum, nblocks);
					Assert(nblocks > 0);

					if (!UmbraChunkPairedPhysicalCapacity(nblocks,
														 &physical_nblocks))
						elog(PANIC,
							 "invalid UMBRA skip-WAL chunk physical capacity for relation %u/%u/%u fork %d nblocks %u",
							 xlrec->rlocator.spcOid,
							 xlrec->rlocator.dbOid,
							 xlrec->rlocator.relNumber,
							 forknum, nblocks);

					MapSBlockBumpNextFreePhysBlock(ctx, xlrec->rlocator,
												   forknum, physical_nblocks,
												   record->EndRecPtr);
					MapSBlockBumpPhysicalNblocks(ctx, xlrec->rlocator,
												 forknum, physical_nblocks,
												 record->EndRecPtr);
					MapSBlockSetLogicalNblocks(ctx, xlrec->rlocator,
											   forknum, nblocks,
											   record->EndRecPtr);
				}
			}
			break;

		default:
			elog(PANIC, "umbra_redo: unknown op code %u", info);
	}
}
