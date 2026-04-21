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

/*
 * Log a mapping establishment/switch for one logical block.
 *
 * The chosen physical block number is recorded in WAL so redo never allocates
 * locally; that keeps mapping deterministic in recovery.
 */
XLogRecPtr
log_umbra_map_set(RelFileLocator rlocator, ForkNumber forknum,
				  BlockNumber lblkno, BlockNumber old_pblkno,
				  BlockNumber new_pblkno)
{
	xl_umbra_map_set xlrec;

	xlrec.rlocator = rlocator;
	xlrec.forknum = forknum;
	xlrec.lblkno = lblkno;
	xlrec.old_pblkno = old_pblkno;
	xlrec.new_pblkno = new_pblkno;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));

	return XLogInsert(RM_UMBRA_ID, XLOG_UMBRA_MAP_SET | XLR_SPECIAL_REL_UPDATE);
}

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
		case XLOG_UMBRA_MAP_SET:
			{
				xl_umbra_map_set *xlrec = (xl_umbra_map_set *) XLogRecGetData(record);
				SMgrRelation reln;
				UmbraFileContext *ctx;
				static const PGIOAlignedBlock zero_page = {{0}};
				bool		materialized = false;

				reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);
				ctx = umfile_ctx_acquire(reln->smgr_rlocator);

				/*
				 * During replay of drop/tablespace churn, the relation path can
				 * already be gone. Treat missing MAIN+MAP as stale WAL and skip.
				 */
				if (!UmMetadataExists(reln))
					break;

				/*
				 * MAP_SET(old,new) replays the physical copy first, then switches
				 * the mapping. This keeps crash recovery independent of whether
				 * background flushing persisted the new physical page before crash.
				 */
				if (xlrec->old_pblkno != InvalidBlockNumber)
				{
					BlockNumber nblocks;
					char		pagebuf[BLCKSZ];

					nblocks = umfile_ctx_get_nblocks(ctx, xlrec->forknum,
													 UMFILE_NBLOCKS_SPARSE);
					if (xlrec->old_pblkno < nblocks)
					{
						umfile_ctx_read(ctx, xlrec->forknum, xlrec->old_pblkno,
										pagebuf, BLCKSZ);
						umfile_ctx_extend(ctx, xlrec->forknum, xlrec->new_pblkno,
										  pagebuf);
						umfile_ctx_register_dirty(ctx, xlrec->forknum,
												  xlrec->new_pblkno,
												  false, false);
						materialized = true;
					}
					else
					{
						ereport(DEBUG1,
								(errmsg_internal("skip UMBRA MAP_SET relocation replay for relation %u/%u/%u fork %d lblk %u: old pblk %u beyond nblocks %u",
												 xlrec->rlocator.spcOid,
												 xlrec->rlocator.dbOid,
												 xlrec->rlocator.relNumber,
												 xlrec->forknum,
												 xlrec->lblkno,
												 xlrec->old_pblkno,
												 nblocks)));
					}
				}

				MapSetMapping(ctx, xlrec->rlocator, xlrec->forknum,
							  xlrec->lblkno, xlrec->new_pblkno,
							  record->EndRecPtr);
				MapSBlockBumpNextFreePhysBlock(ctx, xlrec->rlocator,
											   xlrec->forknum,
											   xlrec->new_pblkno + 1,
											   record->EndRecPtr);

				/*
				 * MAP_SET with invalid old_pblkno means first mapping for this
				 * logical block (extend/zeroextend path). Keep superblock
				 * logical_nblocks in sync during redo as well.
				 */
				if (xlrec->old_pblkno == InvalidBlockNumber)
				{
					/*
					 * Ensure the mapped physical page exists even if there is no
					 * later WAL record that overwrites it (e.g. dummy pages used
					 * to fill gaps for smgrextend semantics).  It's safe to write
					 * zeros even if a later record will overwrite the page image.
					 */
					umfile_ctx_extend(ctx, xlrec->forknum, xlrec->new_pblkno,
									  (const char *) zero_page.data);
					umfile_ctx_register_dirty(ctx, xlrec->forknum,
											  xlrec->new_pblkno,
											  false, false);
					materialized = true;

					MapSBlockBumpLogicalNblocks(ctx, xlrec->rlocator,
												xlrec->forknum,
												xlrec->lblkno + 1,
												record->EndRecPtr);
				}

				if (materialized)
					MapSBlockBumpPhysicalNblocks(ctx, xlrec->rlocator,
												 xlrec->forknum,
												 xlrec->new_pblkno + 1,
												 record->EndRecPtr);
			}
			break;

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

					if (!UmbraForkUsesMapTranslation(forknum) ||
						!BlockNumberIsValid(nblocks))
						elog(PANIC,
							 "invalid UMBRA skip-WAL dense-map record for relation %u/%u/%u fork %d nblocks %u",
							 xlrec->rlocator.spcOid,
							 xlrec->rlocator.dbOid,
							 xlrec->rlocator.relNumber,
							 forknum, nblocks);
					Assert(nblocks > 0);

					for (BlockNumber lblk = 0; lblk < nblocks; lblk++)
						MapSetMapping(ctx, xlrec->rlocator, forknum,
									  lblk, lblk, record->EndRecPtr);

					MapSBlockBumpNextFreePhysBlock(ctx, xlrec->rlocator,
												   forknum, nblocks,
												   record->EndRecPtr);
					MapSBlockBumpPhysicalNblocks(ctx, xlrec->rlocator,
												 forknum, nblocks,
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
