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
log_umbra_range_remap(RelFileLocator rlocator, ForkNumber forknum,
					  uint16 count,
					  const xl_umbra_range_remap_entry *entries)
{
	xl_umbra_range_remap xlrec;

	Assert(count > 0);
	Assert(entries != NULL);

	xlrec.rlocator = rlocator;
	xlrec.forknum = forknum;
	xlrec.count = count;
	xlrec.padding = 0;
	xlrec.end_lblkno = entries[count - 1].lblkno;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, offsetof(xl_umbra_range_remap, entries));
	XLogRegisterData((char *) entries,
					 sizeof(xl_umbra_range_remap_entry) * count);

	return XLogInsert(RM_UMBRA_ID, XLOG_UMBRA_RANGE_REMAP | XLR_SPECIAL_REL_UPDATE);
}

XLogRecPtr
log_umbra_range_remap_compact(RelFileLocator rlocator, ForkNumber forknum,
							  BlockNumber first_lblkno,
							  BlockNumber first_pblkno,
							  uint16 count)
{
	xl_umbra_range_remap_compact xlrec;

	Assert(count > 0);

	xlrec.rlocator = rlocator;
	xlrec.forknum = forknum;
	xlrec.count = count;
	xlrec.padding = 0;
	xlrec.first_lblkno = first_lblkno;
	xlrec.first_pblkno = first_pblkno;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));

	return XLogInsert(RM_UMBRA_ID,
					  XLOG_UMBRA_RANGE_REMAP_COMPACT | XLR_SPECIAL_REL_UPDATE);
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

					nblocks = umfile_ctx_get_nblocks(ctx, xlrec->forknum);
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

		case XLOG_UMBRA_RANGE_REMAP:
			{
				xl_umbra_range_remap *xlrec;
				xl_umbra_range_remap_entry *entries;
				SMgrRelation reln;
				BlockNumber *pblknos;

				xlrec = (xl_umbra_range_remap *) XLogRecGetData(record);
				entries = xlrec->entries;
				reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);

				if (!UmMetadataExists(reln))
					break;

				pblknos = palloc(sizeof(BlockNumber) * xlrec->count);
				for (int i = 0; i < xlrec->count; i++)
					pblknos[i] = entries[i].new_pblkno;

				UmApplyReservedRangeRemap(reln, xlrec->forknum,
										  entries[0].lblkno, xlrec->count,
										  pblknos, record->EndRecPtr, true);
				pfree(pblknos);
			}
			break;

		case XLOG_UMBRA_RANGE_REMAP_COMPACT:
			{
				xl_umbra_range_remap_compact *xlrec;
				SMgrRelation reln;
				BlockNumber *pblknos;

				xlrec = (xl_umbra_range_remap_compact *) XLogRecGetData(record);
				reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);

				if (!UmMetadataExists(reln))
					break;

				pblknos = palloc(sizeof(BlockNumber) * xlrec->count);
				for (int i = 0; i < xlrec->count; i++)
					pblknos[i] = xlrec->first_pblkno + i;

				UmApplyReservedRangeRemap(reln, xlrec->forknum,
										  xlrec->first_lblkno, xlrec->count,
										  pblknos, record->EndRecPtr, true);
				pfree(pblknos);
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

					for (BlockNumber lblk = 0; lblk < nblocks; lblk++)
					{
						BlockNumber pblk;

						if (!UmbraChunkPairedBasePblk(lblk, &pblk))
							elog(PANIC,
								 "invalid UMBRA skip-WAL chunk base mapping for relation %u/%u/%u fork %d lblk %u",
								 xlrec->rlocator.spcOid,
								 xlrec->rlocator.dbOid,
								 xlrec->rlocator.relNumber,
								 forknum, lblk);

						MapSetMapping(ctx, xlrec->rlocator, forknum,
									  lblk, pblk, record->EndRecPtr);
					}

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
