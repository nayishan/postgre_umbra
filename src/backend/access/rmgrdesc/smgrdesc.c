/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/smgrdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_xlog.h"


void
smgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) rec;

		appendStringInfoString(buf,
							   relpathperm(xlrec->rlocator, xlrec->forkNum).str);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;

		appendStringInfo(buf, "%s to %u blocks flags %d",
						 relpathperm(xlrec->rlocator, MAIN_FORKNUM).str,
						 xlrec->blkno, xlrec->flags);
	}
#ifdef USE_UMBRA
	else if (info == XLOG_SMGR_UMBRA_MAP_EXTEND)
	{
		DecodedBkpBlock *block = XLogRecGetBlock(record, 0);

		appendStringInfo(buf, "%s logical %u..%u physical %u..%u",
						 relpathperm(block->rlocator, block->forknum).str,
						 block->first_lblkno,
						 block->first_lblkno + block->nblocks - 1,
						 block->first_pblkno,
						 block->first_pblkno + block->nblocks - 1);
	}
#endif
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
#ifdef USE_UMBRA
		case XLOG_SMGR_UMBRA_MAP_EXTEND:
			id = "UMBRA_MAP_EXTEND";
			break;
#endif
	}

	return id;
}
