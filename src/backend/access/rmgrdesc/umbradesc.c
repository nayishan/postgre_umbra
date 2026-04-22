/*-------------------------------------------------------------------------
 *
 * umbradesc.c
 *	  rmgr descriptor routines for Umbra MAP WAL records
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/umbra_xlog.h"
#include "common/relpath.h"
#include "storage/um_defs.h"

static RelPathStr
umbra_metadata_relpath(RelFileLocator rlocator)
{
	RelPathStr	base;
	RelPathStr	path;

	base = relpathperm(rlocator, MAIN_FORKNUM);
	snprintf(path.str, sizeof(path.str), "%s_map", base.str);
	return path;
}

static RelPathStr
umbra_fork_relpath(RelFileLocator rlocator, ForkNumber forknum)
{
	if (forknum == UMBRA_METADATA_FORKNUM)
		return umbra_metadata_relpath(rlocator);

	return relpathperm(rlocator, forknum);
}

void
umbra_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_UMBRA_MAP_SET)
	{
		xl_umbra_map_set *xlrec = (xl_umbra_map_set *) rec;
		RelPathStr	path = umbra_fork_relpath(xlrec->rlocator, xlrec->forknum);

		appendStringInfo(buf, "%s lblk %u old %u new %u",
						 path.str, xlrec->lblkno, xlrec->old_pblkno,
						 xlrec->new_pblkno);
	}
	else if (info == XLOG_UMBRA_RANGE_REMAP)
	{
		xl_umbra_range_remap *xlrec = (xl_umbra_range_remap *) rec;
		RelPathStr	path = umbra_fork_relpath(xlrec->rlocator, xlrec->forknum);

		appendStringInfo(buf, "%s count %u end_lblk %u",
						 path.str, xlrec->count, xlrec->end_lblkno);
	}
	else if (info == XLOG_UMBRA_RANGE_REMAP_COMPACT)
	{
		xl_umbra_range_remap_compact *xlrec =
			(xl_umbra_range_remap_compact *) rec;
		RelPathStr	path = umbra_fork_relpath(xlrec->rlocator, xlrec->forknum);

		appendStringInfo(buf, "%s compact first_lblk %u first_pblk %u count %u",
						 path.str, xlrec->first_lblkno, xlrec->first_pblkno,
						 xlrec->count);
	}
	else if (info == XLOG_UMBRA_SKIP_WAL_DENSE_MAP)
	{
		xl_umbra_skip_wal_dense_map *xlrec =
			(xl_umbra_skip_wal_dense_map *) rec;
		RelPathStr	path = umbra_metadata_relpath(xlrec->rlocator);

		appendStringInfo(buf, "%s skip_wal_dense count %u",
						 path.str, xlrec->count);
		for (uint16 i = 0; i < xlrec->count; i++)
			appendStringInfo(buf, " fork %d nblocks %u",
							 xlrec->entries[i].forknum,
							 xlrec->entries[i].nblocks);
	}
	else if (info == XLOG_UMBRA_RECLAIM_UNLINK)
	{
		xl_umbra_reclaim_unlink *xlrec = (xl_umbra_reclaim_unlink *) rec;
		RelPathStr	path = umbra_fork_relpath(xlrec->rlocator, xlrec->forknum);

		appendStringInfo(buf, "%s seg %u reclaim_unlink",
						 path.str, xlrec->segno);
	}
}

const char *
umbra_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_UMBRA_MAP_SET:
			id = "MAP_SET";
			break;
		case XLOG_UMBRA_RANGE_REMAP:
			id = "RANGE_REMAP";
			break;
		case XLOG_UMBRA_RANGE_REMAP_COMPACT:
			id = "RANGE_REMAP_COMPACT";
			break;
		case XLOG_UMBRA_SKIP_WAL_DENSE_MAP:
			id = "SKIP_WAL_DENSE_MAP";
			break;
		case XLOG_UMBRA_RECLAIM_UNLINK:
			id = "RECLAIM_UNLINK";
			break;
	}

	return id;
}
