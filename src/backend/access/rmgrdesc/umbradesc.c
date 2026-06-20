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

void
umbra_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_UMBRA_SKIP_WAL_DENSE_MAP)
	{
		xl_umbra_skip_wal_dense_map *xlrec =
			(xl_umbra_skip_wal_dense_map *) rec;
		RelPathStr	path = umbra_metadata_relpath(xlrec->rlocator);

		appendStringInfo(buf, "%s skip_wal_chunk_base count %u",
						 path.str, xlrec->count);
		for (uint16 i = 0; i < xlrec->count; i++)
			appendStringInfo(buf, " fork %d nblocks %u",
							 xlrec->entries[i].forknum,
							 xlrec->entries[i].nblocks);
	}
}

const char *
umbra_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_UMBRA_SKIP_WAL_DENSE_MAP:
			id = "SKIP_WAL_CHUNK_BASE";
			break;
	}

	return id;
}
