
#include "postgres.h"

#include "storage/shadow.h"

void
shd_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SHD_RELMETA_ALLOCATE)
	{
		xl_shd_relmeta xlrec;
		memcpy(&xlrec, rec, SizeOfShdRelMeta);
		appendStringInfo(buf, "dbsId:%d; lrelId:%d; oid:%d", xlrec.sdbId, xlrec.lrelId, xlrec.oid);
	}
	else if (info == XLOG_SHD_DBMETA_ALLOCATE)
	{
	  xl_shd_dbmeta xlrec;
	  memcpy(&xlrec, rec, SizeOfShdDbMeta);
	  appendStringInfo(buf, "dbsId:%d; dbOid:%d; srcSdbId:%d, srcDbOid:%d", xlrec.sdbId, xlrec.dbOid, xlrec.srcSdbId, xlrec.srcDbOid);
	}
}

const char *
shd_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SHD_RELMETA_ALLOCATE:
			id = "RELMETA_ALLOCATE";
			break;
		case XLOG_SHD_DBMETA_ALLOCATE:
			id = "DBMETA_ALLOCATE";
			break;
	}

	return id;
}
