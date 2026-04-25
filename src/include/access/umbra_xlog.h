/*-------------------------------------------------------------------------
 *
 * umbra_xlog.h
 *	  WAL support for Umbra MAP metadata.
 *
 * Umbra logs these record types:
 * - MAP_SET: establish/switch lblkno -> pblkno mapping
 * - SKIP_WAL_DENSE_MAP: record non-empty skip-WAL dense lblk==pblk frontiers
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMBRA_XLOG_H
#define UMBRA_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

/* XLOG gives us high 4 bits */
#define XLOG_UMBRA_MAP_SET			0x10
#define XLOG_UMBRA_SKIP_WAL_DENSE_MAP	0x60

typedef struct xl_umbra_map_set
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber lblkno;
	BlockNumber old_pblkno;
	BlockNumber new_pblkno;
} xl_umbra_map_set;

typedef struct xl_umbra_skip_wal_dense_map_entry
{
	ForkNumber	forknum;
	BlockNumber nblocks;
} xl_umbra_skip_wal_dense_map_entry;

typedef struct xl_umbra_skip_wal_dense_map
{
	RelFileLocator rlocator;
	uint16		count;
	uint16		padding;
	xl_umbra_skip_wal_dense_map_entry entries[FLEXIBLE_ARRAY_MEMBER];
} xl_umbra_skip_wal_dense_map;

extern XLogRecPtr log_umbra_map_set(RelFileLocator rlocator, ForkNumber forknum,
									BlockNumber lblkno, BlockNumber old_pblkno,
									BlockNumber new_pblkno);
extern XLogRecPtr log_umbra_skip_wal_dense_map(RelFileLocator rlocator,
											   uint16 count,
											   const xl_umbra_skip_wal_dense_map_entry *entries);

extern void umbra_redo(XLogReaderState *record);
extern void umbra_desc(StringInfo buf, XLogReaderState *record);
extern const char *umbra_identify(uint8 info);

#endif							/* UMBRA_XLOG_H */
