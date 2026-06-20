/*-------------------------------------------------------------------------
 *
 * umbra_xlog.h
 *	  WAL support for Umbra MAP metadata.
 *
 * Umbra logs these record types:
 * - MAP_SET: establish/switch lblkno -> pblkno mapping
 * - RANGE_REMAP: atomically establish a range of first-born mappings
 * - RANGE_REMAP_COMPACT: same semantics for contiguous lblk/pblk runs
 * - SKIP_WAL_DENSE_MAP: record non-empty skip-WAL chunk-base frontiers
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
#define XLOG_UMBRA_RANGE_REMAP		0x30
#define XLOG_UMBRA_RANGE_REMAP_COMPACT	0x50
#define XLOG_UMBRA_SKIP_WAL_DENSE_MAP	0x60

typedef struct xl_umbra_map_set
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber lblkno;
	BlockNumber old_pblkno;
	BlockNumber new_pblkno;
} xl_umbra_map_set;

typedef struct xl_umbra_range_remap_entry
{
	BlockNumber	lblkno;
	BlockNumber	new_pblkno;
} xl_umbra_range_remap_entry;

typedef struct xl_umbra_range_remap
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	uint16		count;
	uint16		padding;
	BlockNumber end_lblkno;
	xl_umbra_range_remap_entry entries[FLEXIBLE_ARRAY_MEMBER];
} xl_umbra_range_remap;

typedef struct xl_umbra_range_remap_compact
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	uint16		count;
	uint16		padding;
	BlockNumber	first_lblkno;
	BlockNumber	first_pblkno;
} xl_umbra_range_remap_compact;

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
extern XLogRecPtr log_umbra_range_remap(RelFileLocator rlocator,
										ForkNumber forknum,
										uint16 count,
										const xl_umbra_range_remap_entry *entries);
extern XLogRecPtr log_umbra_range_remap_compact(RelFileLocator rlocator,
												ForkNumber forknum,
												BlockNumber first_lblkno,
												BlockNumber first_pblkno,
												uint16 count);
extern XLogRecPtr log_umbra_skip_wal_dense_map(RelFileLocator rlocator,
											   uint16 count,
											   const xl_umbra_skip_wal_dense_map_entry *entries);

extern void umbra_redo(XLogReaderState *record);
extern void umbra_desc(StringInfo buf, XLogReaderState *record);
extern const char *umbra_identify(uint8 info);

#endif							/* UMBRA_XLOG_H */
