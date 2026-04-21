/*-------------------------------------------------------------------------
 *
 * mapsuper.h
 *	  MAP superblock metadata helpers.
 *
 * The on-disk layout is a 512-byte sector:
 * - first 64 bytes: MapSuperblockData payload
 * - remaining 448 bytes: zero padding
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPSUPER_H
#define MAPSUPER_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "port/pg_crc32c.h"
#include "storage/block.h"

#define MAP_SUPERBLOCK_MAGIC		0x554D4252U	/* "UMBR" */
#define MAP_SUPERBLOCK_VERSION		1U
#define MAP_SUPERBLOCK_SIZE			512
#define MAP_SUPERBLOCK_PAYLOAD_SIZE 64

#define MAP_SUPERBLOCK_FLAG_SKIP_WAL_PENDING 0x00000001U

typedef struct pg_attribute_packed() MapSuperblockData
{
	/* identity/version */
	uint32		magic;
	uint32		version;
	uint32		blcksz;
	uint32		flags;

	/* physical allocator state */
	BlockNumber next_free_phys_block_main;
	BlockNumber phys_capacity_main;
	BlockNumber next_free_phys_block_fsm;
	BlockNumber phys_capacity_fsm;
	BlockNumber next_free_phys_block_vm;
	BlockNumber phys_capacity_vm;

	/* logical block count cache */
	BlockNumber logical_nblocks_main;
	BlockNumber logical_nblocks_fsm;
	BlockNumber logical_nblocks_vm;

	/* crash-safety metadata */
	XLogRecPtr	last_updated_lsn;
	pg_crc32c	crc;
} MapSuperblockData;

typedef union MapSuperblock
{
	MapSuperblockData data;
	char		padding[MAP_SUPERBLOCK_SIZE];
} MapSuperblock;

typedef char MapSuperblockDataSizeCheck
[(sizeof(MapSuperblockData) == MAP_SUPERBLOCK_PAYLOAD_SIZE) ? 1 : -1];
typedef char MapSuperblockDataCRCOffsetCheck
[(offsetof(MapSuperblockData, crc) == 60) ? 1 : -1];
typedef char MapSuperblockSizeCheck
[(sizeof(MapSuperblock) == MAP_SUPERBLOCK_SIZE) ? 1 : -1];

extern void MapSuperblockInit(MapSuperblock *super, uint32 flags);
extern bool MapSuperblockHasValidIdentity(const MapSuperblock *super);
extern bool MapSuperblockIsValid(const MapSuperblock *super);
extern bool MapSuperblockCheckCRC(const MapSuperblock *super);
extern void MapSuperblockRefreshCRC(MapSuperblock *super);

extern void MapSuperblockSetFlags(MapSuperblock *super, uint32 flags);
extern uint32 MapSuperblockGetFlags(const MapSuperblock *super);

extern void MapSuperblockSetLastUpdatedLSN(MapSuperblock *super, XLogRecPtr lsn);
extern XLogRecPtr MapSuperblockGetLastUpdatedLSN(const MapSuperblock *super);

extern BlockNumber MapSuperblockGetNextFreePhysBlock(const MapSuperblock *super,
													 ForkNumber forknum);
extern void MapSuperblockSetNextFreePhysBlock(MapSuperblock *super,
											  ForkNumber forknum,
											  BlockNumber blkno);

extern BlockNumber MapSuperblockGetPhysCapacity(const MapSuperblock *super,
												ForkNumber forknum);
extern void MapSuperblockSetPhysCapacity(MapSuperblock *super, ForkNumber forknum,
										 BlockNumber blkno);

extern BlockNumber MapSuperblockGetLogicalNblocks(const MapSuperblock *super,
												  ForkNumber forknum);
extern void MapSuperblockSetLogicalNblocks(MapSuperblock *super, ForkNumber forknum,
										   BlockNumber nblocks);

/* 512-byte sector I/O helpers */
extern void MapSuperblockPackSector(const MapSuperblock *super,
									char sector[MAP_SUPERBLOCK_SIZE]);
extern void MapSuperblockUnpackSector(MapSuperblock *super,
									  const char sector[MAP_SUPERBLOCK_SIZE]);

#endif							/* MAPSUPER_H */
