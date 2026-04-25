/*-------------------------------------------------------------------------
 *
 * mapsuper.h
 *	  Umbra metadata superblock helpers.
 *
 * The superblock is stored in metadata block 0. Its first 512 bytes contain a
 * versioned payload plus CRC, and the remainder of the block is reserved.
 *
 * src/include/storage/mapsuper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPSUPER_H
#define MAPSUPER_H

#include "access/xlogdefs.h"
#include "port/pg_crc32c.h"
#include "storage/block.h"
#include "storage/smgr.h"

#define MAP_SUPERBLOCK_MAGIC		0x554D4252U	/* "UMBR" */
#define MAP_SUPERBLOCK_VERSION		1U
#define MAP_SUPERBLOCK_SIZE			512
#define MAP_SUPERBLOCK_PAYLOAD_SIZE 64

#define MAP_SUPERBLOCK_FLAG_SKIP_WAL_PENDING 0x00000001U

typedef struct pg_attribute_packed() MapSuperblockData
{
	uint32		magic;
	uint32		version;
	uint32		blcksz;
	uint32		flags;

	BlockNumber next_free_phys_block_main;
	BlockNumber phys_capacity_main;
	BlockNumber next_free_phys_block_fsm;
	BlockNumber phys_capacity_fsm;
	BlockNumber next_free_phys_block_vm;
	BlockNumber phys_capacity_vm;

	BlockNumber logical_nblocks_main;
	BlockNumber logical_nblocks_fsm;
	BlockNumber logical_nblocks_vm;

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

extern void MapSuperblockPackPage(const MapSuperblock *super, char page[BLCKSZ]);
extern void MapSuperblockUnpackPage(MapSuperblock *super, const char page[BLCKSZ]);

extern bool MapSBlockRead(SMgrRelation reln, MapSuperblock *super);
extern void MapSBlockWrite(SMgrRelation reln, const MapSuperblock *super,
						   bool skipFsync);
extern void MapSBlockInitNew(SMgrRelation reln, uint32 flags, XLogRecPtr lsn,
							 bool skipFsync);

#endif							/* MAPSUPER_H */
