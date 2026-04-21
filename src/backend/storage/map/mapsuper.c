/*-------------------------------------------------------------------------
 *
 * mapsuper.c
 *	  Umbra metadata superblock helpers.
 *
 * This file contains on-disk superblock encoding and direct metadata-file I/O
 * helpers.
 *
 * src/backend/storage/map/mapsuper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/mapsuper.h"
#include "storage/umbra.h"

static void MapSBlockReportCorrupt(SMgrRelation reln, const char *reason);

void
MapSuperblockRefreshCRC(MapSuperblock *super)
{
	pg_crc32c	crc;

	Assert(super != NULL);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &super->data, offsetof(MapSuperblockData, crc));
	FIN_CRC32C(crc);
	super->data.crc = crc;
}

bool
MapSuperblockCheckCRC(const MapSuperblock *super)
{
	pg_crc32c	crc;

	Assert(super != NULL);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &super->data, offsetof(MapSuperblockData, crc));
	FIN_CRC32C(crc);

	return crc == super->data.crc;
}

void
MapSuperblockInit(MapSuperblock *super, uint32 flags)
{
	Assert(super != NULL);

	MemSet(super, 0, sizeof(*super));

	super->data.magic = MAP_SUPERBLOCK_MAGIC;
	super->data.version = MAP_SUPERBLOCK_VERSION;
	super->data.blcksz = BLCKSZ;
	super->data.flags = flags;
	super->data.next_free_phys_block_fsm = InvalidBlockNumber;
	super->data.phys_capacity_fsm = InvalidBlockNumber;
	super->data.next_free_phys_block_vm = InvalidBlockNumber;
	super->data.phys_capacity_vm = InvalidBlockNumber;
	super->data.logical_nblocks_fsm = InvalidBlockNumber;
	super->data.logical_nblocks_vm = InvalidBlockNumber;
	super->data.last_updated_lsn = InvalidXLogRecPtr;
	super->data.crc = 0;
}

bool
MapSuperblockHasValidIdentity(const MapSuperblock *super)
{
	Assert(super != NULL);

	if (super->data.magic != MAP_SUPERBLOCK_MAGIC)
		return false;
	if (super->data.version != MAP_SUPERBLOCK_VERSION)
		return false;
	if (super->data.blcksz != BLCKSZ)
		return false;

	return true;
}

bool
MapSuperblockIsValid(const MapSuperblock *super)
{
	Assert(super != NULL);

	if (!MapSuperblockHasValidIdentity(super))
		return false;

	return MapSuperblockCheckCRC(super);
}

void
MapSuperblockSetFlags(MapSuperblock *super, uint32 flags)
{
	Assert(super != NULL);

	super->data.flags = flags;
}

uint32
MapSuperblockGetFlags(const MapSuperblock *super)
{
	Assert(super != NULL);

	return super->data.flags;
}

void
MapSuperblockSetLastUpdatedLSN(MapSuperblock *super, XLogRecPtr lsn)
{
	Assert(super != NULL);

	super->data.last_updated_lsn = lsn;
}

XLogRecPtr
MapSuperblockGetLastUpdatedLSN(const MapSuperblock *super)
{
	Assert(super != NULL);

	return super->data.last_updated_lsn;
}

BlockNumber
MapSuperblockGetNextFreePhysBlock(const MapSuperblock *super, ForkNumber forknum)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return super->data.next_free_phys_block_main;
		case FSM_FORKNUM:
			return super->data.next_free_phys_block_fsm;
		case VISIBILITYMAP_FORKNUM:
			return super->data.next_free_phys_block_vm;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}

	pg_unreachable();
}

void
MapSuperblockSetNextFreePhysBlock(MapSuperblock *super, ForkNumber forknum,
								  BlockNumber blkno)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			super->data.next_free_phys_block_main = blkno;
			break;
		case FSM_FORKNUM:
			super->data.next_free_phys_block_fsm = blkno;
			break;
		case VISIBILITYMAP_FORKNUM:
			super->data.next_free_phys_block_vm = blkno;
			break;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}
}

BlockNumber
MapSuperblockGetPhysCapacity(const MapSuperblock *super, ForkNumber forknum)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return super->data.phys_capacity_main;
		case FSM_FORKNUM:
			return super->data.phys_capacity_fsm;
		case VISIBILITYMAP_FORKNUM:
			return super->data.phys_capacity_vm;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}

	pg_unreachable();
}

void
MapSuperblockSetPhysCapacity(MapSuperblock *super, ForkNumber forknum,
							 BlockNumber blkno)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			super->data.phys_capacity_main = blkno;
			break;
		case FSM_FORKNUM:
			super->data.phys_capacity_fsm = blkno;
			break;
		case VISIBILITYMAP_FORKNUM:
			super->data.phys_capacity_vm = blkno;
			break;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}
}

BlockNumber
MapSuperblockGetLogicalNblocks(const MapSuperblock *super, ForkNumber forknum)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return super->data.logical_nblocks_main;
		case FSM_FORKNUM:
			return super->data.logical_nblocks_fsm;
		case VISIBILITYMAP_FORKNUM:
			return super->data.logical_nblocks_vm;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}

	pg_unreachable();
}

void
MapSuperblockSetLogicalNblocks(MapSuperblock *super, ForkNumber forknum,
							   BlockNumber nblocks)
{
	Assert(super != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			super->data.logical_nblocks_main = nblocks;
			break;
		case FSM_FORKNUM:
			super->data.logical_nblocks_fsm = nblocks;
			break;
		case VISIBILITYMAP_FORKNUM:
			super->data.logical_nblocks_vm = nblocks;
			break;
		default:
			elog(ERROR, "unsupported fork number for superblock: %d", forknum);
	}
}

void
MapSuperblockPackPage(const MapSuperblock *super, char page[BLCKSZ])
{
	Assert(super != NULL);
	Assert(page != NULL);

	MemSet(page, 0, BLCKSZ);
	memcpy(page, super->padding, MAP_SUPERBLOCK_SIZE);
}

void
MapSuperblockUnpackPage(MapSuperblock *super, const char page[BLCKSZ])
{
	Assert(super != NULL);
	Assert(page != NULL);

	memcpy(super->padding, page, MAP_SUPERBLOCK_SIZE);
}

bool
MapSBlockRead(SMgrRelation reln, MapSuperblock *super)
{
	char		page[BLCKSZ];

	Assert(reln != NULL);
	Assert(super != NULL);

	if (!UmMetadataExists(reln))
		return false;

	if (UmMetadataNblocks(reln) == 0)
		return false;

	UmMetadataRead(reln, MAP_BLOCK_SUPER, page);
	MapSuperblockUnpackPage(super, page);

	if (!MapSuperblockHasValidIdentity(super))
		MapSBlockReportCorrupt(reln, "invalid identity");
	if (!MapSuperblockCheckCRC(super))
		MapSBlockReportCorrupt(reln, "CRC mismatch");

	return true;
}

void
MapSBlockWrite(SMgrRelation reln, const MapSuperblock *super, bool skipFsync)
{
	MapSuperblock write_super;
	char		page[BLCKSZ];

	Assert(reln != NULL);
	Assert(super != NULL);

	write_super = *super;
	MapSuperblockRefreshCRC(&write_super);
	MapSuperblockPackPage(&write_super, page);

	if (!UmMetadataOpenOrCreate(reln, false, NULL))
		elog(ERROR, "could not open Umbra metadata file for superblock write");

	if (UmMetadataNblocks(reln) == 0)
		UmMetadataExtend(reln, MAP_BLOCK_SUPER, page, skipFsync);
	else
		UmMetadataWrite(reln, MAP_BLOCK_SUPER, page, skipFsync);
}

void
MapSBlockInitNew(SMgrRelation reln, uint32 flags, XLogRecPtr lsn, bool skipFsync)
{
	MapSuperblock super;

	MapSuperblockInit(&super, flags);
	MapSuperblockSetLastUpdatedLSN(&super, lsn);
	MapSBlockWrite(reln, &super, skipFsync);
}

static void
MapSBlockReportCorrupt(SMgrRelation reln, const char *reason)
{
	RelFileLocator rlocator = reln->smgr_rlocator.locator;

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("Umbra metadata superblock is corrupted for relation %u/%u/%u: %s",
					rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, reason)));
}
