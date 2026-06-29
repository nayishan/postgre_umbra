/*-------------------------------------------------------------------------
 *
 * mapsuper.c
 *	  MAP superblock metadata helpers.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "storage/map.h"
#include "storage/mapsuper.h"
#include "storage/mapsuper_internal.h"
#include "storage/shmem.h"

#define MAP_SUPER_NPARTITIONS		128
#define MAP_SUPER_NPARTITION_BITS	7
#define MAPSUPER_INDEX_EMPTY		(-1)
#define MAPSUPER_INDEX_DELETED		(-2)
#define MAPSUPER_FREENEXT_END		(-1)
#define MAPSUPER_FREENEXT_NOT_IN_LIST (-2)

#if MAP_SUPER_NPARTITIONS != (1 << MAP_SUPER_NPARTITION_BITS)
#error "MAP_SUPER_NPARTITIONS must match MAP_SUPER_NPARTITION_BITS"
#endif

typedef struct MapSuperIndexSlot
{
	int			slot_id;
} MapSuperIndexSlot;

typedef struct MapSuperCtl
{
	int			free_head;
	slock_t		free_list_lock;
} MapSuperCtl;

typedef enum MapSBlockReadStatus
{
	MAP_SBLOCK_READ_OK,
	MAP_SBLOCK_READ_MISSING,
	MAP_SBLOCK_READ_CORRUPT
} MapSBlockReadStatus;

MapSuperEntry *MapSuperEntries = NULL;
int			MapSuperCapacity = 0;

static MapSuperIndexSlot *MapSuperIndex = NULL;
static MapSuperCtl *MapSuperCtlData = NULL;
static LWLockPadded *MapSuperPartitionLocks = NULL;
static int	MapSuperIndexCapacityPerPartition = 0;

static void MapSuperTableRefreshDerivedState(void);
static MapSBlockReadStatus MapSuperLoadFromDisk(UmbraFileContext *map_ctx,
												RelFileLocator rnode,
												MapSuperblock *super);
static int	MapSuperIndexCapacityForPartition(int capacity);
static uint32 MapSuperHashCode(RelFileLocator rnode);
static int	MapSuperPartitionForHash(uint32 hashcode);
static LWLock *MapSuperPartitionLock(uint32 hashcode);
static int	MapSuperLookupSlotLocked(RelFileLocator rnode, uint32 hashcode,
									 int partition, int *insert_bucket);
static bool MapForkUsesAbsentSentinel(ForkNumber forknum);
static uint32 MapSuperExtendingFlag(ForkNumber forknum);
static BlockNumber MapSuperGetExtendingTarget(const MapSuperEntry *entry,
											  ForkNumber forknum);
static void MapSuperSetExtendingTarget(MapSuperEntry *entry,
									   ForkNumber forknum,
									   BlockNumber nblocks);
static bool MapSuperPrepareEntryForUpdate(UmbraFileContext *map_ctx,
										  RelFileLocator rnode,
										  XLogRecPtr map_lsn,
										  const char *missing_errmsg,
										  MapSuperEntry **entry_p);
static void MapSBlockUpdateLogicalNblocks(UmbraFileContext *map_ctx,
										  RelFileLocator rnode,
										  ForkNumber forknum,
										  BlockNumber nblocks,
										  XLogRecPtr map_lsn,
										  bool bump_only);
static void MapSBlockSetPendingFlag(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									bool pending,
									XLogRecPtr map_lsn);
static bool MapSBlockEnsurePhysicalNblocksInternal(UmbraFileContext *map_ctx,
												   RelFileLocator rnode,
												   ForkNumber forknum,
												   BlockNumber nblocks,
												   bool skipFsync,
												   bool zero_fill);
void MapSBlockBumpPhysicalState(UmbraFileContext *map_ctx,
								RelFileLocator rnode,
								ForkNumber forknum,
								BlockNumber nblocks,
								bool bump_next_free,
								bool bump_capacity,
								XLogRecPtr map_lsn);

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
MapSuperblockPackSector(const MapSuperblock *super, char sector[MAP_SUPERBLOCK_SIZE])
{
	Assert(super != NULL);
	Assert(sector != NULL);

	memcpy(sector, super->padding, MAP_SUPERBLOCK_SIZE);
}

void
MapSuperblockUnpackSector(MapSuperblock *super,
						  const char sector[MAP_SUPERBLOCK_SIZE])
{
	Assert(super != NULL);
	Assert(sector != NULL);

	memcpy(super->padding, sector, MAP_SUPERBLOCK_SIZE);
}

void
MapSBlockReportCorrupt(RelFileLocator rnode, const char *reason)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("map superblock is corrupted for relation %u/%u/%u: %s",
					rnode.spcOid, rnode.dbOid, rnode.relNumber, reason)));
}

static MapSBlockReadStatus
MapSuperLoadFromDisk(UmbraFileContext *map_ctx, RelFileLocator rnode,
					 MapSuperblock *super)
{
	char		sector[MAP_SUPERBLOCK_SIZE];

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return MAP_SBLOCK_READ_MISSING;

	if (umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM) <= MAP_BLOCK_SUPER)
		return MAP_SBLOCK_READ_CORRUPT;

	umfile_ctx_read(map_ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
					sector, MAP_SUPERBLOCK_SIZE);
	MapSuperblockUnpackSector(super, sector);

	if (!MapSuperblockHasValidIdentity(super) ||
		!MapSuperblockCheckCRC(super))
		return MAP_SBLOCK_READ_CORRUPT;

	return MAP_SBLOCK_READ_OK;
}

static uint32
MapSuperHashCode(RelFileLocator rnode)
{
	return DatumGetUInt32(hash_any((const unsigned char *) &rnode,
								   sizeof(RelFileLocator)));
}

static int
MapSuperIndexCapacityForPartition(int capacity)
{
	int			index_capacity = 1;
	long		total_target = (long) capacity * 2L;
	long		per_partition_target;

	per_partition_target =
		(total_target + MAP_SUPER_NPARTITIONS - 1) / MAP_SUPER_NPARTITIONS;
	while ((long) index_capacity < per_partition_target)
		index_capacity <<= 1;

	return index_capacity;
}

static int
MapSuperPartitionForHash(uint32 hashcode)
{
	return hashcode & (MAP_SUPER_NPARTITIONS - 1);
}

static LWLock *
MapSuperPartitionLock(uint32 hashcode)
{
	return &MapSuperPartitionLocks[MapSuperPartitionForHash(hashcode)].lock;
}

static int
MapSuperLookupSlotLocked(RelFileLocator rnode, uint32 hashcode, int partition,
						 int *insert_bucket)
{
	int			mask = MapSuperIndexCapacityPerPartition - 1;
	int			base = partition * MapSuperIndexCapacityPerPartition;
	int			bucket = (hashcode >> MAP_SUPER_NPARTITION_BITS) & mask;
	int			first_deleted = -1;
	int			probes;
	LWLock	   *partition_lock = MapSuperPartitionLock(hashcode);

	Assert(LWLockHeldByMe(partition_lock));

	for (probes = 0; probes < MapSuperIndexCapacityPerPartition; probes++)
	{
		int			slot_id = MapSuperIndex[base + bucket].slot_id;

		if (slot_id == MAPSUPER_INDEX_EMPTY)
		{
			if (insert_bucket != NULL)
				*insert_bucket = (first_deleted >= 0) ?
					(base + first_deleted) : (base + bucket);
			return -1;
		}

		if (slot_id == MAPSUPER_INDEX_DELETED)
		{
			if (first_deleted < 0)
				first_deleted = bucket;
		}
		else
		{
			MapSuperEntry *entry = MapSuperEntryBySlot(slot_id);

			if (entry->in_use && RelFileLocatorEquals(entry->key.rnode, rnode))
			{
				if (insert_bucket != NULL)
					*insert_bucket = base + bucket;
				return slot_id;
			}
		}

		bucket = (bucket + 1) & mask;
	}

	if (insert_bucket != NULL)
		*insert_bucket = (first_deleted >= 0) ? (base + first_deleted) : -1;

	return -1;
}

bool
MapSuperFindEntryLocked(RelFileLocator rnode, LWLockMode mode,
						MapSuperEntry **entry)
{
	uint32		hashcode;
	int			partition;
	int			slot_id;
	LWLock	   *partition_lock;

	hashcode = MapSuperHashCode(rnode);
	partition = MapSuperPartitionForHash(hashcode);
	partition_lock = &MapSuperPartitionLocks[partition].lock;

	LWLockAcquire(partition_lock, LW_SHARED);
	slot_id = MapSuperLookupSlotLocked(rnode, hashcode, partition, NULL);
	if (slot_id >= 0)
	{
		*entry = MapSuperEntryBySlot(slot_id);
		LWLockAcquire(&(*entry)->lock, mode);
		LWLockRelease(partition_lock);
		return true;
	}

	LWLockRelease(partition_lock);
	*entry = NULL;
	return false;
}

bool
MapSuperFindEntryTryLocked(RelFileLocator rnode, LWLockMode mode,
						   MapSuperEntry **entry)
{
	uint32		hashcode;
	int			partition;
	int			slot_id;
	LWLock	   *partition_lock;

	hashcode = MapSuperHashCode(rnode);
	partition = MapSuperPartitionForHash(hashcode);
	partition_lock = &MapSuperPartitionLocks[partition].lock;

	LWLockAcquire(partition_lock, LW_SHARED);
	slot_id = MapSuperLookupSlotLocked(rnode, hashcode, partition, NULL);
	if (slot_id >= 0)
	{
		*entry = MapSuperEntryBySlot(slot_id);
		if (!LWLockConditionalAcquire(&(*entry)->lock, mode))
		{
			LWLockRelease(partition_lock);
			*entry = NULL;
			return false;
		}
		LWLockRelease(partition_lock);
		return true;
	}

	LWLockRelease(partition_lock);
	*entry = NULL;
	return false;
}

MapSuperEntry *
MapSuperEnsureEntryLocked(RelFileLocator rnode)
{
	MapSuperEntry *entry;
	uint32		hashcode;
	int			partition;
	int			slot_id;
	int			insert_bucket = -1;
	LWLock	   *partition_lock;

	hashcode = MapSuperHashCode(rnode);
	partition = MapSuperPartitionForHash(hashcode);
	partition_lock = &MapSuperPartitionLocks[partition].lock;

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	slot_id = MapSuperLookupSlotLocked(rnode, hashcode, partition, &insert_bucket);
	if (slot_id >= 0)
	{
		entry = MapSuperEntryBySlot(slot_id);
		LWLockAcquire(&entry->lock, LW_EXCLUSIVE);
		LWLockRelease(partition_lock);
		return entry;
	}

	if (insert_bucket < 0)
	{
		LWLockRelease(partition_lock);
		ereport(ERROR,
				(errmsg("map superblock index table is full"),
				 errhint("Increase map_superblocks and restart the server.")));
	}

	SpinLockAcquire(&MapSuperCtlData->free_list_lock);
	slot_id = MapSuperCtlData->free_head;
	if (slot_id == MAPSUPER_FREENEXT_END)
	{
		SpinLockRelease(&MapSuperCtlData->free_list_lock);
		LWLockRelease(partition_lock);
		ereport(ERROR,
				(errmsg("map superblock slot table is full"),
				 errhint("Increase map_superblocks and restart the server.")));
	}

	entry = MapSuperEntryBySlot(slot_id);
	MapSuperCtlData->free_head = entry->next_free;
	SpinLockRelease(&MapSuperCtlData->free_list_lock);

	entry->next_free = MAPSUPER_FREENEXT_NOT_IN_LIST;
	entry->in_use = true;
	entry->key.rnode = rnode;
	MemSet(&entry->super, 0, sizeof(entry->super));
	entry->page_lsn = InvalidXLogRecPtr;
	entry->flags = 0;
	entry->runtime_flags = 0;
	entry->reserved_next_free_main = 0;
	entry->reserved_next_free_fsm = 0;
	entry->reserved_next_free_vm = 0;
	entry->extending_target_main = InvalidBlockNumber;
	entry->extending_target_fsm = InvalidBlockNumber;
	entry->extending_target_vm = InvalidBlockNumber;
	entry->prealloc_count_main = 0;
	entry->prealloc_count_fsm = 0;
	entry->prealloc_count_vm = 0;
	MapSuperIndex[insert_bucket].slot_id = slot_id;

	LWLockAcquire(&entry->lock, LW_EXCLUSIVE);
	LWLockRelease(partition_lock);

	return entry;
}

void
MapSuperDeleteEntry(RelFileLocator rnode)
{
	MapSuperEntry *entry = NULL;
	uint32		hashcode;
	int			partition;
	int			slot_id;
	int			bucket = -1;
	LWLock	   *partition_lock;

	hashcode = MapSuperHashCode(rnode);
	partition = MapSuperPartitionForHash(hashcode);
	partition_lock = &MapSuperPartitionLocks[partition].lock;

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	slot_id = MapSuperLookupSlotLocked(rnode, hashcode, partition, &bucket);
	if (slot_id >= 0)
	{
		entry = MapSuperEntryBySlot(slot_id);
		LWLockAcquire(&entry->lock, LW_EXCLUSIVE);
		entry->flags = 0;
		entry->runtime_flags = 0;
		entry->page_lsn = InvalidXLogRecPtr;
		entry->reserved_next_free_main = 0;
		entry->reserved_next_free_fsm = 0;
		entry->reserved_next_free_vm = 0;
		entry->extending_target_main = InvalidBlockNumber;
		entry->extending_target_fsm = InvalidBlockNumber;
		entry->extending_target_vm = InvalidBlockNumber;
		entry->prealloc_count_main = 0;
		entry->prealloc_count_fsm = 0;
		entry->prealloc_count_vm = 0;
		entry->in_use = false;
		SpinLockAcquire(&MapSuperCtlData->free_list_lock);
		entry->next_free = MapSuperCtlData->free_head;
		MapSuperCtlData->free_head = slot_id;
		SpinLockRelease(&MapSuperCtlData->free_list_lock);
		LWLockRelease(&entry->lock);
		MapSuperIndex[bucket].slot_id = MAPSUPER_INDEX_DELETED;
	}
	LWLockRelease(partition_lock);
}

static MapSBlockReadStatus
MapSBlockRead(UmbraFileContext *map_ctx, RelFileLocator rnode, MapSuperblock *super)
{
	MapSuperEntry *entry;
	MapSBlockReadStatus status = MAP_SBLOCK_READ_OK;
	MapSuperblock	disk_super;

	Assert(map_ctx != NULL);
	Assert(super != NULL);

	if (!MapSuperFindEntryLocked(rnode, LW_SHARED, &entry))
	{
		status = MapSuperLoadFromDisk(map_ctx, rnode, &disk_super);
		if (status == MAP_SBLOCK_READ_MISSING)
			return MAP_SBLOCK_READ_MISSING;

		entry = MapSuperEnsureEntryLocked(rnode);
		if ((entry->flags & MAPSUPER_FLAG_VALID) == 0)
		{
			if (status == MAP_SBLOCK_READ_OK)
			{
				entry->super = disk_super;
				entry->page_lsn = MapSuperblockGetLastUpdatedLSN(&disk_super);
				entry->flags = MAPSUPER_FLAG_VALID;
				MapSuperResetReservedNextFrees(entry);
				Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					MAIN_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
				Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					FSM_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
				Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					VISIBILITYMAP_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
			}
			else
			{
				MapSuperblockInit(&entry->super, 0);
				entry->page_lsn = InvalidXLogRecPtr;
				entry->flags = MAPSUPER_FLAG_VALID | MAPSUPER_FLAG_CORRUPT;
				MapSuperResetReservedNextFrees(entry);
				Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					MAIN_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
				Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					FSM_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
				Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					VISIBILITYMAP_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
			}
		}
		else if (entry->flags & MAPSUPER_FLAG_CORRUPT)
			status = MAP_SBLOCK_READ_CORRUPT;
		else
			status = MAP_SBLOCK_READ_OK;
	}
	else if ((entry->flags & MAPSUPER_FLAG_VALID) == 0)
	{
		LWLockRelease(&entry->lock);
		status = MapSuperLoadFromDisk(map_ctx, rnode, &disk_super);
		if (status == MAP_SBLOCK_READ_MISSING)
			return MAP_SBLOCK_READ_MISSING;

		entry = MapSuperEnsureEntryLocked(rnode);
		if ((entry->flags & MAPSUPER_FLAG_VALID) == 0)
		{
			if (status == MAP_SBLOCK_READ_OK)
			{
				entry->super = disk_super;
				entry->page_lsn = MapSuperblockGetLastUpdatedLSN(&disk_super);
				entry->flags = MAPSUPER_FLAG_VALID;
				MapSuperResetReservedNextFrees(entry);
				Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					MAIN_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
				Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					FSM_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
				Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					VISIBILITYMAP_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
			}
			else
			{
				MapSuperblockInit(&entry->super, 0);
				entry->page_lsn = InvalidXLogRecPtr;
				entry->flags = MAPSUPER_FLAG_VALID | MAPSUPER_FLAG_CORRUPT;
				MapSuperResetReservedNextFrees(entry);
				Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					MAIN_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
				Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					FSM_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
				Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
												  MapSuperblockGetNextFreePhysBlock(&entry->super,
																					VISIBILITYMAP_FORKNUM)) <=
					   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
			}
		}
		else if (entry->flags & MAPSUPER_FLAG_CORRUPT)
			status = MAP_SBLOCK_READ_CORRUPT;
		else
			status = MAP_SBLOCK_READ_OK;
	}
	else
	{
		/*
		 * Once a superblock is loaded into a valid shared entry, hot reads
		 * should consume that runtime state directly. Disk identity/CRC
		 * validation belongs to the slow path that populates shared state.
		 */
		Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
										  MapSuperblockGetNextFreePhysBlock(&entry->super,
																			MAIN_FORKNUM)) <=
			   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
		Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
										  MapSuperblockGetNextFreePhysBlock(&entry->super,
																			FSM_FORKNUM)) <=
			   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
		Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
										  MapSuperblockGetNextFreePhysBlock(&entry->super,
																			VISIBILITYMAP_FORKNUM)) <=
			   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
		*super = entry->super;
		status = (entry->flags & MAPSUPER_FLAG_CORRUPT) ?
			MAP_SBLOCK_READ_CORRUPT : MAP_SBLOCK_READ_OK;
		LWLockRelease(&entry->lock);
		return status;
	}

	switch (status)
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
			LWLockRelease(&entry->lock);
			return MAP_SBLOCK_READ_MISSING;
		case MAP_SBLOCK_READ_CORRUPT:
			LWLockRelease(&entry->lock);
			return MAP_SBLOCK_READ_CORRUPT;
	}

	*super = entry->super;
	LWLockRelease(&entry->lock);
	return MAP_SBLOCK_READ_OK;
}

bool
MapForkHasMappedState(ForkNumber forknum)
{
	switch (forknum)
	{
		case MAIN_FORKNUM:
		case FSM_FORKNUM:
		case VISIBILITYMAP_FORKNUM:
			return true;
		default:
			return false;
	}
}

static bool
MapForkUsesAbsentSentinel(ForkNumber forknum)
{
	switch (forknum)
	{
		case FSM_FORKNUM:
		case VISIBILITYMAP_FORKNUM:
			return true;
		default:
			return false;
	}
}

BlockNumber
MapNormalizeForkBlockCount(ForkNumber forknum, BlockNumber raw)
{
	if (MapForkUsesAbsentSentinel(forknum) &&
		raw == InvalidBlockNumber)
		return 0;

	return raw;
}

bool
MapSuperForkExists(const MapSuperblock *super, ForkNumber forknum)
{
	if (!MapForkHasMappedState(forknum))
		return false;

	if (!MapForkUsesAbsentSentinel(forknum))
		return true;

	return MapSuperblockGetLogicalNblocks(super, forknum) != InvalidBlockNumber;
}

uint32
MapSuperPreallocFlag(ForkNumber forknum)
{
	switch (forknum)
	{
		case MAIN_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_PREALLOC_MAIN;
		case FSM_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_PREALLOC_FSM;
		case VISIBILITYMAP_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_PREALLOC_VM;
		default:
			return 0;
	}
}

static uint32
MapSuperExtendingFlag(ForkNumber forknum)
{
	switch (forknum)
	{
		case MAIN_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_EXTENDING_MAIN;
		case FSM_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_EXTENDING_FSM;
		case VISIBILITYMAP_FORKNUM:
			return MAPSUPER_RUNTIME_FLAG_EXTENDING_VM;
		default:
			return 0;
	}
}

static BlockNumber
MapSuperGetExtendingTarget(const MapSuperEntry *entry, ForkNumber forknum)
{
	Assert(entry != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return entry->extending_target_main;
		case FSM_FORKNUM:
			return entry->extending_target_fsm;
		case VISIBILITYMAP_FORKNUM:
			return entry->extending_target_vm;
		default:
			return InvalidBlockNumber;
	}
}

static void
MapSuperSetExtendingTarget(MapSuperEntry *entry, ForkNumber forknum,
						   BlockNumber nblocks)
{
	Assert(entry != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			entry->extending_target_main = nblocks;
			break;
		case FSM_FORKNUM:
			entry->extending_target_fsm = nblocks;
			break;
		case VISIBILITYMAP_FORKNUM:
			entry->extending_target_vm = nblocks;
			break;
		default:
			elog(ERROR, "unsupported fork number for extend target: %d", forknum);
	}
}

static bool
MapSuperPrepareEntryForUpdate(UmbraFileContext *map_ctx, RelFileLocator rnode,
							  XLogRecPtr map_lsn, const char *missing_errmsg,
							  MapSuperEntry **entry_p)
{
	MapSuperEntry *entry;
	uint32			flags;

	Assert(map_ctx != NULL);
	Assert(entry_p != NULL);

	if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
	{
		MapSuperblock	disk_super;
		MapSBlockReadStatus status;

		status = MapSuperLoadFromDisk(map_ctx, rnode, &disk_super);
		if (status == MAP_SBLOCK_READ_MISSING)
		{
#ifdef USE_UMBRA
			if (InRecovery)
			{
				XLogLogMissingRelationMetadata(rnode);
				return false;
			}
#endif
			elog(ERROR, "%s", missing_errmsg);
		}

		entry = MapSuperEnsureEntryLocked(rnode);
		if ((entry->flags & MAPSUPER_FLAG_VALID) == 0)
		{
			if (status == MAP_SBLOCK_READ_OK)
			{
				entry->super = disk_super;
				entry->page_lsn = MapSuperblockGetLastUpdatedLSN(&disk_super);
				entry->flags = MAPSUPER_FLAG_VALID;
				MapSuperResetReservedNextFrees(entry);
			}
			else
			{
				MapSuperblockInit(&entry->super, 0);
				entry->page_lsn = InvalidXLogRecPtr;
				entry->flags = MAPSUPER_FLAG_VALID | MAPSUPER_FLAG_CORRUPT;
				MapSuperResetReservedNextFrees(entry);
			}
		}
	}

	flags = entry->flags;

	if ((flags & MAPSUPER_FLAG_CORRUPT) ||
		!MapSuperblockHasValidIdentity(&entry->super) ||
		((flags & MAPSUPER_FLAG_DIRTY) == 0 &&
		 !MapSuperblockCheckCRC(&entry->super)))
	{
		if (!InRecovery || map_lsn == InvalidXLogRecPtr)
			MapSBlockReportCorrupt(rnode, "invalid identity or CRC");

		/*
		 * Update paths rebuild superblock state from WAL-backed metadata.
		 * Never continue from an untrusted superblock image.
		 */
		MapSuperblockInit(&entry->super, 0);
		entry->flags = MAPSUPER_FLAG_VALID;
		MapSuperResetReservedNextFrees(entry);
	}

	Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		MAIN_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
	Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		FSM_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
	Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		VISIBILITYMAP_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
	*entry_p = entry;
	return true;
}

static void
MapSBlockUpdateLogicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							  ForkNumber forknum, BlockNumber nblocks,
							  XLogRecPtr map_lsn, bool bump_only)
{
	MapSuperEntry *entry;
	BlockNumber		current;

	if (!MapForkHasMappedState(forknum))
		return;

	if (!MapSuperPrepareEntryForUpdate(map_ctx, rnode, map_lsn,
									   "MAP fork is missing while updating superblock",
									   &entry))
		return;

	current = MapSuperblockGetLogicalNblocks(&entry->super, forknum);
	current = MapNormalizeForkBlockCount(forknum, current);
	if (!bump_only || current < nblocks)
		MapSuperblockSetLogicalNblocks(&entry->super, forknum, nblocks);

	if (!bump_only || current < nblocks)
	{
		if (map_lsn == InvalidXLogRecPtr)
		{
			if (InRecovery)
				map_lsn = GetXLogReplayRecPtr(NULL);
			else
				map_lsn = GetXLogWriteRecPtr();
		}
		MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
		entry->page_lsn = map_lsn;
		entry->flags |= MAPSUPER_FLAG_DIRTY;
	}

	Assert(MapNormalizeForkBlockCount(forknum,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		forknum)) <=
		   MapSuperGetReservedNextFree(entry, forknum));
	LWLockRelease(&entry->lock);
}

static void
MapSBlockSetPendingFlag(UmbraFileContext *map_ctx, RelFileLocator rnode,
						bool pending, XLogRecPtr map_lsn)
{
	MapSuperEntry *entry;
	uint32		super_flags;

	if (!MapSuperPrepareEntryForUpdate(map_ctx, rnode, map_lsn,
									   "MAP fork is missing while updating superblock state",
									   &entry))
		return;

	super_flags = MapSuperblockGetFlags(&entry->super);
	if (pending)
		super_flags |= MAP_SUPERBLOCK_FLAG_SKIP_WAL_PENDING;
	else
		super_flags &= ~MAP_SUPERBLOCK_FLAG_SKIP_WAL_PENDING;

	if (super_flags != MapSuperblockGetFlags(&entry->super))
	{
		if (map_lsn == InvalidXLogRecPtr)
		{
			if (InRecovery)
				map_lsn = GetXLogReplayRecPtr(NULL);
			else
				map_lsn = GetXLogWriteRecPtr();
		}

		MapSuperblockSetFlags(&entry->super, super_flags);
		MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
		entry->page_lsn = map_lsn;
		entry->flags |= MAPSUPER_FLAG_DIRTY;
	}

	LWLockRelease(&entry->lock);
}

void
MapSBlockBumpPhysicalState(UmbraFileContext *map_ctx, RelFileLocator rnode,
						   ForkNumber forknum, BlockNumber nblocks,
						   bool bump_next_free, bool bump_capacity,
						   XLogRecPtr map_lsn)
{
	MapSuperEntry *entry;
	BlockNumber		current_next;
	BlockNumber		current_capacity;
	bool			changed = false;

	if (!MapForkHasMappedState(forknum))
		return;

	if (!MapSuperPrepareEntryForUpdate(map_ctx, rnode, map_lsn,
									   "MAP fork is missing while updating superblock",
									   &entry))
		return;

	current_next = MapSuperblockGetNextFreePhysBlock(&entry->super, forknum);
	current_capacity = MapSuperblockGetPhysCapacity(&entry->super, forknum);
	current_next = MapNormalizeForkBlockCount(forknum, current_next);
	current_capacity = MapNormalizeForkBlockCount(forknum, current_capacity);
	Assert(MapNormalizeForkBlockCount(forknum,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		forknum)) <=
		   MapSuperGetReservedNextFree(entry, forknum));

	if (bump_next_free && current_next < nblocks)
	{
		MapSuperblockSetNextFreePhysBlock(&entry->super, forknum, nblocks);
		MapSuperMaybeBumpReservedNextFree(entry, forknum, nblocks);
		changed = true;
	}
	if (bump_capacity && current_capacity < nblocks)
	{
		MapSuperblockSetPhysCapacity(&entry->super, forknum, nblocks);
		changed = true;
	}

	if (changed)
	{
		if (map_lsn == InvalidXLogRecPtr)
		{
			if (InRecovery)
				map_lsn = GetXLogReplayRecPtr(NULL);
			else
				map_lsn = GetXLogWriteRecPtr();
		}
		MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
		entry->page_lsn = map_lsn;
		entry->flags |= MAPSUPER_FLAG_DIRTY;
	}

	Assert(MapNormalizeForkBlockCount(forknum,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		forknum)) <=
		   MapSuperGetReservedNextFree(entry, forknum));
	LWLockRelease(&entry->lock);
}

bool
MapSBlockEnsurePhysicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							   ForkNumber forknum, BlockNumber nblocks,
							   bool skipFsync)
{
	return MapSBlockEnsurePhysicalNblocksInternal(map_ctx, rnode, forknum,
												  nblocks, skipFsync, false);
}

bool
MapSBlockEnsurePhysicalNblocksZeroFill(UmbraFileContext *map_ctx,
									   RelFileLocator rnode,
									   ForkNumber forknum,
									   BlockNumber nblocks,
									   bool skipFsync)
{
	return MapSBlockEnsurePhysicalNblocksInternal(map_ctx, rnode, forknum,
												  nblocks, skipFsync, true);
}

static bool
MapSBlockEnsurePhysicalNblocksInternal(UmbraFileContext *map_ctx,
									   RelFileLocator rnode,
									   ForkNumber forknum,
									   BlockNumber nblocks,
									   bool skipFsync,
									   bool zero_fill)
{
	MapSuperEntry *entry;
	uint32		extend_flag;
	BlockNumber	current;
	BlockNumber	desired;
	bool		success = false;

	if (!MapForkHasMappedState(forknum))
		return false;

	if (nblocks == 0)
		return true;

	if (!MapSBlockEnsureLoaded(map_ctx, rnode))
		return false;

	extend_flag = MapSuperExtendingFlag(forknum);
	Assert(extend_flag != 0);

retry:
	if (!MapSuperPrepareEntryForUpdate(map_ctx, rnode, InvalidXLogRecPtr,
									   "MAP fork is missing while materializing physical blocks",
									   &entry))
		return false;

	current = MapSuperblockGetPhysCapacity(&entry->super, forknum);
	current = MapNormalizeForkBlockCount(forknum, current);
	if (current >= nblocks)
	{
		LWLockRelease(&entry->lock);
		return true;
	}

	if ((entry->runtime_flags & extend_flag) != 0)
	{
		if (MapSuperGetExtendingTarget(entry, forknum) < nblocks)
			MapSuperSetExtendingTarget(entry, forknum, nblocks);
		LWLockRelease(&entry->lock);
		pg_usleep(1000L);

		CHECK_FOR_INTERRUPTS();
		goto retry;
	}

	entry->runtime_flags |= extend_flag;
	MapSuperSetExtendingTarget(entry, forknum, nblocks);
	LWLockRelease(&entry->lock);

	PG_TRY();
	{
		desired = nblocks;

		for (;;)
		{
			if (zero_fill)
			{
				current = MapNormalizeForkBlockCount(forknum, current);
				if (current < desired)
				{
					BlockNumber zero_start = current;

					while (zero_start < desired)
					{
						int			zero_blocks;

						zero_blocks = (int) Min(desired - zero_start,
												 (BlockNumber) INT_MAX);
						umfile_zeroextend(map_ctx, forknum, zero_start,
										  zero_blocks, skipFsync);
						zero_start += (BlockNumber) zero_blocks;
					}
				}
			}
			else if (!umfile_ctx_preallocate_blocks(map_ctx, forknum, desired,
												   skipFsync) &&
					 !umfile_ctx_block_exists(map_ctx, forknum, desired - 1))
			{
				/*
				 * Fall back to making EOF cover the published physical capacity
				 * when the platform cannot preallocate the range. Sparse holes
				 * read back as zeroes, so writing the final block is enough to
				 * avoid later EOF/short reads without forcing every intervening
				 * page through the foreground extension path.
				 */
				umfile_zeroextend(map_ctx, forknum, desired - 1,
								  1, skipFsync);
			}

			if (!MapSuperPrepareEntryForUpdate(map_ctx, rnode, InvalidXLogRecPtr,
											   "MAP fork is missing while materializing physical blocks",
											   &entry))
				elog(ERROR,
					 "MAP fork disappeared while materializing relation %u/%u/%u fork %d",
					 rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum);

			current = MapSuperblockGetPhysCapacity(&entry->super, forknum);
			current = MapNormalizeForkBlockCount(forknum, current);
			if (current < desired)
			{
				XLogRecPtr	map_lsn;

				if (InRecovery)
					map_lsn = GetXLogReplayRecPtr(NULL);
				else
					map_lsn = GetXLogWriteRecPtr();

				MapSuperblockSetPhysCapacity(&entry->super, forknum, desired);
				MapSuperblockSetLastUpdatedLSN(&entry->super, map_lsn);
				entry->page_lsn = map_lsn;
				entry->flags |= MAPSUPER_FLAG_DIRTY;
				current = desired;
			}

			desired = Max(desired, MapSuperGetExtendingTarget(entry, forknum));
			if (current >= desired)
			{
				entry->runtime_flags &= ~extend_flag;
				MapSuperSetExtendingTarget(entry, forknum, InvalidBlockNumber);
				LWLockRelease(&entry->lock);
				success = true;
				break;
			}

			MapSuperSetExtendingTarget(entry, forknum, desired);
			LWLockRelease(&entry->lock);
		}
	}
	PG_CATCH();
	{
		if (MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
		{
			entry->runtime_flags &= ~extend_flag;
			MapSuperSetExtendingTarget(entry, forknum, InvalidBlockNumber);
			LWLockRelease(&entry->lock);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return success;
}

void
MapSBlockInit(UmbraFileContext *map_ctx, RelFileLocator rnode, XLogRecPtr map_lsn)
{
	MapSuperEntry *entry;
	MapSuperblock	super;
	MapSuperblock	write_super;
	char		sector[MAP_SUPERBLOCK_SIZE];
	XLogRecPtr	write_lsn;

	Assert(map_ctx != NULL);
	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		elog(ERROR, "MAP fork is missing while initializing superblock");

	entry = MapSuperEnsureEntryLocked(rnode);

	MapSuperblockInit(&super, 0);

	entry->super = super;
	entry->page_lsn = (map_lsn != InvalidXLogRecPtr) ?
		map_lsn : GetXLogWriteRecPtr();
	MapSuperblockSetLastUpdatedLSN(&entry->super, entry->page_lsn);
	entry->flags = MAPSUPER_FLAG_VALID | MAPSUPER_FLAG_DIRTY;
	MapSuperResetReservedNextFrees(entry);
	Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		MAIN_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
	Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		FSM_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
	Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		VISIBILITYMAP_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));

	/*
	 * Persist superblock immediately so later backends in bootstrap/initdb can
	 * read block 0 even before checkpoint/mapwriter gets a chance to flush.
	 * This keeps create-time O(1): only one 512-byte sector is written.
	 */
	write_super = entry->super;
	write_lsn = entry->page_lsn;
	LWLockRelease(&entry->lock);

	if (!InRecovery && write_lsn != InvalidXLogRecPtr)
		XLogFlush(write_lsn);

	MapSuperblockRefreshCRC(&write_super);
	MapSuperblockPackSector(&write_super, sector);
	umfile_ctx_write(map_ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
					 sector, MAP_SUPERBLOCK_SIZE, false);
	umfile_ctx_register_dirty(map_ctx, UMBRA_METADATA_FORKNUM, MAP_BLOCK_SUPER,
							  false, false);
}

bool
MapSBlockEnsureLoaded(UmbraFileContext *map_ctx, RelFileLocator rnode)
{
	MapSuperEntry *entry;

	Assert(map_ctx != NULL);

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return false;

	if (!MapSuperFindEntryLocked(rnode, LW_SHARED, &entry))
	{
		MapSuperblock	disk_super;
		MapSBlockReadStatus status;

		status = MapSuperLoadFromDisk(map_ctx, rnode, &disk_super);
		if (status == MAP_SBLOCK_READ_MISSING)
			return false;

		entry = MapSuperEnsureEntryLocked(rnode);
		if ((entry->flags & MAPSUPER_FLAG_VALID) == 0)
		{
			if (status == MAP_SBLOCK_READ_OK)
			{
				entry->super = disk_super;
				entry->page_lsn = MapSuperblockGetLastUpdatedLSN(&disk_super);
				entry->flags = MAPSUPER_FLAG_VALID;
				MapSuperResetReservedNextFrees(entry);
			}
			else
			{
				MapSuperblockInit(&entry->super, 0);
				entry->page_lsn = InvalidXLogRecPtr;
				entry->flags = MAPSUPER_FLAG_VALID | MAPSUPER_FLAG_CORRUPT;
				MapSuperResetReservedNextFrees(entry);
			}
		}
	}

	Assert(MapNormalizeForkBlockCount(MAIN_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		MAIN_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, MAIN_FORKNUM));
	Assert(MapNormalizeForkBlockCount(FSM_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		FSM_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, FSM_FORKNUM));
	Assert(MapNormalizeForkBlockCount(VISIBILITYMAP_FORKNUM,
									  MapSuperblockGetNextFreePhysBlock(&entry->super,
																		VISIBILITYMAP_FORKNUM)) <=
		   MapSuperGetReservedNextFree(entry, VISIBILITYMAP_FORKNUM));
	LWLockRelease(&entry->lock);
	return true;
}

bool
MapSBlockTryGetLogicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							  ForkNumber forknum,
							  BlockNumber *nblocks)
{
	MapSuperblock super;

	Assert(nblocks != NULL);

	if (!MapForkHasMappedState(forknum))
		return false;

	switch (MapSBlockRead(map_ctx, rnode, &super))
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
			return false;
		case MAP_SBLOCK_READ_CORRUPT:
			if (!InRecovery)
				MapSBlockReportCorrupt(rnode, "invalid identity/CRC or short file");
			return false;
	}

	if (!MapSuperblockHasValidIdentity(&super))
		return false;

	*nblocks = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetLogicalNblocks(&super, forknum));
	return true;
}

bool
MapSBlockForkExists(UmbraFileContext *map_ctx, RelFileLocator rnode,
					ForkNumber forknum)
{
	MapSuperblock super;

	if (!MapForkHasMappedState(forknum))
		return false;

	switch (MapSBlockRead(map_ctx, rnode, &super))
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
		case MAP_SBLOCK_READ_CORRUPT:
			return false;
	}

	if (!MapSuperblockHasValidIdentity(&super))
		return false;

	return MapSuperForkExists(&super, forknum);
}

bool
MapSBlockTryGetPhysicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							   ForkNumber forknum, BlockNumber *nblocks)
{
	MapSuperblock super;

	Assert(nblocks != NULL);

	if (!MapForkHasMappedState(forknum))
		return false;

	switch (MapSBlockRead(map_ctx, rnode, &super))
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
			return false;
		case MAP_SBLOCK_READ_CORRUPT:
			if (!InRecovery)
				MapSBlockReportCorrupt(rnode, "invalid identity/CRC or short file");
			return false;
	}

	if (!MapSuperblockHasValidIdentity(&super))
		return false;

	*nblocks = MapNormalizeForkBlockCount(forknum,
										  MapSuperblockGetPhysCapacity(&super, forknum));
	return true;
}

bool
MapSBlockTryGetNextFreePhysBlock(UmbraFileContext *map_ctx, RelFileLocator rnode,
								 ForkNumber forknum, BlockNumber *next_free_pblk)
{
	MapSuperblock super;

	Assert(next_free_pblk != NULL);

	if (!MapForkHasMappedState(forknum))
		return false;

	switch (MapSBlockRead(map_ctx, rnode, &super))
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
			return false;
		case MAP_SBLOCK_READ_CORRUPT:
			if (!InRecovery)
				MapSBlockReportCorrupt(rnode, "invalid identity/CRC or short file");
			return false;
	}

	if (!MapSuperblockHasValidIdentity(&super))
		return false;

	*next_free_pblk = MapNormalizeForkBlockCount(forknum,
												 MapSuperblockGetNextFreePhysBlock(&super, forknum));
	return true;
}

void
MapSBlockBumpLogicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							ForkNumber forknum, BlockNumber nblocks,
							XLogRecPtr map_lsn)
{
	MapSBlockUpdateLogicalNblocks(map_ctx, rnode, forknum, nblocks,
								  map_lsn, true);
}

void
MapSBlockBumpPhysicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
							 ForkNumber forknum, BlockNumber nblocks,
							 XLogRecPtr map_lsn)
{
	MapSBlockBumpPhysicalState(map_ctx, rnode, forknum, nblocks,
							   false, true, map_lsn);
}

void
MapSBlockBumpNextFreePhysBlock(UmbraFileContext *map_ctx, RelFileLocator rnode,
							   ForkNumber forknum, BlockNumber next_free_pblk,
							   XLogRecPtr map_lsn)
{
	MapSBlockBumpPhysicalState(map_ctx, rnode, forknum, next_free_pblk,
							   true, false, map_lsn);
}

void
MapSBlockSetLogicalNblocks(UmbraFileContext *map_ctx, RelFileLocator rnode,
						   ForkNumber forknum, BlockNumber nblocks,
						   XLogRecPtr map_lsn)
{
	MapSBlockUpdateLogicalNblocks(map_ctx, rnode, forknum, nblocks,
								  map_lsn, false);
}

void
MapSBlockSetSkipWalPending(UmbraFileContext *map_ctx, RelFileLocator rnode,
						   bool pending, XLogRecPtr map_lsn)
{
	MapSBlockSetPendingFlag(map_ctx, rnode, pending, map_lsn);
}

bool
MapSBlockIsSkipWalPending(UmbraFileContext *map_ctx, RelFileLocator rnode)
{
	MapSuperblock super;

	switch (MapSBlockRead(map_ctx, rnode, &super))
	{
		case MAP_SBLOCK_READ_OK:
			break;
		case MAP_SBLOCK_READ_MISSING:
		case MAP_SBLOCK_READ_CORRUPT:
			return false;
	}

	if (!MapSuperblockHasValidIdentity(&super))
		return false;

	return (MapSuperblockGetFlags(&super) &
			MAP_SUPERBLOCK_FLAG_SKIP_WAL_PENDING) != 0;
}

static void
MapSuperTableRefreshDerivedState(void)
{
	MapSuperCapacity = Max(map_superblocks, MAP_SUPERBLOCK_MIN_ENTRIES);
	MapSuperIndexCapacityPerPartition =
		MapSuperIndexCapacityForPartition(MapSuperCapacity);
}

void
MapSuperTableShmemRequest(void)
{
	int			total_index_slots;

	MapSuperTableRefreshDerivedState();
	total_index_slots =
		MapSuperIndexCapacityPerPartition * MAP_SUPER_NPARTITIONS;

	ShmemRequestStruct(.name = "Map Superblock Table Ctl",
					   .size = sizeof(MapSuperCtl),
					   .ptr = (void **) &MapSuperCtlData,
		);

	ShmemRequestStruct(.name = "Map Superblock Partition Locks",
					   .size = MAP_SUPER_NPARTITIONS * sizeof(LWLockPadded),
					   .ptr = (void **) &MapSuperPartitionLocks,
		);

	ShmemRequestStruct(.name = "Map Superblock Table Entries",
					   .size = MapSuperCapacity * sizeof(MapSuperEntry),
					   .ptr = (void **) &MapSuperEntries,
		);

	ShmemRequestStruct(.name = "Map Superblock Table Index",
					   .size = total_index_slots * sizeof(MapSuperIndexSlot),
					   .ptr = (void **) &MapSuperIndex,
		);
}

void
MapSuperTableShmemInit(void)
{
	int			total_index_slots;
	int			i;

	MapSuperTableRefreshDerivedState();
	total_index_slots = MapSuperIndexCapacityPerPartition * MAP_SUPER_NPARTITIONS;

	for (i = 0; i < MAP_SUPER_NPARTITIONS; i++)
		LWLockInitialize(&MapSuperPartitionLocks[i].lock,
						 LWTRANCHE_MAP_BUFFER_CONTENT);

	MapSuperCtlData->free_head = 0;
	SpinLockInit(&MapSuperCtlData->free_list_lock);
	for (i = 0; i < MapSuperCapacity; i++)
	{
		MapSuperEntry *entry = &MapSuperEntries[i];

		MemSet(entry, 0, sizeof(*entry));
		entry->next_free =
			(i == MapSuperCapacity - 1) ? MAPSUPER_FREENEXT_END : (i + 1);
		entry->in_use = false;
		entry->reserved_next_free_main = 0;
		entry->reserved_next_free_fsm = 0;
		entry->reserved_next_free_vm = 0;
		entry->extending_target_main = InvalidBlockNumber;
		entry->extending_target_fsm = InvalidBlockNumber;
		entry->extending_target_vm = InvalidBlockNumber;
		entry->prealloc_count_main = 0;
		entry->prealloc_count_fsm = 0;
		entry->prealloc_count_vm = 0;
		LWLockInitialize(&entry->lock, LWTRANCHE_MAP_BUFFER_CONTENT);
	}

	for (i = 0; i < total_index_slots; i++)
		MapSuperIndex[i].slot_id = MAPSUPER_INDEX_EMPTY;
}

void
MapSuperTableShmemAttach(void)
{
	MapSuperTableRefreshDerivedState();
}
