/*-------------------------------------------------------------------------
 *
 * mapclock.c
 *	  Lookup and replacement strategy for ordinary Umbra MAP pages.
 *
 * This pool follows the shared-buffer shape: a partitioned tag hash protects
 * identity lookup, while a freelist and clock sweep choose unpinned victims.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapclock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "storage/bufmgr.h"
#include "storage/map_internal.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

/* One MAP page covers BLCKSZ / sizeof(BlockNumber) logical data blocks. */
#define MAP_PAGE_MIN_BUFFERS		16
#define MAP_PAGE_BUFFER_FRACTION 128
#define MAP_PAGE_CACHE_PARTITIONS 16
#define MAP_PAGE_EXTENSION_PARTITIONS 16

typedef struct MapPageCacheEntry
{
	MapPageTag	tag;
	int			slot_id;
} MapPageCacheEntry;

MapPagePoolCtl *MapPagePoolCtlData = NULL;
MapPageDesc *MapPageDescriptors = NULL;
PGIOAlignedBlock *MapPageBlocks = NULL;
int			MapPageBufferCount = 0;

static HTAB *MapPageCacheHash = NULL;
static LWLockPadded *MapPageCacheLocks = NULL;
static LWLockPadded *MapPageExtensionLocks = NULL;

static void MapPageRefreshBufferCount(void);
static uint32 MapPageRelationHash(RelFileLocatorBackend rlocator);
static int	MapPageClockTick(void);
static bool MapPageTryReservePendingSlot(void);
static void MapPageReleasePendingSlot(void);

void
MapPagePoolShmemRequest(void)
{
	long		hash_size;

	MapPageRefreshBufferCount();
	hash_size = Max((long) MAP_PAGE_CACHE_PARTITIONS,
					(long) MapPageBufferCount * 2L);

	ShmemRequestStruct(.name = "Umbra MAP Page Pool Control",
					   .size = sizeof(MapPagePoolCtl),
					   .ptr = (void **) &MapPagePoolCtlData,
		);
	ShmemRequestStruct(.name = "Umbra MAP Page Descriptors",
					   .size = mul_size(MapPageBufferCount,
										sizeof(MapPageDesc)),
					   .ptr = (void **) &MapPageDescriptors,
		);
	ShmemRequestStruct(.name = "Umbra MAP Page Data",
					   .size = mul_size(MapPageBufferCount,
										sizeof(PGIOAlignedBlock)),
					   .alignment = PG_IO_ALIGN_SIZE,
					   .ptr = (void **) &MapPageBlocks,
		);
	ShmemRequestStruct(.name = "Umbra MAP Page Mapping Locks",
					   .size = mul_size(MAP_PAGE_CACHE_PARTITIONS,
										sizeof(LWLockPadded)),
					   .ptr = (void **) &MapPageCacheLocks,
		);
	ShmemRequestStruct(.name = "Umbra MAP Page Extension Locks",
					   .size = mul_size(MAP_PAGE_EXTENSION_PARTITIONS,
										sizeof(LWLockPadded)),
					   .ptr = (void **) &MapPageExtensionLocks,
		);
	ShmemRequestHash(.name = "Umbra MAP Page Lookup Table",
					 .nelems = hash_size,
					 .ptr = &MapPageCacheHash,
					 .hash_info.keysize = sizeof(MapPageTag),
					 .hash_info.entrysize = sizeof(MapPageCacheEntry),
					 .hash_info.num_partitions = MAP_PAGE_CACHE_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
		);
}

void
MapPagePoolShmemInit(void)
{
	int			slot_id;

	MapPagePoolCtlData->nslots = MapPageBufferCount;
	MapPagePoolCtlData->first_free = 0;
	pg_atomic_init_u64(&MapPagePoolCtlData->next_victim, 0);
	pg_atomic_init_u32(&MapPagePoolCtlData->pending_reservations, 0);
	SpinLockInit(&MapPagePoolCtlData->strategy_lock);

	for (slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
	{
		MapPageDesc *desc = &MapPageDescriptors[slot_id];

		MemSet(&desc->tag, 0, sizeof(desc->tag));
		MemSet(desc->pending_bits, 0, sizeof(desc->pending_bits));
		MemSet(&desc->pending_range, 0, sizeof(desc->pending_range));
		MemSet(&desc->replay_range, 0, sizeof(desc->replay_range));
		desc->pending_pin_refs = 0;
		desc->slot_id = slot_id;
		desc->free_next = slot_id == MapPageBufferCount - 1 ?
			MAP_PAGE_FREENEXT_END : slot_id + 1;
		pg_atomic_init_u64(&desc->state, 0);
		LWLockInitialize(&desc->content_lock, LWTRANCHE_MAP_PAGE_CONTENT);
		LWLockInitialize(&desc->io_lock, LWTRANCHE_MAP_PAGE_CONTENT);
	}

	for (slot_id = 0; slot_id < MAP_PAGE_CACHE_PARTITIONS; slot_id++)
		LWLockInitialize(&MapPageCacheLocks[slot_id].lock,
						 LWTRANCHE_MAP_PAGE_MAPPING);
	for (slot_id = 0; slot_id < MAP_PAGE_EXTENSION_PARTITIONS; slot_id++)
		LWLockInitialize(&MapPageExtensionLocks[slot_id].lock,
						 LWTRANCHE_MAP_PAGE_EXTENSION);
}

void
MapPagePoolShmemAttach(void)
{
	MapPageRefreshBufferCount();
	MapPageEnsureInitialized();
	Assert(MapPagePoolCtlData->nslots == MapPageBufferCount);
}

void
MapPageEnsureInitialized(void)
{
	if (!MapPagePoolIsInitialized())
		elog(ERROR, "Umbra MAP page buffer pool is not initialized");
}

bool
MapPagePoolIsInitialized(void)
{
	return MapPagePoolCtlData != NULL && MapPageDescriptors != NULL &&
		MapPageBlocks != NULL && MapPageCacheHash != NULL &&
		MapPageCacheLocks != NULL && MapPageExtensionLocks != NULL;
}

uint32
MapPageCacheHashCode(const MapPageTag *tag)
{
	Assert(tag != NULL);
	Assert(MapPageCacheHash != NULL);
	return get_hash_value(MapPageCacheHash, tag);
}

int
MapPageCachePartition(uint32 hashcode)
{
	return hashcode & (MAP_PAGE_CACHE_PARTITIONS - 1);
}

LWLock *
MapPageCachePartitionLock(uint32 hashcode)
{
	return &MapPageCacheLocks[MapPageCachePartition(hashcode)].lock;
}

int
MapPageCacheLookup(const MapPageTag *tag, uint32 hashcode)
{
	MapPageCacheEntry *entry;

	Assert(LWLockHeldByMe(MapPageCachePartitionLock(hashcode)));
	entry = hash_search_with_hash_value(MapPageCacheHash, tag, hashcode,
										HASH_FIND, NULL);
	return entry == NULL ? -1 : entry->slot_id;
}

int
MapPageCacheInsert(const MapPageTag *tag, uint32 hashcode, int slot_id)
{
	MapPageCacheEntry *entry;
	bool		found;

	Assert(LWLockHeldByMeInMode(MapPageCachePartitionLock(hashcode),
								LW_EXCLUSIVE));
	entry = hash_search_with_hash_value(MapPageCacheHash, tag, hashcode,
										HASH_ENTER, &found);
	if (found)
		return entry->slot_id;

	entry->slot_id = slot_id;
	return -1;
}

void
MapPageCacheDelete(const MapPageTag *tag, uint32 hashcode, int slot_id)
{
	MapPageCacheEntry *entry;

	Assert(LWLockHeldByMeInMode(MapPageCachePartitionLock(hashcode),
								LW_EXCLUSIVE));
	entry = hash_search_with_hash_value(MapPageCacheHash, tag, hashcode,
										HASH_FIND, NULL);
	if (entry != NULL && entry->slot_id == slot_id)
		(void) hash_search_with_hash_value(MapPageCacheHash, tag, hashcode,
										   HASH_REMOVE, NULL);
}

LWLock *
MapPageExtensionLock(RelFileLocatorBackend rlocator)
{
	uint32		hashcode = MapPageRelationHash(rlocator);

	return &MapPageExtensionLocks[hashcode &
								  (MAP_PAGE_EXTENSION_PARTITIONS - 1)].lock;
}

int
MapPageClockGetBuffer(void)
{
	int			trycounter;

	MapPageEnsureInitialized();

	for (;;)
	{
		int			slot_id;

		SpinLockAcquire(&MapPagePoolCtlData->strategy_lock);
		slot_id = MapPagePoolCtlData->first_free;
		if (slot_id >= 0)
		{
			MapPageDesc *desc = &MapPageDescriptors[slot_id];

			MapPagePoolCtlData->first_free = desc->free_next;
			desc->free_next = MAP_PAGE_FREENEXT_NOT_IN_LIST;
		}
		SpinLockRelease(&MapPagePoolCtlData->strategy_lock);

		if (slot_id < 0)
			break;
		if (MapPageTryClaimBuffer(slot_id))
		{
			MapPageRememberPin(slot_id);
			return slot_id;
		}
	}

	trycounter = MapPagePoolCtlData->nslots;
	for (;;)
	{
		MapPageDesc *desc;
		uint64		old_state;
		int			slot_id;

		slot_id = MapPageClockTick();
		desc = &MapPageDescriptors[slot_id];
		old_state = pg_atomic_read_u64(&desc->state);

		for (;;)
		{
			uint64		new_state;
			bool		decrement_usage;

			if (MAP_PAGE_GET_REFCOUNT(old_state) != 0)
				break;
			decrement_usage = MAP_PAGE_GET_USAGE(old_state) != 0;
			if (decrement_usage)
				new_state = old_state - MAP_PAGE_USAGE_ONE;
			else
				new_state = old_state + 1;

			if (!pg_atomic_compare_exchange_u64(&desc->state, &old_state,
												new_state))
				continue;

			if (decrement_usage)
			{
				trycounter = MapPagePoolCtlData->nslots;
				break;
			}

			MapPageRememberPin(slot_id);
			return slot_id;
		}

		if (--trycounter == 0)
			elog(ERROR, "no unpinned Umbra MAP page buffers are available");
	}
}

void
MapPageClockFreeBuffer(int slot_id)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];

	SpinLockAcquire(&MapPagePoolCtlData->strategy_lock);
	if (desc->free_next == MAP_PAGE_FREENEXT_NOT_IN_LIST)
	{
		desc->free_next = MapPagePoolCtlData->first_free;
		MapPagePoolCtlData->first_free = slot_id;
	}
	SpinLockRelease(&MapPagePoolCtlData->strategy_lock);
}

bool
MapPageRegisterPendingPin(MapPageDesc *desc)
{
	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	if (desc->pending_pin_refs == UINT32_MAX)
		return false;
	if (desc->pending_pin_refs == 0 && !MapPageTryReservePendingSlot())
		return false;
	desc->pending_pin_refs++;
	return true;
}

void
MapPageUnregisterPendingPin(MapPageDesc *desc)
{
	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	Assert(desc->pending_pin_refs > 0);
	desc->pending_pin_refs--;
	if (desc->pending_pin_refs == 0)
		MapPageReleasePendingSlot();
}

static bool
MapPageTryReservePendingSlot(void)
{
	uint32		limit;
	uint32		old_reservations;

	Assert(MapPagePoolIsInitialized());
	limit = MapPagePoolCtlData->nslots - 1;
	old_reservations = pg_atomic_read_u32(
		&MapPagePoolCtlData->pending_reservations);
	for (;;)
	{
		uint32		new_reservations;

		if (old_reservations >= limit)
			return false;
		new_reservations = old_reservations + 1;
		if (pg_atomic_compare_exchange_u32(
				&MapPagePoolCtlData->pending_reservations,
				&old_reservations, new_reservations))
			return true;
	}
}

static void
MapPageReleasePendingSlot(void)
{
	uint32		old_reservations;

	Assert(MapPagePoolIsInitialized());
	old_reservations = pg_atomic_fetch_sub_u32(
		&MapPagePoolCtlData->pending_reservations, 1);
	if (old_reservations == 0)
		elog(PANIC, "Umbra MAP pending slot reservation underflow");
}

static void
MapPageRefreshBufferCount(void)
{
	MapPageBufferCount = Max(MAP_PAGE_MIN_BUFFERS,
							 NBuffers / MAP_PAGE_BUFFER_FRACTION);
}

static uint32
MapPageRelationHash(RelFileLocatorBackend rlocator)
{
	RelFileLocatorBackend key = {0};

	key = rlocator;
	return hash_bytes((const unsigned char *) &key, sizeof(key));
}

static int
MapPageClockTick(void)
{
	uint64		victim;

	victim = pg_atomic_fetch_add_u64(&MapPagePoolCtlData->next_victim, 1);
	return victim % MapPagePoolCtlData->nslots;
}
