/*-------------------------------------------------------------------------
 *
 * mapclock.c
 *	  clock sweep algorithm for map buffer replacement
 *
 * This implements a clock sweep algorithm similar to freelist.c,
 * but for managing map buffers instead of data buffers.
 *
 * Also handles the map cache hash table, similar to buf_table.c.
 *
 * src/backend/storage/map/mapclock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/hsearch.h"

#define LOG2_NUM_MAP_CACHE_PARTITIONS 5
#define NUM_MAP_CACHE_PARTITIONS (1 << LOG2_NUM_MAP_CACHE_PARTITIONS)

typedef struct MapCacheTag
{
	RelFileLocator	rnode;
	ForkNumber	forknum;
	BlockNumber	map_blkno;
} MapCacheTag;

typedef struct MapCacheEntry
{
	MapCacheTag	key;
	int			slot_id;
} MapCacheEntry;

static HTAB *MapCacheHash = NULL;
static LWLockPadded *MapCachePartitionLocks = NULL;

static inline uint32
MapCacheHashCode(MapCacheTag *tag)
{
	Assert(MapCacheHash != NULL);
	return get_hash_value(MapCacheHash, (void *) tag);
}

static inline LWLock *
MapCachePartitionLock(uint32 hashcode)
{
	return &MapCachePartitionLocks[hashcode & (NUM_MAP_CACHE_PARTITIONS - 1)].lock;
}

void
MapCacheTableShmemRequest(void)
{
	long		hash_size;

	hash_size = Max((long) map_buffers, (long) map_buffers * 2L);

	ShmemRequestStruct(.name = "Map Cache Partition Locks",
					   .size = NUM_MAP_CACHE_PARTITIONS * sizeof(LWLockPadded),
					   .ptr = (void **) &MapCachePartitionLocks,
		);

	ShmemRequestHash(.name = "Map Cache Lookup Table",
					 .nelems = hash_size,
					 .ptr = &MapCacheHash,
					 .hash_info.keysize = sizeof(MapCacheTag),
					 .hash_info.entrysize = sizeof(MapCacheEntry),
					 .hash_info.num_partitions = NUM_MAP_CACHE_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
		);
}

void
MapCacheTableShmemInit(void)
{
	int			i;

	for (i = 0; i < NUM_MAP_CACHE_PARTITIONS; i++)
		LWLockInitialize(&MapCachePartitionLocks[i].lock,
						 LWTRANCHE_MAP_BUFFER_CONTENT);
}

/*
 * MapCacheLookup - lookup a buffer slot in the cache
 * Returns slot_id if found, -1 otherwise
 */
int
MapCacheLookup(RelFileLocator rnode, ForkNumber forknum, BlockNumber map_blkno)
{
	MapCacheTag	tag;
	MapCacheEntry *entry;
	uint32		hashcode;
	int			slot_id = -1;
	LWLock	   *partition_lock;

	tag.rnode = rnode;
	tag.forknum = forknum;
	tag.map_blkno = map_blkno;
	hashcode = MapCacheHashCode(&tag);
	partition_lock = MapCachePartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	entry = (MapCacheEntry *)
		hash_search_with_hash_value(MapCacheHash,
									(void *) &tag,
									hashcode,
									HASH_FIND,
									NULL);
	if (entry != NULL)
		slot_id = entry->slot_id;
	LWLockRelease(partition_lock);

	return slot_id;
}

/*
 * MapCacheInsert - insert a buffer slot into the cache.
 *
 * Returns -1 on successful insertion. If another slot already owns the tag,
 * returns that slot id and leaves the existing entry unchanged.
 */
int
MapCacheInsert(RelFileLocator rnode, ForkNumber forknum, BlockNumber map_blkno, int slot_id)
{
	MapCacheTag	tag;
	MapCacheEntry *entry;
	uint32		hashcode;
	bool		found;
	LWLock	   *partition_lock;

	Assert(slot_id >= 0);

	tag.rnode = rnode;
	tag.forknum = forknum;
	tag.map_blkno = map_blkno;
	hashcode = MapCacheHashCode(&tag);
	partition_lock = MapCachePartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	entry = (MapCacheEntry *)
		hash_search_with_hash_value(MapCacheHash,
									(void *) &tag,
									hashcode,
									HASH_ENTER,
									&found);
	if (found)
	{
		int			existing_slot_id = entry->slot_id;

		LWLockRelease(partition_lock);
		return existing_slot_id;
	}

	entry->slot_id = slot_id;
	LWLockRelease(partition_lock);

	return -1;
}

/*
 * MapCacheDelete - remove a buffer slot from the cache
 */
void
MapCacheDelete(RelFileLocator rnode, ForkNumber forknum, BlockNumber map_blkno,
			   int slot_id)
{
	MapCacheTag	tag;
	MapCacheEntry *entry;
	uint32		hashcode;
	LWLock	   *partition_lock;

	Assert(slot_id >= 0);

	tag.rnode = rnode;
	tag.forknum = forknum;
	tag.map_blkno = map_blkno;
	hashcode = MapCacheHashCode(&tag);
	partition_lock = MapCachePartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	entry = (MapCacheEntry *)
		hash_search_with_hash_value(MapCacheHash,
									(void *) &tag,
									hashcode,
									HASH_FIND,
									NULL);
	if (entry != NULL && entry->slot_id == slot_id)
	{
		(void) hash_search_with_hash_value(MapCacheHash,
										   (void *) &tag,
										   hashcode,
										   HASH_REMOVE,
										   NULL);
	}
	LWLockRelease(partition_lock);
}

/*
 * ClockSweepTick - advance the clock hand
 *
 * Returns the next slot to examine.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32      victim;
	int         num_slots;

	num_slots = MapShared->num_slots;

	/*
	 * Atomically move hand ahead one slot.
	 * Multiple processes can do this concurrently.
	 */
	victim = pg_atomic_fetch_add_u32(&MapShared->next_victim_buffer, 1);

	/* Handle wraparound */
	if (victim >= (uint32) num_slots)
	{
		uint32      originalVictim = victim;

		/* What we actually look up in MapBuffers */
		victim = victim % num_slots;

		/*
		 * If we're the one that just caused a wraparound, increment
		 * completePasses while holding the lock.
		 */
		if (victim == 0)
		{
			uint32      expected;
			uint32      wrapped;
			bool        success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				SpinLockAcquire(&MapShared->clock_lock);

				wrapped = expected % num_slots;

				success = pg_atomic_compare_exchange_u32(
					&MapShared->next_victim_buffer,
					&expected, wrapped);
				if (success)
					MapShared->complete_passes++;

				SpinLockRelease(&MapShared->clock_lock);
			}
		}
	}

	return victim;
}

/*
 * MapClockGetBuffer - select a buffer slot using clock algorithm
 *
 * Returns a slot ID that is safe to use (not pinned).
 * The caller is responsible for initializing the slot.
 */
int
MapClockGetBuffer(void)
{
	MapBufferDesc *buf;
	int         trycounter;
	uint32      local_buf_state;
	int         num_slots = MapShared->num_slots;

	/*
	 * If mapwriter asked for allocation notification, wake it up.
	 */
	MapWakeWriter();

	/*
	 * First, check if there's a buffer on the free list.
	 */
	if (MapShared->first_free_buffer >= 0)
	{
		while (true)
		{
			int         slot_id;

			SpinLockAcquire(&MapShared->clock_lock);

			if (MapShared->first_free_buffer < 0)
			{
				SpinLockRelease(&MapShared->clock_lock);
				break;
			}

			slot_id = MapShared->first_free_buffer;
			buf = &MapBuffers[slot_id];

			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Remove from free list */
			MapShared->first_free_buffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			SpinLockRelease(&MapShared->clock_lock);

			/*
			 * Check if the buffer is actually usable.
			 * (It might have been used after being put on free list)
			 */
			local_buf_state = pg_atomic_read_u32(&buf->state);

				if (MAPBUF_GET_REFCOUNT(local_buf_state) == 0 &&
					MAPBUF_GET_USAGECOUNT(local_buf_state) == 0)
			{
				/* Found a usable buffer */
				pg_atomic_fetch_add_u32(&MapShared->num_allocs, 1);
				return slot_id;
			}

			/*
			 * Buffer not usable (pinned or still has usage_count).
			 *
			 * Keep it off free list and let normal clock sweep handle it.
			 * Re-queuing it at free-list head can livelock when the same
			 * non-usable slot is popped repeatedly.
			 */
			continue;
		}
	}

	/*
	 * No free buffers, run the clock sweep algorithm.
	 */
	trycounter = num_slots;

	for (;;)
	{
		uint32      victim_slot;

		victim_slot = ClockSweepTick();
		buf = &MapBuffers[victim_slot];

		local_buf_state = pg_atomic_read_u32(&buf->state);

		/*
		 * If the buffer is pinned, we cannot use it.
		 * If it has a non-zero usage_count, decrement it and continue.
		 */
			if (MAPBUF_GET_REFCOUNT(local_buf_state) == 0)
		{
			if (MAPBUF_GET_USAGECOUNT(local_buf_state) != 0)
			{
				/* Decrement usage_count */
				uint32_t    old_state;
				uint32_t    new_state;

				do
				{
					old_state = pg_atomic_read_u32(&buf->state);
					new_state = old_state - MAPBUF_USAGECOUNT_ONE;
				}
				while (!pg_atomic_compare_exchange_u32(&buf->state,
														&old_state, new_state));

				/* Reset try counter since we made progress */
				trycounter = num_slots;
			}
			else
			{
				/* Found a usable buffer */
				pg_atomic_fetch_add_u32(&MapShared->num_allocs, 1);

				/* Dirty-victim writeback is handled by caller (MapReadBuffer). */

				return (int) victim_slot;
			}
		}
		else if (--trycounter == 0)
		{
			/*
			 * We've scanned all buffers and all are pinned.
			 * This shouldn't happen with reasonable sizing.
			 */
			elog(ERROR, "no unpinned map buffers available");
		}
	}
}

/*
 * MapClockFreeBuffer - return a buffer to the free list
 *
 * Low-level function that adds a buffer to the free list.
 * The buffer's state should already be cleaned before calling this.
 * This is called by MapInvalidateBuffer.
 */
void
MapClockFreeBuffer(int slot_id)
{
	MapBufferDesc *buf;
	uint32      state;

	buf = &MapBuffers[slot_id];

	/* Check if buffer is already on free list */
	SpinLockAcquire(&MapShared->clock_lock);

	if (buf->freeNext != FREENEXT_NOT_IN_LIST)
	{
		/* Already on free list, just return */
		SpinLockRelease(&MapShared->clock_lock);
		return;
	}

	/*
	 * Free list must only contain fully reusable slots.
	 * Caller is responsible for clearing refcount/usage first.
	 */
	state = pg_atomic_read_u32(&buf->state);
	Assert(MAPBUF_GET_REFCOUNT(state) == 0);
	Assert(MAPBUF_GET_USAGECOUNT(state) == 0);

	/* Insert at head of free list */
	buf->freeNext = MapShared->first_free_buffer;
	MapShared->first_free_buffer = slot_id;

	SpinLockRelease(&MapShared->clock_lock);
}

/*
 * MapSyncStart - tell checkpoint where to start syncing
 *
 * Returns the starting slot ID for checkpoint sync.
 */
int
MapSyncStart(uint32 *complete_passes, uint32 *num_allocs)
{
	uint32      next_victim;
	int         result;

	SpinLockAcquire(&MapShared->clock_lock);

	next_victim = pg_atomic_read_u32(&MapShared->next_victim_buffer);
	result = next_victim % MapShared->num_slots;

	if (complete_passes)
	{
		*complete_passes = MapShared->complete_passes;
		*complete_passes += next_victim / MapShared->num_slots;
	}

	if (num_allocs)
	{
		*num_allocs = pg_atomic_exchange_u32(&MapShared->num_allocs, 0);
	}

	SpinLockRelease(&MapShared->clock_lock);

	return result;
}
