/*-------------------------------------------------------------------------
 *
 * mapinit.c
 *	  shared-memory and backend initialization for the MAP layer
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper.h"
#include "storage/mapsuper_internal.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/* GUCs */
int			map_buffers = 1024;	/* Number of map buffer slots */
/*
 * Dedicated shared-memory slots for MAP superblocks.
 *
 * These entries back extremely hot runtime metadata.  They are not managed as
 * an LRU-style cache; instead they remain resident until explicit relation or
 * database invalidation releases the slot.  Keep the default large so hot
 * relations do not churn through repeated ensure/load cycles.
 */
int			map_superblocks = 262144;

/* Shared memory pointer */
MapSharedData *MapShared = NULL;

/* Per-process buffer descriptors */
MapBufferDesc *MapBuffers = NULL;

/* Actual page data (contiguous block) */
char	   *MapPageData = NULL;

static void MapShmemRequest(void *arg);
static void MapShmemInit(void *arg);
static void MapShmemAttach(void *arg);

const ShmemCallbacks MapShmemCallbacks = {
	.request_fn = MapShmemRequest,
	.init_fn = MapShmemInit,
	.attach_fn = MapShmemAttach,
};

static void
MapRefreshBufferSlots(void)
{
	int computed_slots = NBuffers >> 7;

	if (computed_slots < 4096)
		computed_slots = 4096;

	map_buffers = computed_slots;
}

void
MapBackendInit(void)
{
	static bool initialized = false;

	if (initialized)
		return;

	MapRefreshBufferSlots();
	MapEnsurePrivateRefCount();
	initialized = true;
}

static void
MapShmemRequest(void *arg)
{
	MapRefreshBufferSlots();

	ShmemRequestStruct(.name = "Map Shared Data",
					   .size = sizeof(MapSharedData),
					   .ptr = (void **) &MapShared,
		);

	ShmemRequestStruct(.name = "Map Buffers",
					   .size = map_buffers * sizeof(MapBufferDesc),
					   .ptr = (void **) &MapBuffers,
		);

	ShmemRequestStruct(.name = "Map Page Data",
					   .size = map_buffers * BLCKSZ,
					   .ptr = (void **) &MapPageData,
		);

	MapCacheTableShmemRequest();
	MapSuperTableShmemRequest();
}

/*
 * Initialize shared memory for map layer during postmaster startup.
 */
static void
MapShmemInit(void *arg)
{
	int			i;

	MapShared->num_slots = map_buffers;
	MapShared->first_free_buffer = 0;
	MapShared->mapwriter_procno = -1;
	pg_atomic_init_u32(&MapShared->next_victim_buffer, 0);
	pg_atomic_init_u32(&MapShared->num_allocs, 0);
	MapShared->complete_passes = 0;
	SpinLockInit(&MapShared->clock_lock);

	for (i = 0; i < map_buffers; i++)
	{
		MapBufferDesc *buf = &MapBuffers[i];

		buf->id = i;
		buf->freeNext = (i == map_buffers - 1) ? FREENEXT_END_OF_LIST : i + 1;
		pg_atomic_init_u32(&buf->state, 0);
		buf->wait_backend_pid = 0;

		memset(&buf->rnode, 0, sizeof(RelFileLocator));
		buf->forknum = InvalidForkNumber;
		buf->page_number = -1;
		buf->page_lsn = 0;

		LWLockInitialize(&buf->buffer_lock, LWTRANCHE_MAP_BUFFER_CONTENT);
		LWLockInitialize(&buf->io_in_progress_lock, LWTRANCHE_MAP_BUFFER_CONTENT);
	}

	memset(MapPageData, 0, map_buffers * BLCKSZ);

	MapCacheTableShmemInit();
	MapSuperTableShmemInit();
}

static void
MapShmemAttach(void *arg)
{
	Assert(MapShared != NULL);
	Assert(MapBuffers != NULL);
	Assert(MapPageData != NULL);
	Assert(MapShared->num_slots == map_buffers);

	MapSuperTableShmemAttach();
}
