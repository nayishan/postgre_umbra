/*-------------------------------------------------------------------------
 *
 * mapinit.c
 *	  Shared-memory initialization for the resident MAP superblock cache.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map_internal.h"
#include "storage/subsystems.h"

static void MapShmemRequest(void *arg);
static void MapShmemInit(void *arg);
static void MapShmemAttach(void *arg);

const ShmemCallbacks MapShmemCallbacks = {
	.request_fn = MapShmemRequest,
	.init_fn = MapShmemInit,
	.attach_fn = MapShmemAttach,
};

static void
MapShmemRequest(void *arg)
{
	MapSuperTableShmemRequest();
}

static void
MapShmemInit(void *arg)
{
	MapSuperTableShmemInit();
}

static void
MapShmemAttach(void *arg)
{
	MapSuperTableShmemAttach();
}
