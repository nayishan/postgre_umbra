/*-------------------------------------------------------------------------
 *
 * mapinit.c
 *    Shared-memory initialization for Umbra selector-page buffers.
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
	MapPagePoolShmemRequest();
}

static void
MapShmemInit(void *arg)
{
	MapPagePoolShmemInit();
}

static void
MapShmemAttach(void *arg)
{
	MapPagePoolShmemAttach();
}
