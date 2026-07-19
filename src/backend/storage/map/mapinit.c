/*-------------------------------------------------------------------------
 *
 * mapinit.c
 *	  Shared-memory initialization for Umbra MAP caches.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapinit.c
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
	MapSuperTableShmemRequest();
	MapReclaimShmemRequest();
}

static void
MapShmemInit(void *arg)
{
	MapPagePoolShmemInit();
	MapSuperTableShmemInit();
	MapReclaimShmemInit();
}

static void
MapShmemAttach(void *arg)
{
	MapPagePoolShmemAttach();
	MapSuperTableShmemAttach();
	MapReclaimShmemAttach();
}
