/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  Umbra MAP background-worker coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/umfile.h"
#include "storage/ummap.h"

void
MapPageRecordAllocation(void)
{
	pg_atomic_fetch_add_u32(&MapPagePoolCtlData->num_allocs, 1);
	MapWakeWriter();
}

uint32
MapPageTakeRecentAllocations(void)
{
	return pg_atomic_exchange_u32(&MapPagePoolCtlData->num_allocs, 0);
}

void
MapStrategyNotifyWriter(int mapwriter_procno)
{
	SpinLockAcquire(&MapPagePoolCtlData->strategy_lock);
	MapPagePoolCtlData->mapwriter_procno = mapwriter_procno;
	SpinLockRelease(&MapPagePoolCtlData->strategy_lock);
}

void
MapWakeWriter(void)
{
	int			mapwriter_procno;

	SpinLockAcquire(&MapPagePoolCtlData->strategy_lock);
	mapwriter_procno = MapPagePoolCtlData->mapwriter_procno;
	if (mapwriter_procno != INVALID_PROC_NUMBER)
		MapPagePoolCtlData->mapwriter_procno = INVALID_PROC_NUMBER;
	SpinLockRelease(&MapPagePoolCtlData->strategy_lock);

	if (mapwriter_procno != INVALID_PROC_NUMBER)
		SetLatch(&ProcGlobal->allProcs[mapwriter_procno].procLatch);
}

int
MapPreallocStep(int max_relations)
{
	static int	scan_start = 0;
	MapSuperTag *tags;
	int			count;
	int			visited;
	int			operations = 0;

	if (InRecovery || max_relations <= 0)
		return 0;

	count = MapSuperCollectTags(InvalidOid, InvalidOid, &tags);
	if (count == 0)
		return 0;
	scan_start %= count;
	visited = Min(count, max_relations);

	for (int i = 0; i < visited; i++)
	{
		RelFileLocatorBackend rlocator =
			tags[(scan_start + i) % count].rlocator;

		if (RelFileLocatorBackendIsTemp(rlocator))
			continue;
		if (ummap_maybe_preallocate(NULL, rlocator, MAIN_FORKNUM, true))
			operations++;
	}

	scan_start = (scan_start + visited) % count;
	pfree(tags);
	return operations;
}
