/*-------------------------------------------------------------------------
 *
 * mapbgproc.c
 *	  MAP background writer coordination.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/proc.h"

void
MapStrategyNotifyWriter(int mapwriter_procno)
{
	SpinLockAcquire(&MapShared->clock_lock);
	MapShared->mapwriter_procno = mapwriter_procno;
	SpinLockRelease(&MapShared->clock_lock);
}

void
MapWakeWriter(void)
{
	int			mapwriter_procno = -1;

	SpinLockAcquire(&MapShared->clock_lock);
	mapwriter_procno = MapShared->mapwriter_procno;
	if (mapwriter_procno != -1)
		MapShared->mapwriter_procno = -1;
	SpinLockRelease(&MapShared->clock_lock);

	if (mapwriter_procno != -1)
		SetLatch(&ProcGlobal->allProcs[mapwriter_procno].procLatch);
}
