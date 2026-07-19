/*-------------------------------------------------------------------------
 *
 * mapwriter.c
 *	  Umbra MAP background writer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/mapwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/mapcompactor.h"
#include "postmaster/mapwriter.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/map.h"
#include "storage/procnumber.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/wait_event.h"

#define MAPWRITER_HIBERNATE_FACTOR 50

int			MapWriterDelay = 200;
int			MapWriterMaxPages = 100;
int			MapWriterPreallocMaxRelations = 32;
double		MapWriterLRUMultiplier = 2.0;

static void
MapWriterExitCallback(int code, Datum arg)
{
	(void) code;
	(void) arg;
	MapStrategyNotifyWriter(INVALID_PROC_NUMBER);
}

void
MapBackgroundWorkersRegister(void)
{
	BackgroundWorker worker;

	MemSet(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "MapWriterMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "Umbra mapwriter");
	snprintf(worker.bgw_type, BGW_MAXLEN, "map writer");
	worker.bgw_restart_time = 5;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&worker);
	MapCompactorRegister();
}

void
MapWriterMain(Datum arg)
{
	bool		previously_idle = false;

	(void) arg;
	before_shmem_exit(MapWriterExitCallback, 0);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(InvalidOid, InvalidOid, 0);

	for (;;)
	{
		uint32		recent_allocations;
		int			cleaned = 0;
		int			preallocated = 0;
		bool		idle;
		int			rc;

		ResetLatch(MyLatch);
		MapStrategyNotifyWriter(MyProcNumber);
		ProcessMainLoopInterrupts();

		recent_allocations = MapPageTakeRecentAllocations();
		if (MapWriterPreallocMaxRelations > 0)
			preallocated = MapPreallocStep(MapWriterPreallocMaxRelations);

		if (MapWriterMaxPages > 0)
		{
			double		target;
			int			target_pages;

			if (recent_allocations == 0)
				target_pages = Max(1, MapWriterMaxPages / 8);
			else
			{
				target = Min((double) MapWriterMaxPages,
							 recent_allocations * MapWriterLRUMultiplier);
				target_pages = (int) (target + 0.5);
			}
			target_pages = Min(MapWriterMaxPages, Max(1, target_pages));
			cleaned = MapPageBgWriterFlush(target_pages);
		}

		idle = recent_allocations == 0 && cleaned == 0 && preallocated == 0;
		if (FirstCallSinceLastCheckpoint())
			smgrdestroyall();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   MapWriterDelay,
					   WAIT_EVENT_MAPWRITER_MAIN);
		if (rc == WL_TIMEOUT && idle && previously_idle)
			(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 MapWriterDelay * MAPWRITER_HIBERNATE_FACTOR,
						 WAIT_EVENT_MAPWRITER_HIBERNATE);
		MapStrategyNotifyWriter(INVALID_PROC_NUMBER);
		previously_idle = idle;
	}
}
