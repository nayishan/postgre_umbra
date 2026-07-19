/*-------------------------------------------------------------------------
 *
 * mapcompactor.c
 *	  Umbra MAP compactor background worker.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/mapcompactor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/mapcompactor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/map.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#define MAPCOMPACTOR_HIBERNATE_FACTOR 50
#define MAPCOMPACTOR_RECLAIM_TASKS 8

int			MapCompactorDelay = 200;
int			MapCompactorMaxRelations = 8;

static void
MapCompactorExitCallback(int code, Datum arg)
{
	(void) code;
	(void) arg;
	MapStrategyNotifyCompactor(INVALID_PROC_NUMBER);
}

void
MapCompactorRegister(void)
{
	BackgroundWorker worker;

	MemSet(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "MapCompactorMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "Umbra mapcompactor");
	snprintf(worker.bgw_type, BGW_MAXLEN, "map compactor");
	worker.bgw_restart_time = 5;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&worker);
}

void
MapCompactorMain(Datum arg)
{
	MemoryContext mapcompactor_context;
	bool		previously_idle = false;

	(void) arg;
	before_shmem_exit(MapCompactorExitCallback, 0);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(InvalidOid, InvalidOid, 0);
	mapcompactor_context = AllocSetContextCreate(TopMemoryContext,
											 "Map Compactor",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(mapcompactor_context);

	for (;;)
	{
		int			moves = 0;
		int			reclaimed = 0;
		bool		idle;
		int			rc;

		ResetLatch(MyLatch);
		MapStrategyNotifyCompactor(MyProcNumber);
		ProcessMainLoopInterrupts();

		PG_TRY();
		{
			StartTransactionCommand();
			reclaimed = MapReclaimStep(MAPCOMPACTOR_RECLAIM_TASKS);
			if (map_compactor_enable && MapCompactorMaxRelations > 0)
				moves = MapCompactorStep(MapCompactorMaxRelations);
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			HOLD_INTERRUPTS();
			EmitErrorReport();
			AbortOutOfAnyTransaction();
			FlushErrorState();
			MemoryContextSwitchTo(mapcompactor_context);
			MemoryContextReset(mapcompactor_context);
			moves = 0;
			reclaimed = 0;
			RESUME_INTERRUPTS();
		}
		PG_END_TRY();

		idle = moves == 0 && reclaimed == 0;
		if (FirstCallSinceLastCheckpoint())
			smgrdestroyall();
		MemoryContextSwitchTo(mapcompactor_context);
		MemoryContextReset(mapcompactor_context);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   MapCompactorDelay,
					   WAIT_EVENT_MAPCOMPACTOR_MAIN);
		if (rc == WL_TIMEOUT && idle && previously_idle)
			(void) WaitLatch(MyLatch,
								 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 MapCompactorDelay * MAPCOMPACTOR_HIBERNATE_FACTOR,
								 WAIT_EVENT_MAPCOMPACTOR_HIBERNATE);
		MapStrategyNotifyCompactor(INVALID_PROC_NUMBER);
		previously_idle = idle;
	}
}
