/*-------------------------------------------------------------------------
 *
 * mapwriter.c
 *	  Umbra map writer background worker.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/mapwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/mapwriter.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/map.h"
#include "storage/procnumber.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
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
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "MapWriterMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "Umbra mapwriter");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "map writer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&bgw);
}

void
MapWriterMain(Datum arg)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext mapwriter_context;
	bool		prev_hibernate = false;

	(void) arg;
	before_shmem_exit(MapWriterExitCallback, 0);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, PG_SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGQUIT, SignalHandlerForCrashExit);
	pqsignal(SIGALRM, PG_SIG_IGN);
	pqsignal(SIGPIPE, PG_SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, PG_SIG_IGN);
	pqsignal(SIGCHLD, PG_SIG_DFL);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(InvalidOid, InvalidOid, 0);

	mapwriter_context = AllocSetContextCreate(TopMemoryContext,
												  "Map Writer",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(mapwriter_context);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		EmitErrorReport();

		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		MapAbortBufferIO();
		MapStrategyNotifyWriter(INVALID_PROC_NUMBER);
		MapBackendExitCleanup();
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		MemoryContextSwitchTo(mapwriter_context);
		FlushErrorState();
		MemoryContextReset(mapwriter_context);
		RESUME_INTERRUPTS();

		pg_usleep(1000000L);
		smgrreleaseall();
	}

	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		uint32		recent_alloc = 0;
		int			target_pages = 0;
		int			cleaned = 0;
		int			prealloc_ops = 0;
		bool		can_hibernate = false;

		ResetLatch(MyLatch);
		ProcessMainLoopInterrupts();

		(void) MapSyncStart(NULL, &recent_alloc);
		if (recent_alloc > 0 && MapWriterPreallocMaxRelations > 0)
			prealloc_ops = MapPreallocStep(MapWriterPreallocMaxRelations);

		if (MapWriterMaxPages > 0)
		{
			int			idle_pages;
			double		target_f;

			idle_pages = Max(1, MapWriterMaxPages / 8);
			if (recent_alloc > 0)
			{
				target_f = recent_alloc * MapWriterLRUMultiplier;
				target_pages = (int) (target_f + 0.5);
			}
			else
				target_pages = idle_pages;

			target_pages = Min(MapWriterMaxPages, Max(1, target_pages));
			cleaned = MapBgWriterFlush(target_pages);
		}

		can_hibernate = (recent_alloc == 0 &&
						 cleaned == 0 &&
						 prealloc_ops == 0);

		if (FirstCallSinceLastCheckpoint())
			smgrreleaseall();

		MapStrategyNotifyWriter(MyProcNumber);
		if (WaitLatch(MyLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  MapWriterDelay,
					  WAIT_EVENT_MAPWRITER_MAIN) == WL_TIMEOUT &&
			can_hibernate &&
			prev_hibernate)
		{
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 MapWriterDelay * MAPWRITER_HIBERNATE_FACTOR,
							 WAIT_EVENT_MAPWRITER_HIBERNATE);
		}
		MapStrategyNotifyWriter(INVALID_PROC_NUMBER);
		prev_hibernate = can_hibernate;
	}
}
