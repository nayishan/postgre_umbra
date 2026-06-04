/*-------------------------------------------------------------------------
 *
 * mapcompactor.c
 *	  Umbra map compactor background worker.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/mapcompactor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xact.h"
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
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#define MAPCOMPACTOR_HIBERNATE_FACTOR 50

int			MapCompactorDelay = 200;
int			MapCompactorMaxRelations = 8;
int			MapCompactorBusyAllocThreshold = 128;

static void
MapCompactorExitCallback(int code, Datum arg)
{
	(void) code;
	(void) arg;
	MapStrategyNotifyCompactor(INVALID_PROC_NUMBER);
}

void
MapCompactorMain(Datum arg)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext mapcompactor_context;
	bool		prev_hibernate = false;

	(void) arg;
	before_shmem_exit(MapCompactorExitCallback, 0);

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

	mapcompactor_context = AllocSetContextCreate(TopMemoryContext,
													 "Map Compactor",
												 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(mapcompactor_context);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		EmitErrorReport();

		AbortOutOfAnyTransaction();
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		MapBackendExitCleanup();
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		MemoryContextSwitchTo(mapcompactor_context);
		FlushErrorState();
		MemoryContextReset(mapcompactor_context);
		RESUME_INTERRUPTS();

		pg_usleep(1000000L);
		smgrreleaseall();
	}

	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		int			compact_moves = 0;
		uint32		alloc_pressure = 0;
		bool		busy_round = false;

		ResetLatch(MyLatch);
		ProcessMainLoopInterrupts();

		alloc_pressure = MapAllocPressurePeek();
		busy_round = (MapCompactorBusyAllocThreshold > 0 &&
					  alloc_pressure >= (uint32) MapCompactorBusyAllocThreshold);

		if (!busy_round && MapCompactorMaxRelations > 0)
		{
			PG_TRY();
			{
				StartTransactionCommand();
				compact_moves = MapCompactorStep(MapCompactorMaxRelations);
				CommitTransactionCommand();
				MemoryContextSwitchTo(mapcompactor_context);
			}
			PG_CATCH();
			{
				HOLD_INTERRUPTS();
				EmitErrorReport();
				AbortOutOfAnyTransaction();
				pgstat_report_wait_end();
				MapBackendExitCleanup();
				FlushErrorState();
				MemoryContextSwitchTo(mapcompactor_context);
				MemoryContextReset(mapcompactor_context);
				RESUME_INTERRUPTS();
			}
			PG_END_TRY();
		}

		if (FirstCallSinceLastCheckpoint())
			smgrreleaseall();

		MapStrategyNotifyCompactor(MyProcNumber);
		if (busy_round)
		{
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 MapCompactorDelay * MAPCOMPACTOR_HIBERNATE_FACTOR,
							 WAIT_EVENT_MAPCOMPACTOR_HIBERNATE);
		}
		else if (WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   MapCompactorDelay,
						   WAIT_EVENT_MAPCOMPACTOR_MAIN) == WL_TIMEOUT &&
				 compact_moves == 0 &&
				 prev_hibernate)
		{
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 MapCompactorDelay * MAPCOMPACTOR_HIBERNATE_FACTOR,
							 WAIT_EVENT_MAPCOMPACTOR_HIBERNATE);
		}
		MapStrategyNotifyCompactor(INVALID_PROC_NUMBER);
		prev_hibernate = (compact_moves == 0 || busy_round);
	}
}
