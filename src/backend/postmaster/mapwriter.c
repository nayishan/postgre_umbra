/*-------------------------------------------------------------------------
 *
 * mapwriter.c
 *    Umbra MAP background writer.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/mapwriter.h"
#include "storage/latch.h"
#include "storage/map.h"
#include "storage/procsignal.h"
#include "utils/wait_event.h"

int			MapWriterDelay = 200;
int			MapWriterMaxPages = 100;

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
	snprintf(worker.bgw_name, BGW_MAXLEN, "Umbra map writer");
	snprintf(worker.bgw_type, BGW_MAXLEN, "map writer");
	worker.bgw_restart_time = 5;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&worker);
}

void
MapWriterMain(Datum arg)
{
	(void) arg;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, PG_SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(InvalidOid, InvalidOid, 0);

	for (;;)
	{
		ResetLatch(MyLatch);
		ProcessMainLoopInterrupts();

		if (MapWriterMaxPages > 0)
			(void) MapPageBgWriterFlush(MapWriterMaxPages);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 MapWriterDelay,
						 WAIT_EVENT_MAPWRITER_MAIN);
	}
}
