/*-------------------------------------------------------------------------
 *
 * assert.c
 *	  Assert code.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/assert.c
 *
 * NOTE
 *	  This should eventually work with elog()
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <execinfo.h>

static void
print_stack_trace_on_assert(void);
/*
 * ExceptionalCondition - Handles the failure of an Assert()
 */
void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	print_stack_trace_on_assert();
	if (!PointerIsValid(conditionName)
		|| !PointerIsValid(fileName)
		|| !PointerIsValid(errorType))
		write_stderr("TRAP: ExceptionalCondition: bad arguments\n");
	else
	{
		write_stderr("TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
					 errorType, conditionName,
					 fileName, lineNumber);
	}

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

#ifdef SLEEP_ON_ASSERT

	/*
	 * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
	 * minutes, which seems too short.
	 */
	sleep(1000000);
#endif

	abort();
}
/*
 * 我们的自定义函数，用于在断言失败时打印堆栈轨迹。
 */
static void
print_stack_trace_on_assert(void)
{
    void *buffer[100];
    int nptrs;
    char **strings;

    /* 获取当前线程的调用栈地址 */
    nptrs = backtrace(buffer, 100);

    /* 将地址转换为可读的函数名字符串 */
    strings = backtrace_symbols(buffer, nptrs);

    if (strings == NULL)
    {
        elog(WARNING, "could not get stack trace: backtrace_symbols failed");
        return;
    }

    /* 将堆栈信息逐行打印到 PostgreSQL 的日志中 */
    elog(WARNING, "--- BEGIN STACK TRACE ---");
    for (int i = 0; i < nptrs; i++)
    {
        /* 使用 WARNING 级别，这样它会打印到日志但不会立即终止进程 */
        elog(WARNING, "  %s", strings[i]);
    }
    elog(WARNING, "--- END STACK TRACE ---");

    /* 释放 backtrace_symbols 分配的内存 */
    free(strings);
}
