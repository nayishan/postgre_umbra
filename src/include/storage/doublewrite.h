/*-------------------------------------------------------------------------
 *
 * doublewrite.h
 *    Crash-safe shadow copies for ordinary relation page writes.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DOUBLEWRITE_H
#define DOUBLEWRITE_H

#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/guc.h"

#define PG_DOUBLEWRITE_DIR "pg_doublewrite"

extern PGDLLIMPORT bool enableDoubleWrite;

extern bool check_double_write(bool *newval, void **extra, GucSource source);
extern bool check_fsync_for_double_write(bool *newval, void **extra,
									 GucSource source);

extern bool DoubleWriteEnabled(void);
extern bool DoubleWriteBeginWrite(SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, const void **buffers,
							  BlockNumber nblocks);
extern void DoubleWriteEndWrite(void);

extern void DoubleWriteStartup(void);
extern void DoubleWriteCheckpointBegin(void);
extern void DoubleWriteCheckpointComplete(bool remove_active_generation);

#endif							/* DOUBLEWRITE_H */
