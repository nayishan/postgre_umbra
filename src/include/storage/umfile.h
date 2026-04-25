/*-------------------------------------------------------------------------
 *
 * umfile.h
 *    Umbra file/segment manager (backend-local).
 *
 * This layer owns backend-local file contexts keyed by RelFileLocatorBackend.
 * It provides low-level physical file/segment handling for Umbra forks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef UMFILE_H
#define UMFILE_H

#include "storage/fd.h"
#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/um_defs.h"

typedef struct UmbraFileContext UmbraFileContext;

typedef enum UmFileNblocksMode
{
	UMFILE_NBLOCKS_DENSE,
	UMFILE_NBLOCKS_SPARSE
} UmFileNblocksMode;

typedef enum UmFileExistsMode
{
	UMFILE_EXISTS_DENSE,
	UMFILE_EXISTS_SPARSE
} UmFileExistsMode;

/*
 * Backend-local context registry.
 *
 * umfile owns physical file contexts keyed by RelFileLocatorBackend. smgr and
 * MAP may borrow a context, but umfile is the only owner.
 */
extern UmbraFileContext *umfile_ctx_lookup(RelFileLocatorBackend rlocator);
extern UmbraFileContext *umfile_ctx_acquire(RelFileLocatorBackend rlocator);
extern void umfile_ctx_forget(RelFileLocatorBackend rlocator);
extern void umfile_ctx_close_fork(UmbraFileContext *ctx, ForkNumber forknum);
extern UmbraFileContext *umfile_ctx_create_temporary(RelFileLocatorBackend rlocator);
extern void umfile_ctx_destroy_temporary(UmbraFileContext *ctx);

/*
 * Low-level context I/O helpers for Umbra MAP subsystem.
 *
 * These provide direct physical addressing against fork files without going
 * through smgr mapping translation.
 */
extern bool umfile_ctx_fork_exists(UmbraFileContext *ctx, ForkNumber forknum,
								   UmFileExistsMode mode);
extern BlockNumber umfile_ctx_get_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
										  UmFileNblocksMode mode);
extern void umfile_ctx_read(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
							char *buffer, int nbytes);
extern void umfile_ctx_write(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
							 const char *buffer, int nbytes, bool skipFsync);
extern void umfile_ctx_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno,
							  const char *buffer);
extern bool umfile_ctx_preallocate_blocks(UmbraFileContext *ctx, ForkNumber forknum,
										  UmFileNblocksMode mode,
										  BlockNumber target_nblocks);
extern void umfile_ctx_prefetch(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blkno);
extern bool umfile_ctx_block_exists(UmbraFileContext *ctx, ForkNumber forknum,
									BlockNumber blkno);
extern bool umfile_ctx_segment_exists(UmbraFileContext *ctx, ForkNumber forknum,
									  BlockNumber segno);
extern void umfile_ctx_register_dirty(UmbraFileContext *ctx, ForkNumber forknum,
									  BlockNumber blkno, bool skipFsync,
									  bool isTempRelation);
extern void umfile_ctx_unlinkfork(RelFileLocatorBackend rlocator, ForkNumber forkNum,
								  bool isRedo);

/* lifecycle */
extern void umfile_init(void);

/* smgr-equivalent operations (physical file semantics) */
extern void umfile_create(UmbraFileContext *ctx, ForkNumber forknum, bool isRedo);
extern bool umfile_exists(UmbraFileContext *ctx, ForkNumber forknum,
						  UmFileExistsMode mode);
extern bool umfile_open_or_create(UmbraFileContext *ctx, ForkNumber forknum,
								  bool isRedo, bool *created);
extern void umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
extern void umfile_extend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
						  const void *buffer, bool skipFsync);
extern void umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
							  int nblocks, bool skipFsync);
extern bool umfile_prefetch(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum, int nblocks);
extern uint32 umfile_maxcombine(ForkNumber forknum, BlockNumber blocknum);
extern void umfile_readv(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks);
extern void umfile_startreadv(PgAioHandle *ioh, UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers, BlockNumber nblocks);
/*
 * Start an async read using physical addressing, while preserving the logical
 * identity (block number) for error reporting and reopen semantics.
 *
 * This is used by Umbra's MAP translation: the file/offset are based on the
 * physical block number, but smgr target identity remains logical.
 */
extern void umfile_startreadv_physical(PgAioHandle *ioh, UmbraFileContext *ctx,
						   ForkNumber forknum,
						   BlockNumber logical_blocknum,
						   BlockNumber physical_blocknum,
						   void **buffers, BlockNumber nblocks);
extern void umfile_writev(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum,
						  const void **buffers, BlockNumber nblocks, bool skipFsync);
extern void umfile_writeback(UmbraFileContext *ctx, ForkNumber forknum,
							 BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
								  UmFileNblocksMode mode);
extern void umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber old_blocks, BlockNumber nblocks);
extern void umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum);
extern int umfile_fd(UmbraFileContext *ctx, ForkNumber forknum, BlockNumber blocknum, uint32 *off);

#endif							/* UMFILE_H */
