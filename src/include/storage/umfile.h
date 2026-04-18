/*-------------------------------------------------------------------------
 *
 * umfile.h
 *	  Umbra backend-local file/context helpers.
 *
 * This layer owns backend-local file contexts keyed by RelFileLocatorBackend.
 * It is the low-level file access boundary beneath Umbra metadata and mapping
 * code.
 *
 * src/include/storage/umfile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMFILE_H
#define UMFILE_H

#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/fd.h"
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

extern void umfile_init(void);

extern UmbraFileContext *umfile_ctx_lookup(RelFileLocatorBackend rlocator);
extern UmbraFileContext *umfile_ctx_acquire(RelFileLocatorBackend rlocator);
extern UmbraFileContext *umfile_ctx_create_temporary(RelFileLocatorBackend rlocator);
extern void umfile_ctx_destroy_temporary(UmbraFileContext *ctx);
extern void umfile_ctx_release(RelFileLocatorBackend rlocator);
extern void umfile_ctx_forget(RelFileLocatorBackend rlocator);
extern void umfile_ctx_close_fork(UmbraFileContext *ctx, ForkNumber forknum);

extern bool umfile_ctx_fork_exists(UmbraFileContext *ctx, ForkNumber forknum,
								   UmFileExistsMode mode);
extern BlockNumber umfile_ctx_get_nblocks(UmbraFileContext *ctx,
										  ForkNumber forknum,
										  UmFileNblocksMode mode);
extern void umfile_ctx_read(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber blkno, char *buffer, int nbytes);
extern void umfile_ctx_write(UmbraFileContext *ctx, ForkNumber forknum,
							 BlockNumber blkno, const char *buffer,
							 int nbytes, bool skipFsync);
extern void umfile_ctx_extend(UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blkno, const char *buffer);
extern void umfile_ctx_unlinkfork(RelFileLocatorBackend rlocator,
								  ForkNumber forknum, bool isRedo);

extern bool umfile_exists(UmbraFileContext *ctx, ForkNumber forknum,
						  UmFileExistsMode mode);
extern bool umfile_open_or_create(UmbraFileContext *ctx, ForkNumber forknum,
								  bool isRedo, bool *created);
extern BlockNumber umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
								  UmFileNblocksMode mode);
extern void umfile_readv(UmbraFileContext *ctx, ForkNumber forknum,
						 BlockNumber blocknum, void **buffers,
						 BlockNumber nblocks);
extern void umfile_writev(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blocknum, const void **buffers,
						  BlockNumber nblocks, bool skipFsync);
extern void umfile_extend(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blocknum, const void *buffer,
						  bool skipFsync);
extern void umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, int nblocks,
							  bool skipFsync);
extern void umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber old_blocks, BlockNumber nblocks);
extern void umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
						  bool isRedo);

/* Metadata-only convenience wrappers over the generic umfile surface. */
extern bool umfile_metadata_exists(UmbraFileContext *ctx);
extern bool umfile_metadata_open_or_create(UmbraFileContext *ctx,
										   bool isRedo, bool *created);
extern BlockNumber umfile_metadata_nblocks(UmbraFileContext *ctx);
extern void umfile_metadata_read(UmbraFileContext *ctx, BlockNumber blkno,
								 void *buffer);
extern void umfile_metadata_write(UmbraFileContext *ctx, BlockNumber blkno,
								  const void *buffer);
extern void umfile_metadata_extend(UmbraFileContext *ctx, BlockNumber blkno,
								   const void *buffer);
extern void umfile_metadata_immedsync(UmbraFileContext *ctx);
extern void umfile_metadata_unlink(RelFileLocatorBackend rlocator, bool isRedo);

#endif							/* UMFILE_H */
