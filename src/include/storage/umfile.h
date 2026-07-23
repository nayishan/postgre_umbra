/*-------------------------------------------------------------------------
 *
 * umfile.h
 *	  Umbra physical segment file manager declarations.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/umfile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMFILE_H
#define UMFILE_H

#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/sync.h"
#include "storage/um_defs.h"

typedef struct UmbraFileContext UmbraFileContext;

extern void umfile_init(void);
extern UmbraFileContext *umfile_open(RelFileLocatorBackend rlocator);
extern UmbraFileContext *umfile_open_temporary(RelFileLocatorBackend rlocator);
extern void umfile_close(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_destroy(UmbraFileContext *ctx);

extern void umfile_create(UmbraFileContext *ctx, ForkNumber forknum,
						  bool isRedo);
extern bool umfile_exists(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
						  bool isRedo);
extern void umfile_extend(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blocknum, const void *buffer,
						  bool skipFsync);
extern void umfile_zeroextend(UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, int nblocks,
							  bool skipFsync);
extern void umfile_read_bytes(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blocknum, void *buffer, int nbytes);
extern void umfile_write_bytes(UmbraFileContext *ctx, ForkNumber forknum,
						   BlockNumber blocknum, const void *buffer, int nbytes,
						   bool skipFsync);
extern bool umfile_prefetch(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber blocknum, int nblocks);
extern uint32 umfile_maxcombine(UmbraFileContext *ctx, ForkNumber forknum,
								BlockNumber blocknum);
extern void umfile_readv(UmbraFileContext *ctx, ForkNumber forknum,
						 BlockNumber blocknum, void **buffers,
						 BlockNumber nblocks);
extern void umfile_startreadv(PgAioHandle *ioh,
							  UmbraFileContext *ctx, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers,
							  BlockNumber nblocks);
extern void umfile_writev(UmbraFileContext *ctx, ForkNumber forknum,
						  BlockNumber blocknum, const void **buffers,
						  BlockNumber nblocks, bool skipFsync);
extern void umfile_writeback(UmbraFileContext *ctx, ForkNumber forknum,
							 BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber umfile_nblocks(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_truncate(UmbraFileContext *ctx, ForkNumber forknum,
							BlockNumber curnblk, BlockNumber nblocks);
extern void umfile_immedsync(UmbraFileContext *ctx, ForkNumber forknum);
extern void umfile_immedsync_prepared(UmbraFileContext *ctx,
									  ForkNumber forknum);
extern void umfile_registersync(UmbraFileContext *ctx, ForkNumber forknum);
extern int	umfile_fd(UmbraFileContext *ctx, ForkNumber forknum,
					  BlockNumber blocknum, uint32 *off);

extern int	umfilesyncfiletag(const FileTag *ftag, char *path);
extern int	umfileunlinkfiletag(const FileTag *ftag, char *path);
extern bool umfilefiletagmatches(const FileTag *ftag,
								 const FileTag *candidate);

#endif							/* UMFILE_H */
