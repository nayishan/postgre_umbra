/*-------------------------------------------------------------------------
 *
 * ummap.h
 *	  Umbra private map fork declarations.
 *
 * This header describes Umbra's relation-local private map fork container.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/storage/ummap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMMAP_H
#define UMMAP_H

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

typedef struct UmbraFileContext UmbraFileContext;

/*
 * Umbra-private relation-local map fork.
 *
 * This is not a PostgreSQL-visible ForkNumber.  It is only a private slot and
 * path convention used below the Umbra smgr implementation.
 */
#define UMBRA_MAP_FORKNUM	((ForkNumber) (MAX_FORKNUM + 1))
#define UMBRA_NUM_FORKS		((int) UMBRA_MAP_FORKNUM + 1)

extern RelPathStr ummap_relpath(RelFileLocatorBackend rlocator);

extern bool ummap_tracks_fork(ForkNumber forknum);
extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx, bool isRedo);
extern void ummap_close(UmbraFileContext *ctx);
extern void ummap_immedsync_if_exists(UmbraFileContext *ctx);
extern void ummap_registersync_if_exists(UmbraFileContext *ctx);
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);
extern void ummap_init_fork(UmbraFileContext *ctx, ForkNumber forknum,
							bool skipFsync);
extern bool ummap_fork_exists(UmbraFileContext *ctx, ForkNumber forknum);
extern BlockNumber ummap_nblocks(UmbraFileContext *ctx, ForkNumber forknum);
extern bool ummap_set_nblocks(UmbraFileContext *ctx, ForkNumber forknum,
								  BlockNumber nblocks, bool skipFsync);

extern BlockNumber ummap_lookup_block(UmbraFileContext *ctx,
									  ForkNumber forknum,
									  BlockNumber lblkno);
extern BlockNumber ummap_lookup_run(UmbraFileContext *ctx,
									ForkNumber forknum,
									BlockNumber lblkno,
									BlockNumber maxblocks,
									BlockNumber *pblkno);
extern BlockNumber ummap_identity_run_limit(ForkNumber forknum,
											BlockNumber lblkno,
											BlockNumber maxblocks,
											BlockNumber *pblkno);
extern BlockNumber ummap_set_identity_block(UmbraFileContext *ctx,
											ForkNumber forknum,
											BlockNumber lblkno,
											bool skipFsync);
extern BlockNumber ummap_set_identity_run(UmbraFileContext *ctx,
										  ForkNumber forknum,
										  BlockNumber lblkno,
										  BlockNumber maxblocks,
										  BlockNumber *pblkno,
										  bool skipFsync);

#endif							/* UMMAP_H */
