/*-------------------------------------------------------------------------
 *
 * ummap.h
 *	  Umbra-private metadata root lifecycle.
 *
 * The root format and its interpretation are private to ummap.c.  This
 * header exposes only the lifecycle hooks used by Umbra's smgr layer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMMAP_H
#define UMMAP_H

#include "access/xlogdefs.h"
#include "storage/relfilelocator.h"
#include "storage/shmem.h"

typedef struct UmbraFileContext UmbraFileContext;

/*
 * The slot0 names below expose root-format activation bits.  Activation
 * enables the mapped three-slot layout whose zero selectors initially choose
 * slot 0; individual MAIN, FSM, and VM pages may later select slots 1 or 2.
 */

extern void ummap_root_cache_backend_init(void);

extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx,
					 RelFileLocatorBackend rlocator, bool isRedo);
extern bool ummap_main_slot0_active(UmbraFileContext *ctx,
							RelFileLocatorBackend rlocator);
extern bool ummap_try_main_slot0_active(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator, bool *active);
extern void ummap_activate_main_slot0(UmbraFileContext *ctx,
							  RelFileLocatorBackend rlocator,
							  XLogRecPtr activation_lsn);
extern BlockNumber ummap_get_main_frontier(UmbraFileContext *ctx,
										RelFileLocatorBackend rlocator);
extern void ummap_set_main_frontier(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator,
								BlockNumber logical_eof);
extern void ummap_prepare_main_frontier(UmbraFileContext *ctx,
									RelFileLocatorBackend rlocator);
extern void ummap_publish_prepared_main_frontier(UmbraFileContext *ctx,
											 RelFileLocatorBackend rlocator,
											 BlockNumber logical_eof);
extern bool ummap_aux_slot0_active(UmbraFileContext *ctx,
										ForkNumber forknum,
									RelFileLocatorBackend rlocator);
extern bool ummap_try_aux_slot0_active(UmbraFileContext *ctx,
										ForkNumber forknum,
										RelFileLocatorBackend rlocator,
										bool *active);
extern void ummap_activate_aux_slot0(UmbraFileContext *ctx,
									 ForkNumber forknum,
									 RelFileLocatorBackend rlocator);
extern BlockNumber ummap_get_aux_frontier(UmbraFileContext *ctx,
										ForkNumber forknum,
										RelFileLocatorBackend rlocator);
extern void ummap_set_aux_frontier(UmbraFileContext *ctx,
								ForkNumber forknum,
								RelFileLocatorBackend rlocator,
								BlockNumber logical_eof);
extern void ummap_prepare_aux_frontier(UmbraFileContext *ctx,
									ForkNumber forknum,
									RelFileLocatorBackend rlocator);
extern void ummap_publish_prepared_aux_frontier(UmbraFileContext *ctx,
											ForkNumber forknum,
											RelFileLocatorBackend rlocator,
											BlockNumber logical_eof);
extern void ummap_validate_if_exists(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
extern void ummap_immedsync_if_exists(UmbraFileContext *ctx,
							  RelFileLocatorBackend rlocator);
extern void ummap_registersync_if_exists(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator);
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);

extern void ummap_flush_relation(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
extern void ummap_flush_database_tablespace(Oid dbid, Oid spcOid);
extern void ummap_invalidate_database(Oid dbid);
extern void ummap_invalidate_database_tablespace(Oid dbid, Oid spcOid);
extern void ummap_checkpoint(void);

extern const ShmemCallbacks UmbraMapRootShmemCallbacks;

#endif							/* UMMAP_H */
