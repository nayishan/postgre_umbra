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

#include "storage/relfilelocator.h"
#include "storage/shmem.h"

typedef struct UmbraFileContext UmbraFileContext;

/*
 * A valid root enables MAIN mapping.  The auxiliary slot0 names below expose
 * the FSM and VM activation bits; zero selectors initially choose slot 0, and
 * individual mapped pages may later select slots 1 or 2.
 */

extern void ummap_root_cache_backend_init(void);

extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx,
					 RelFileLocatorBackend rlocator, bool isRedo);
/* Nonfatal on-disk root probe used by recovery and layout discovery. */
extern bool ummap_try_validate(UmbraFileContext *ctx);
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
extern void ummap_sync_relation_metadata(UmbraFileContext *ctx,
								  RelFileLocatorBackend rlocator);
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);

extern void ummap_flush_relation(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
/* Flush the private metadata-root cache before a raw file copy. */
extern void ummap_flush_database_tablespace_cache(Oid dbid, Oid spcOid);
/* Discard a database's metadata-root cache state without performing I/O. */
extern void ummap_invalidate_database_cache(Oid dbid);
/* Discard one tablespace's metadata-root cache state without I/O. */
extern void ummap_invalidate_database_tablespace_cache(Oid dbid, Oid spcOid);
extern void ummap_checkpoint(void);

extern const ShmemCallbacks UmbraMapRootShmemCallbacks;

#endif							/* UMMAP_H */
