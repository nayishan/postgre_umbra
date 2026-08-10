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

extern void ummap_root_cache_backend_init(void);

extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx,
					 RelFileLocatorBackend rlocator, bool isRedo);
/* Nonfatal on-disk root probe used by recovery and layout discovery. */
extern bool ummap_try_validate(UmbraFileContext *ctx);
extern void ummap_get_main_frontiers(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 BlockNumber *logical_eof,
							 BlockNumber *physical_capacity);
extern void ummap_set_main_frontiers(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator,
							 BlockNumber logical_eof,
							 BlockNumber physical_capacity);
extern void ummap_prepare_main_frontiers(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator);
extern void ummap_publish_prepared_main_frontiers(UmbraFileContext *ctx,
									  RelFileLocatorBackend rlocator,
									  BlockNumber logical_eof,
									  BlockNumber physical_capacity);
extern void ummap_validate_if_exists(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator);
/*
 * Write and synchronize only relation-level MAP metadata.  The caller must
 * already have established ordinary data-fork and selector-page ordering.
 */
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
