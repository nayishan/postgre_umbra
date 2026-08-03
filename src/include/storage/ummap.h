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
extern void ummap_validate_if_exists(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator);
extern void ummap_immedsync_if_exists(UmbraFileContext *ctx,
							  RelFileLocatorBackend rlocator);
extern void ummap_registersync_if_exists(UmbraFileContext *ctx,
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
