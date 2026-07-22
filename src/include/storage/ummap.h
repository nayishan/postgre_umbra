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
extern void ummap_flush_database_tablespace(Oid dbid, Oid spcOid);
extern void ummap_invalidate_database(Oid dbid);
extern void ummap_invalidate_database_tablespace(Oid dbid, Oid spcOid);
extern void ummap_checkpoint(void);

extern const ShmemCallbacks UmbraMapRootShmemCallbacks;

#endif							/* UMMAP_H */
