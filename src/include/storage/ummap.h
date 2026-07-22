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

typedef struct UmbraFileContext UmbraFileContext;

extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx, bool isRedo);
extern void ummap_validate_if_exists(UmbraFileContext *ctx);
extern void ummap_immedsync_if_exists(UmbraFileContext *ctx);
extern void ummap_registersync_if_exists(UmbraFileContext *ctx);
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);

#endif							/* UMMAP_H */
