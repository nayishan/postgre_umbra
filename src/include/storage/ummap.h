/*-------------------------------------------------------------------------
 *
 * ummap.h
 *    Umbra selector-MAP lifecycle.
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMMAP_H
#define UMMAP_H

#include "storage/relfilelocator.h"

typedef struct UmbraFileContext UmbraFileContext;

/* Flush and synchronize selector pages after ordinary data forks are stable. */
extern void ummap_sync_relation_metadata(UmbraFileContext *ctx,
										 RelFileLocatorBackend rlocator);
extern void ummap_checkpoint(void);

#endif							/* UMMAP_H */
