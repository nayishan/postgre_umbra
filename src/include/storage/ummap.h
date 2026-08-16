/*-------------------------------------------------------------------------
 *
 * ummap.h
 *	  Umbra selector-MAP lifecycle.
 *
 * The selector MAP holds only active-slot values. It is not relation-level
 * layout or extent authority.
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
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);
/* Flush selector pages before a raw file copy. */
extern void ummap_flush_database_tablespace_cache(Oid dbid, Oid spcOid);
/* Discard a database's selector cache state without performing I/O. */
extern void ummap_invalidate_database_cache(Oid dbid);
/* Discard one tablespace's selector cache state without I/O. */
extern void ummap_invalidate_database_tablespace_cache(Oid dbid, Oid spcOid);
extern void ummap_checkpoint(void);

#endif							/* UMMAP_H */
