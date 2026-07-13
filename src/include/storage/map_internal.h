/*-------------------------------------------------------------------------
 *
 * map_internal.h
 *	  Internal declarations for the Umbra MAP metadata cache.
 *
 * src/include/storage/map_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_INTERNAL_H
#define MAP_INTERNAL_H

#include "lib/dshash.h"
#include "storage/map.h"
#include "storage/ummap.h"

typedef struct MapSuperTag
{
	RelFileLocatorBackend rlocator;
} MapSuperTag;

struct MapSuperDesc
{
	MapSuperTag tag;
	LWLock		content_lock;
	bool		valid;
	bool		dirty;
	char		data[UMMAP_SUPERBLOCK_SIZE];
};

typedef struct MapSuperCacheCtl
{
	void	   *raw_dsa_area;
	dshash_table_handle hash_handle;
} MapSuperCacheCtl;

extern MapSuperCacheCtl *MapSuperCacheCtlData;

extern void MapEnsureInitialized(void);
extern bool MapSuperFindEntryLocked(const MapSuperTag *tag, LWLockMode mode,
									MapSuperDesc **desc);
extern void MapSuperDeleteEntry(const MapSuperTag *tag);
extern void MapSuperFlushLocked(MapSuperDesc *desc, UmbraFileContext *ctx);
extern int MapSuperCollectTags(Oid dbid, Oid spcOid, MapSuperTag **tags);
extern void MapSuperTableShmemRequest(void);
extern void MapSuperTableShmemInit(void);
extern void MapSuperTableShmemAttach(void);

#endif							/* MAP_INTERNAL_H */
