/*-------------------------------------------------------------------------
 *
 * mapflush.c
 *	  Flush and invalidation for resident MAP superblocks.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/umfile.h"

static void MapInvalidateDatabaseInternal(Oid dbid, Oid spcOid);
static void MapFlushMatching(Oid dbid, Oid spcOid);
static MapSuperTag MapSuperMakeTag(RelFileLocatorBackend rlocator);

void
MapInvalidateRelation(RelFileLocatorBackend rlocator)
{
	MapSuperTag tag;

	if (MapSuperCacheCtlData == NULL)
		return;

	tag = MapSuperMakeTag(rlocator);
	MapSuperDeleteEntry(&tag);
}

void
MapInvalidateDatabase(Oid dbid)
{
	MapInvalidateDatabaseInternal(dbid, InvalidOid);
}

void
MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(spcOid));
	MapInvalidateDatabaseInternal(dbid, spcOid);
}

void
MapFlushRelation(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	MapSuperDesc *desc;
	MapSuperTag tag;

	MapEnsureInitialized();
	tag = MapSuperMakeTag(rlocator);
	if (!MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE, &desc))
		return;

	PG_TRY();
	{
		MapSuperFlushLocked(desc, ctx);
	}
	PG_FINALLY();
	{
		LWLockRelease(&desc->content_lock);
	}
	PG_END_TRY();
}

void
MapFlushDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapFlushMatching(dbid, spcOid);
}

void
MapCheckpoint(void)
{
	MapFlushMatching(InvalidOid, InvalidOid);
}

static void
MapInvalidateDatabaseInternal(Oid dbid, Oid spcOid)
{
	MapSuperTag *tags;
	int			count;
	int			i;

	Assert(OidIsValid(dbid));
	count = MapSuperCollectTags(dbid, spcOid, &tags);
	for (i = 0; i < count; i++)
		MapSuperDeleteEntry(&tags[i]);
	if (tags != NULL)
		pfree(tags);
}

static void
MapFlushMatching(Oid dbid, Oid spcOid)
{
	MapSuperTag *tags;
	int			count;
	int			i;

	count = MapSuperCollectTags(dbid, spcOid, &tags);
	for (i = 0; i < count; i++)
	{
		MapSuperDesc *desc;

		if (!MapSuperFindEntryLocked(&tags[i], LW_EXCLUSIVE, &desc))
			continue;

		PG_TRY();
		{
			MapSuperFlushLocked(desc, NULL);
		}
		PG_FINALLY();
		{
			LWLockRelease(&desc->content_lock);
		}
		PG_END_TRY();
	}
	if (tags != NULL)
		pfree(tags);
}

static MapSuperTag
MapSuperMakeTag(RelFileLocatorBackend rlocator)
{
	MapSuperTag tag = {0};

	tag.rlocator = rlocator;
	return tag;
}
