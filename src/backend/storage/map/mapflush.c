/*-------------------------------------------------------------------------
 *
 * mapflush.c
 *	  Flush and invalidation for Umbra MAP page and superblock caches.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/umfile.h"

static void MapInvalidateDatabaseInternal(Oid dbid, Oid spcOid);
static void MapSuperFlushMatching(Oid dbid, Oid spcOid);
static MapSuperTag MapSuperMakeTag(RelFileLocatorBackend rlocator);
static bool MapPageTagMatches(const MapPageTag *tag,
							  const RelFileLocatorBackend *rlocator,
							  Oid dbid, Oid spcOid);
static void MapPageFlushMatching(UmbraFileContext *ctx,
								 const RelFileLocatorBackend *rlocator,
								 Oid dbid, Oid spcOid);
static void MapPageInvalidateMatching(const RelFileLocatorBackend *rlocator,
									  Oid dbid, Oid spcOid);
static void MapPageInvalidateSlot(int slot_id,
								  const RelFileLocatorBackend *rlocator,
								  Oid dbid, Oid spcOid);

void
MapInvalidateRelation(RelFileLocatorBackend rlocator)
{
	MapSuperTag tag;

	MapPageInvalidateRelation(rlocator);
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
	MapPageFlushRelation(ctx, rlocator);
	tag = MapSuperMakeTag(rlocator);
	if (MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE, &desc))
	{
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

	/* Catch mappings published while the independent super pool was scanned. */
	MapPageFlushRelation(ctx, rlocator);
}

void
MapFlushDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapPageFlushDatabase(dbid, spcOid);
	MapSuperFlushMatching(dbid, spcOid);
	MapPageFlushDatabase(dbid, spcOid);
}

void
MapCheckpoint(void)
{
	/*
	 * The pools intentionally share no mapping lock.  Revisit ordinary pages
	 * after scanning the independent superblock pool so concurrently published
	 * mappings get another chance to join this checkpoint's write phase.  The
	 * second pass is not a publication barrier, and recovery does not derive EOF
	 * from MAP continuity.  In identity mode, redo can repair a requested
	 * missing mapping as L -> L.  A future L != P update must establish
	 * WAL-before-MAP ordering for the selected physical block.
	 */
	MapPageFlushAll();
	MapSuperFlushMatching(InvalidOid, InvalidOid);
	MapPageFlushAll();
}

void
MapPageFlushRelation(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	Assert(ctx != NULL);
	MapPageFlushMatching(ctx, &rlocator, InvalidOid, InvalidOid);
}

void
MapPageFlushDatabase(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	MapPageFlushMatching(NULL, NULL, dbid, spcOid);
}

void
MapPageFlushAll(void)
{
	MapPageFlushMatching(NULL, NULL, InvalidOid, InvalidOid);
}

void
MapPageInvalidateRelation(RelFileLocatorBackend rlocator)
{
	if (MapPagePoolCtlData == NULL)
		return;
	MapPageInvalidateMatching(&rlocator, InvalidOid, InvalidOid);
}

void
MapPageInvalidateDatabase(Oid dbid, Oid spcOid)
{
	if (MapPagePoolCtlData == NULL)
		return;
	Assert(OidIsValid(dbid));
	MapPageInvalidateMatching(NULL, dbid, spcOid);
}

static void
MapInvalidateDatabaseInternal(Oid dbid, Oid spcOid)
{
	MapSuperTag *tags;
	int			count;
	int			i;

	Assert(OidIsValid(dbid));
	MapPageInvalidateDatabase(dbid, spcOid);
	count = MapSuperCollectTags(dbid, spcOid, &tags);
	for (i = 0; i < count; i++)
		MapSuperDeleteEntry(&tags[i]);
	if (tags != NULL)
		pfree(tags);
}

static void
MapSuperFlushMatching(Oid dbid, Oid spcOid)
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

static bool
MapPageTagMatches(const MapPageTag *tag,
				  const RelFileLocatorBackend *rlocator,
				  Oid dbid, Oid spcOid)
{
	if (rlocator != NULL)
		return RelFileLocatorBackendEquals(tag->rlocator, *rlocator);
	if (OidIsValid(dbid) && tag->rlocator.locator.dbOid != dbid)
		return false;
	if (OidIsValid(spcOid) && tag->rlocator.locator.spcOid != spcOid)
		return false;
	return true;
}

static void
MapPageFlushMatching(UmbraFileContext *ctx,
					 const RelFileLocatorBackend *rlocator,
					 Oid dbid, Oid spcOid)
{
	int			slot_id;

	MapPageEnsureInitialized();
	for (slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
	{
		MapPageDesc *desc = &MapPageDescriptors[slot_id];
		MapPageTag tag;
		uint64		state;

		LWLockAcquire(&desc->content_lock, LW_SHARED);
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_TAG_VALID) != 0)
			tag = desc->tag;
		LWLockRelease(&desc->content_lock);
		if ((state & MAP_PAGE_TAG_VALID) == 0)
			continue;
		if (!MapPageTagMatches(&tag, rlocator, dbid, spcOid))
			continue;

		(void) MapPageFlushBuffer(slot_id, &tag, ctx, false);
	}
}

static void
MapPageInvalidateMatching(const RelFileLocatorBackend *rlocator,
						  Oid dbid, Oid spcOid)
{
	int			slot_id;

	MapPageEnsureInitialized();
	for (slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
		MapPageInvalidateSlot(slot_id, rlocator, dbid, spcOid);
}

static void
MapPageInvalidateSlot(int slot_id,
					  const RelFileLocatorBackend *rlocator,
					  Oid dbid, Oid spcOid)
{
	MapPageDesc *desc = &MapPageDescriptors[slot_id];

	for (;;)
	{
		MapPageTag tag;
		LWLock	   *partition_lock;
		uint32		hashcode;
		uint64		state;

		LWLockAcquire(&desc->io_lock, LW_EXCLUSIVE);
		LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_TAG_VALID) == 0 ||
			!MapPageTagMatches(&desc->tag, rlocator, dbid, spcOid))
		{
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			return;
		}

		tag = desc->tag;
		hashcode = MapPageCacheHashCode(&tag);
		partition_lock = MapPageCachePartitionLock(hashcode);
		LWLockAcquire(partition_lock, LW_EXCLUSIVE);
		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 0 ||
			!MapPageTryClaimBuffer(slot_id))
		{
			LWLockRelease(partition_lock);
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
			continue;
		}

		MapPageCacheDelete(&tag, hashcode, slot_id);
		MemSet(&desc->tag, 0, sizeof(desc->tag));
		pg_atomic_write_u64(&desc->state, 1);
		LWLockRelease(partition_lock);
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		MapPageClockFreeBuffer(slot_id);
		MapPageReleaseClaimBuffer(slot_id);
		return;
	}
}

static MapSuperTag
MapSuperMakeTag(RelFileLocatorBackend rlocator)
{
	MapSuperTag tag = {0};

	tag.rlocator = rlocator;
	return tag;
}
