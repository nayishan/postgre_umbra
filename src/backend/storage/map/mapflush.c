/*-------------------------------------------------------------------------
 *
 * mapflush.c
 *	  Flush and invalidation for Umbra MAP page and root buffers.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapflush.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/map.h"
#include "storage/map_internal.h"

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
static MapSuperTag MapSuperMakeTag(RelFileLocatorBackend rlocator);
static void MapSuperFlushMatching(Oid dbid, Oid spcOid);
static void MapSuperInvalidateMatching(Oid dbid, Oid spcOid);
static int MapPageBgWriterNextSlot = 0;

void
MapInvalidateRelation(RelFileLocatorBackend rlocator)
{
	MapReclaimForgetRelation(rlocator);
	MapPageInvalidateRelation(rlocator);
	if (MapSuperCacheCtlData != NULL)
	{
		MapSuperTag tag = MapSuperMakeTag(rlocator);

		MapSuperDeleteEntry(&tag);
	}
}

void
MapInvalidateDatabase(Oid dbid)
{
	MapReclaimForgetDatabase(dbid, InvalidOid);
	MapPageInvalidateDatabase(dbid, InvalidOid);
	MapSuperInvalidateMatching(dbid, InvalidOid);
}

void
MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(spcOid));
	MapReclaimForgetDatabase(dbid, spcOid);
	MapPageInvalidateDatabase(dbid, spcOid);
	MapSuperInvalidateMatching(dbid, spcOid);
}

void
MapFlushRelation(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	MapSuperDesc *desc;
	MapSuperTag tag;

	MapPageFlushRelation(ctx, rlocator);
	if (MapSuperCacheCtlData == NULL)
		return;

	MapSuperEnsureInitialized();
	tag = MapSuperMakeTag(rlocator);
	if (!MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE, &desc))
		return;
	MapSuperFlushLocked(desc, ctx);
	LWLockRelease(&desc->content_lock);
}

void
MapFlushDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapPageFlushDatabase(dbid, spcOid);
	MapSuperFlushMatching(dbid, spcOid);
}

void
MapCheckpoint(void)
{
	MapPageFlushAll();
	MapSuperFlushMatching(InvalidOid, InvalidOid);

	/*
	 * A non-WAL MAP update can dirty a page after the first scan has passed
	 * its slot, then advance a root that this checkpoint writes.  Scan again
	 * so a completed checkpoint cannot leave that root ahead of canonical MAP.
	 */
	MapPageFlushAll();
}

int
MapPageBgWriterFlush(int max_pages)
{
	int			cleaned = 0;
	int			scanned = 0;

	if (max_pages <= 0)
		return 0;
	MapPageEnsureInitialized();

	while (scanned < MapPageBufferCount && cleaned < max_pages)
	{
		MapPageDesc *desc;
		uint64		before;
		int			slot_id = MapPageBgWriterNextSlot;

		MapPageBgWriterNextSlot = (MapPageBgWriterNextSlot + 1) %
			MapPageBufferCount;
		scanned++;
		desc = &MapPageDescriptors[slot_id];
		before = pg_atomic_read_u64(&desc->state);
		if ((before & MAP_PAGE_DIRTY) == 0)
			continue;
		if (MapPageFlushBuffer(slot_id, NULL, NULL, true) &&
			(pg_atomic_read_u64(&desc->state) & MAP_PAGE_DIRTY) == 0)
			cleaned++;
	}

	return cleaned;
}

static MapSuperTag
MapSuperMakeTag(RelFileLocatorBackend rlocator)
{
	MapSuperTag tag = {0};

	tag.rlocator = rlocator;
	return tag;
}

static void
MapSuperFlushMatching(Oid dbid, Oid spcOid)
{
	MapSuperTag *tags;
	int			count;
	int			i;

	if (MapSuperCacheCtlData == NULL)
		return;

	count = MapSuperCollectTags(dbid, spcOid, &tags);
	for (i = 0; i < count; i++)
	{
		MapSuperDesc *desc;

		if (!MapSuperFindEntryLocked(&tags[i], LW_EXCLUSIVE, &desc))
			continue;
		MapSuperFlushLocked(desc, NULL);
		LWLockRelease(&desc->content_lock);
	}
	if (tags != NULL)
		pfree(tags);
}

static void
MapSuperInvalidateMatching(Oid dbid, Oid spcOid)
{
	MapSuperTag *tags;
	int			count;
	int			i;

	if (MapSuperCacheCtlData == NULL)
		return;

	count = MapSuperCollectTags(dbid, spcOid, &tags);
	for (i = 0; i < count; i++)
		MapSuperDeleteEntry(&tags[i]);
	if (tags != NULL)
		pfree(tags);
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
		MapPageTag	tag;
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
		MapPageTag	tag;
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
		Assert(desc->pending_pin_refs == 0);
		MemSet(&desc->tag, 0, sizeof(desc->tag));
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		MemSet(desc->pending_bits, 0, sizeof(desc->pending_bits));
		MemSet(&desc->pending_range, 0, sizeof(desc->pending_range));
		MemSet(&desc->replay_range, 0, sizeof(desc->replay_range));
		pg_atomic_write_u64(&desc->state, 1);
		LWLockRelease(partition_lock);
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		MapPageClockFreeBuffer(slot_id);
		MapPageReleaseClaimBuffer(slot_id);
		return;
	}
}
