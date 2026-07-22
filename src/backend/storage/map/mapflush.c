/*-------------------------------------------------------------------------
 *
 * mapflush.c
 *    Flush and invalidation for Umbra selector-page buffers.
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
static int MapPageBgWriterNextSlot = 0;

void
MapInvalidateRelation(RelFileLocatorBackend rlocator)
{
	MapPageInvalidateRelation(rlocator);
}

void
MapInvalidateDatabase(Oid dbid)
{
	MapPageInvalidateDatabase(dbid, InvalidOid);
}

void
MapInvalidateDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(spcOid));
	MapPageInvalidateDatabase(dbid, spcOid);
}

void
MapFlushRelation(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	MapPageFlushRelation(ctx, rlocator);
}

void
MapFlushDatabaseTablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapPageFlushDatabase(dbid, spcOid);
}

void
MapCheckpoint(void)
{
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
	if (MapPageBufferCount == 0)
		return 0;

	while (scanned < MapPageBufferCount && cleaned < max_pages)
	{
		MapPageDesc *desc;
		uint64		state;
		int			slot_id = MapPageBgWriterNextSlot;

		MapPageBgWriterNextSlot = (MapPageBgWriterNextSlot + 1) %
			MapPageBufferCount;
		scanned++;
		desc = &MapPageDescriptors[slot_id];
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_DIRTY) == 0)
			continue;
		if (MapPageFlushBuffer(slot_id, NULL, NULL, true) &&
			(pg_atomic_read_u64(&desc->state) & MAP_PAGE_DIRTY) == 0)
			cleaned++;
	}

	return cleaned;
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
	int         slot_id;

	if (MapPagePoolCtlData == NULL)
		return;
	MapPageEnsureInitialized();
	for (slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
	{
		MapPageDesc *desc = &MapPageDescriptors[slot_id];
		MapPageTag tag;
		uint64      state;

		LWLockAcquire(&desc->content_lock, LW_SHARED);
		state = pg_atomic_read_u64(&desc->state);
		if ((state & MAP_PAGE_TAG_VALID) != 0)
			tag = desc->tag;
		LWLockRelease(&desc->content_lock);
		if ((state & MAP_PAGE_TAG_VALID) == 0 ||
			!MapPageTagMatches(&tag, rlocator, dbid, spcOid))
			continue;
		(void) MapPageFlushBuffer(slot_id, &tag, ctx, false);
	}
}

static void
MapPageInvalidateMatching(const RelFileLocatorBackend *rlocator,
						  Oid dbid, Oid spcOid)
{
	int         slot_id;

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
		uint32      hashcode;
		uint64      state;

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
		LWLockAcquire(MapPageCachePartitionLock(hashcode), LW_EXCLUSIVE);
		state = pg_atomic_read_u64(&desc->state);
		if (MAP_PAGE_GET_REFCOUNT(state) != 0 ||
			!MapPageTryClaimBuffer(slot_id))
		{
			LWLockRelease(MapPageCachePartitionLock(hashcode));
			LWLockRelease(&desc->content_lock);
			LWLockRelease(&desc->io_lock);
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
			continue;
		}

		MapPageCacheDelete(&tag, hashcode, slot_id);
		MemSet(&desc->tag, 0, sizeof(desc->tag));
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		pg_atomic_write_u64(&desc->state, 1);
		LWLockRelease(MapPageCachePartitionLock(hashcode));
		LWLockRelease(&desc->content_lock);
		LWLockRelease(&desc->io_lock);
		MapPageClockFreeBuffer(slot_id);
		MapPageReleaseClaimBuffer(slot_id);
		return;
	}
}
