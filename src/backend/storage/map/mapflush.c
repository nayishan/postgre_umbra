/*-------------------------------------------------------------------------
 *
 * mapflush.c
 *	  MAP checkpoint and writeback implementation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper_internal.h"
#include "storage/umbra.h"
#include "storage/umfile.h"

typedef struct MapFlushWriteCache
{
	bool				valid;
	RelFileLocatorBackend rlocator;
	UmbraFileContext   *ctx;
} MapFlushWriteCache;

typedef struct MapFlushBufferTarget
{
	int				slot_id;
	RelFileLocator	rnode;
	BlockNumber		map_blkno;
} MapFlushBufferTarget;

static void MapFlushWriteCacheReset(MapFlushWriteCache *cache);
static UmbraFileContext *MapFlushContextFor(MapFlushWriteCache *cache,
											RelFileLocator rnode);
static void MapFlushWritePage(RelFileLocatorBackend rlocator,
							  UmbraFileContext *ctx,
							  BlockNumber map_blkno,
							  const void *page,
							  XLogRecPtr page_lsn);
static void MapFlushWriteSuperblockEntry(RelFileLocator rnode,
										 MapSuperEntry *entry);
static int MapCollectDirtyBufferTargets(MapFlushBufferTarget **targets_out,
										const RelFileLocator *filter_rnode,
										bool mark_checkpoint_needed);
static int MapFlushDirtyBuffers(int max_pages, bool checkpoint);
static int MapFlushRelationBuffers(RelFileLocator rnode, bool checkpoint);
static int MapFlushDirtySuperblocks(void);
static int MapFlushRelationSuperblocks(RelFileLocator rnode);
static void MapFlushBufferCached(int slot_id, MapFlushWriteCache *write_cache,
								 bool checkpoint);
static bool MapTablespaceSelected(Oid spcOid, int ntablespaces,
								  const Oid *tablespace_ids);
static inline int map_flush_buffer_target_comparator(
	const MapFlushBufferTarget *a,
	const MapFlushBufferTarget *b);

#define ST_SORT sort_map_flush_buffer_targets
#define ST_ELEMENT_TYPE MapFlushBufferTarget
#define ST_COMPARE(a, b) map_flush_buffer_target_comparator(a, b)
#define ST_SCOPE static
#define ST_DEFINE
#include "lib/sort_template.h"

static void
MapFlushWriteCacheReset(MapFlushWriteCache *cache)
{
	if (cache == NULL || !cache->valid)
		return;

	umfile_ctx_destroy_temporary(cache->ctx);
	cache->ctx = NULL;
	cache->valid = false;
	memset(&cache->rlocator, 0, sizeof(cache->rlocator));
}

static UmbraFileContext *
MapFlushContextFor(MapFlushWriteCache *cache, RelFileLocator rnode)
{
	Assert(cache != NULL);

	if (cache->valid && RelFileLocatorEquals(cache->rlocator.locator, rnode))
		return cache->ctx;

	MapFlushWriteCacheReset(cache);

	cache->rlocator.locator = rnode;
	cache->rlocator.backend = INVALID_PROC_NUMBER;
	cache->ctx = umfile_ctx_create_temporary(cache->rlocator);
	cache->valid = true;
	return cache->ctx;
}

static void
MapFlushWritePage(RelFileLocatorBackend rlocator, UmbraFileContext *ctx,
				  BlockNumber map_blkno, const void *page,
				  XLogRecPtr page_lsn)
{
	Assert(ctx != NULL);
	Assert(page != NULL);
	Assert(map_blkno != MAP_BLOCK_SUPER);
	Assert(umfile_ctx_fork_exists(ctx, UMBRA_METADATA_FORKNUM));

	if (!InRecovery && page_lsn != InvalidXLogRecPtr)
		XLogFlush(page_lsn);

	umfile_ctx_write(ctx, UMBRA_METADATA_FORKNUM, map_blkno,
					 page, BLCKSZ, false);
	umfile_ctx_register_dirty(ctx, UMBRA_METADATA_FORKNUM, map_blkno,
							  false,
							  RelFileLocatorBackendIsTemp(rlocator));
}

static void
MapFlushWriteSuperblockEntry(RelFileLocator rnode, MapSuperEntry *entry)
{
	RelFileLocatorBackend rlocator = {0};
	char			sector[MAP_SUPERBLOCK_SIZE];

	Assert(entry != NULL);

	if (!InRecovery && entry->page_lsn != InvalidXLogRecPtr)
		XLogFlush(entry->page_lsn);

	rlocator.locator = rnode;
	rlocator.backend = INVALID_PROC_NUMBER;

	MapSuperblockSetLastUpdatedLSN(&entry->super, entry->page_lsn);
	MapSuperblockRefreshCRC(&entry->super);
	MapSuperblockPackSector(&entry->super, sector);
	UmMetadataWriteSuperblock(rlocator, sector, false);
}

static int
MapCollectDirtyBufferTargets(MapFlushBufferTarget **targets_out,
							 const RelFileLocator *filter_rnode,
							 bool mark_checkpoint_needed)
{
	MapFlushBufferTarget *targets;
	int			target_cap = 256;
	int			target_count = 0;

	Assert(targets_out != NULL);

	targets = palloc(sizeof(MapFlushBufferTarget) * target_cap);

	for (int i = 0; i < map_buffers; i++)
	{
		MapBufferDesc *buf = &MapBuffers[i];
		uint32		state;
		int			page_number;
		RelFileLocator slot_rnode;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		state = pg_atomic_read_u32(&buf->state);
		if ((state & MAPBUF_DIRTY) == 0)
		{
			LWLockRelease(&buf->buffer_lock);
			continue;
		}

		page_number = buf->page_number;
		slot_rnode = buf->rnode;

		if (page_number < 0 || page_number == MAP_BLOCK_SUPER)
		{
			LWLockRelease(&buf->buffer_lock);
			continue;
		}
		if (filter_rnode != NULL &&
			!RelFileLocatorEquals(slot_rnode, *filter_rnode))
		{
			LWLockRelease(&buf->buffer_lock);
			continue;
		}

		if (mark_checkpoint_needed)
			MapBufferUpdateStateBits(buf, MAPBUF_CHECKPOINT_NEEDED, 0);

		LWLockRelease(&buf->buffer_lock);

		if (target_count >= target_cap)
		{
			target_cap *= 2;
			targets = repalloc(targets,
							   sizeof(MapFlushBufferTarget) * target_cap);
		}

		targets[target_count].slot_id = i;
		targets[target_count].rnode = slot_rnode;
		targets[target_count].map_blkno = (BlockNumber) page_number;
		target_count++;
	}

	if (target_count > 1)
		sort_map_flush_buffer_targets(targets, target_count);

	*targets_out = targets;
	return target_count;
}

static inline int
map_flush_buffer_target_comparator(const MapFlushBufferTarget *a,
								   const MapFlushBufferTarget *b)
{
	if (a->rnode.spcOid < b->rnode.spcOid)
		return -1;
	else if (a->rnode.spcOid > b->rnode.spcOid)
		return 1;
	else if (a->rnode.dbOid < b->rnode.dbOid)
		return -1;
	else if (a->rnode.dbOid > b->rnode.dbOid)
		return 1;
	else if (a->rnode.relNumber < b->rnode.relNumber)
		return -1;
	else if (a->rnode.relNumber > b->rnode.relNumber)
		return 1;
	else if (a->map_blkno < b->map_blkno)
		return -1;
	else if (a->map_blkno > b->map_blkno)
		return 1;

	return 0;
}

void
MapPreCheckpoint(void)
{
	/* no-op: reclaim is handled by sync request queues. */
}

/*
 * MapCheckpoint - sync dirty metadata buffer pages during checkpoint
 *
 * Scans all buffer slots and writes dirty pages to disk.
 * Must handle concurrent access from other backends.
 */
void
MapCheckpoint(void)
{
	/*
	 * Checkpoint ordering: persist active-slot pages first, then superblocks.
	 * This keeps on-disk superblock as a checkpoint-boundary snapshot and
	 * avoids it getting ahead of active-slot page durability.
	 */
	(void) MapFlushDirtyBuffers(-1, true);
	(void) MapFlushDirtySuperblocks();
}

void
MapCheckpointRelation(RelFileLocator rnode)
{
	(void) MapFlushRelationBuffers(rnode, true);
	(void) MapFlushRelationSuperblocks(rnode);
}

void
MapCheckpointDatabaseTablespaces(Oid dbid, int ntablespaces,
								 const Oid *tablespace_ids)
{
	RelFileLocator *targets;
	int			target_cap = 256;
	int			target_count = 0;
	int			i;

	targets = palloc(sizeof(RelFileLocator) * target_cap);

	for (i = 0; i < map_buffers; i++)
	{
		MapBufferDesc *buf = &MapBuffers[i];
		uint32		state_before;
		int			page_number;
		RelFileLocator slot_rnode;

		state_before = pg_atomic_read_u32(&buf->state);
		if ((state_before & MAPBUF_DIRTY) == 0)
			continue;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		page_number = buf->page_number;
		slot_rnode = buf->rnode;
		LWLockRelease(&buf->buffer_lock);

		if (page_number < 0 ||
			slot_rnode.dbOid != dbid ||
			!MapTablespaceSelected(slot_rnode.spcOid, ntablespaces,
								   tablespace_ids))
			continue;

		if (target_count >= target_cap)
		{
			target_cap *= 2;
			targets = repalloc(targets, sizeof(RelFileLocator) * target_cap);
		}
		targets[target_count++] = slot_rnode;
	}

	for (i = 0; i < MapSuperCapacity; i++)
	{
		MapSuperEntry *entry = MapSuperEntryBySlot(i);
		RelFileLocator rnode;

		LWLockAcquire(&entry->lock, LW_SHARED);
		if (!entry->in_use ||
			(entry->flags & MAPSUPER_FLAG_DIRTY) == 0 ||
			entry->key.rnode.dbOid != dbid ||
			!MapTablespaceSelected(entry->key.rnode.spcOid, ntablespaces,
								   tablespace_ids))
		{
			LWLockRelease(&entry->lock);
			continue;
		}
		rnode = entry->key.rnode;
		LWLockRelease(&entry->lock);

		if (target_count >= target_cap)
		{
			target_cap *= 2;
			targets = repalloc(targets, sizeof(RelFileLocator) * target_cap);
		}
		targets[target_count++] = rnode;
	}

	for (i = 0; i < target_count; i++)
	{
		int j;
		bool seen = false;

		for (j = 0; j < i; j++)
		{
			if (RelFileLocatorEquals(targets[j], targets[i]))
			{
				seen = true;
				break;
			}
		}
		if (seen)
			continue;

		MapCheckpointRelation(targets[i]);
	}

	pfree(targets);
}

void
MapPostCheckpoint(void)
{
	/* no-op: reclaim is handled by sync request queues. */
}

int
MapBgWriterFlush(int max_pages)
{
	if (max_pages <= 0)
		return 0;

	/* mapwriter flushes active-slot pages only; superblock is checkpoint-owned. */
	return MapFlushDirtyBuffers(max_pages, false);
}

static int
MapFlushDirtyBuffers(int max_pages, bool checkpoint)
{
	MapFlushBufferTarget *targets;
	int			ntargets;
	int			cleaned = 0;
	MapFlushWriteCache write_cache = {0};

	ntargets = MapCollectDirtyBufferTargets(&targets, NULL, checkpoint);

	for (int i = 0; i < ntargets; i++)
	{
		MapBufferDesc *buf = &MapBuffers[targets[i].slot_id];
		uint32		state_before;
		uint32		state_after;

		if (max_pages >= 0 && cleaned >= max_pages)
			break;

		state_before = pg_atomic_read_u32(&buf->state);
		if ((state_before & MAPBUF_DIRTY) == 0)
			continue;
		if (checkpoint &&
			(state_before & MAPBUF_CHECKPOINT_NEEDED) == 0)
			continue;

		MapFlushBufferCached(targets[i].slot_id, &write_cache, checkpoint);

		state_after = pg_atomic_read_u32(&buf->state);
		if (checkpoint)
		{
			if ((state_before & MAPBUF_CHECKPOINT_NEEDED) != 0 &&
				(state_after & MAPBUF_CHECKPOINT_NEEDED) == 0)
				cleaned++;
		}
		else if ((state_before & MAPBUF_DIRTY) != 0 &&
			(state_after & MAPBUF_DIRTY) == 0)
			cleaned++;
	}

	MapFlushWriteCacheReset(&write_cache);
	pfree(targets);

	return cleaned;
}

static int
MapFlushRelationBuffers(RelFileLocator rnode, bool checkpoint)
{
	MapFlushBufferTarget *targets;
	int			ntargets;
	int			cleaned = 0;
	MapFlushWriteCache write_cache = {0};

	ntargets = MapCollectDirtyBufferTargets(&targets, &rnode, checkpoint);

	for (int i = 0; i < ntargets; i++)
	{
		MapBufferDesc *buf = &MapBuffers[targets[i].slot_id];
		uint32		state_before;
		uint32		state_after;

		state_before = pg_atomic_read_u32(&buf->state);
		if ((state_before & MAPBUF_DIRTY) == 0)
			continue;
		if (checkpoint &&
			(state_before & MAPBUF_CHECKPOINT_NEEDED) == 0)
			continue;

		MapFlushBufferCached(targets[i].slot_id, &write_cache, checkpoint);

		state_after = pg_atomic_read_u32(&buf->state);
		if (checkpoint)
		{
			if ((state_before & MAPBUF_CHECKPOINT_NEEDED) != 0 &&
				(state_after & MAPBUF_CHECKPOINT_NEEDED) == 0)
				cleaned++;
		}
		else if ((state_before & MAPBUF_DIRTY) != 0 &&
			(state_after & MAPBUF_DIRTY) == 0)
			cleaned++;
	}

	MapFlushWriteCacheReset(&write_cache);
	pfree(targets);

	return cleaned;
}

static int
MapFlushDirtySuperblocks(void)
{
	typedef struct MapSuperDirtyTarget
	{
		RelFileLocator	rnode;
	} MapSuperDirtyTarget;

	MapSuperEntry *entry;
	MapSuperDirtyTarget *targets;
	int			target_cap = 256;
	int			target_count;
	bool		need_rescan;
	int			cleaned = 0;

	targets = palloc(sizeof(MapSuperDirtyTarget) * target_cap);

	do
	{
		int			i;
		int			slot_id;

		target_count = 0;
		need_rescan = false;

		for (slot_id = 0; slot_id < MapSuperCapacity; slot_id++)
		{
			entry = MapSuperEntryBySlot(slot_id);
			LWLockAcquire(&entry->lock, LW_SHARED);
			if (entry->in_use &&
				(entry->flags & MAPSUPER_FLAG_DIRTY) != 0)
			{
				if (target_count >= target_cap)
				{
					need_rescan = true;
					LWLockRelease(&entry->lock);
					break;
				}
				targets[target_count].rnode = entry->key.rnode;
				target_count++;
			}
			LWLockRelease(&entry->lock);
		}

		for (i = 0; i < target_count; i++)
		{
			RelFileLocator	rnode = targets[i].rnode;

			if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
				continue;

			if ((entry->flags & MAPSUPER_FLAG_DIRTY) == 0)
			{
				LWLockRelease(&entry->lock);
				continue;
			}

			if (!MapSuperblockHasValidIdentity(&entry->super))
			{
				LWLockRelease(&entry->lock);
				MapSBlockReportCorrupt(rnode,
									   "invalid identity while flushing");
			}

			MapFlushWriteSuperblockEntry(rnode, entry);

			entry->flags &= ~MAPSUPER_FLAG_DIRTY;
			cleaned++;
			LWLockRelease(&entry->lock);
		}

		if (need_rescan)
		{
			target_cap *= 2;
			targets = repalloc(targets,
							   sizeof(MapSuperDirtyTarget) * target_cap);
		}
	}
	while (need_rescan);

	pfree(targets);

	return cleaned;
}

static int
MapFlushRelationSuperblocks(RelFileLocator rnode)
{
	MapSuperEntry *entry;
	int			cleaned = 0;

	if (!MapSuperFindEntryLocked(rnode, LW_EXCLUSIVE, &entry))
		return 0;

	if ((entry->flags & MAPSUPER_FLAG_DIRTY) == 0)
	{
		LWLockRelease(&entry->lock);
		return 0;
	}

	if (!MapSuperblockHasValidIdentity(&entry->super))
	{
		LWLockRelease(&entry->lock);
		MapSBlockReportCorrupt(rnode, "invalid identity while flushing");
	}

	MapFlushWriteSuperblockEntry(rnode, entry);

	entry->flags &= ~MAPSUPER_FLAG_DIRTY;
	cleaned++;
	LWLockRelease(&entry->lock);

	return cleaned;
}

void
MapFlushBuffer(int slot_id)
{
	MapFlushBufferCached(slot_id, NULL, false);
}

static void
MapFlushBufferCached(int slot_id, MapFlushWriteCache *write_cache,
					 bool checkpoint)
{
	int				page_number;
	BlockNumber		map_blkno;
	RelFileLocator	rnode;
	RelFileLocatorBackend rlocator;
	XLogRecPtr		page_lsn;
	MapBufferDesc   *buf;
	MapPage		   *page;
	UmbraFileContext *ctx;

	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);

	/*
	 * First lock I/O state so only one backend writes this slot. Hold content
	 * lock exclusively while writing, so page content and page_lsn stay in
	 * sync for writeback.
	 */
	if (!MapStartBufferIO(buf,
						  checkpoint ? MAPBUF_CHECKPOINT_NEEDED : 0))
		return;

	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);

	page_number = buf->page_number;
	rnode = buf->rnode;
	page_lsn = buf->page_lsn;

	if (page_number < 0)
	{
		/* Defensive cleanup: invalid slot must not stay dirty. */
		MapTerminateBufferIO(buf, true, 0);
		LWLockRelease(&buf->buffer_lock);
		return;
	}
	map_blkno = (BlockNumber) page_number;

	if (map_blkno == MAP_BLOCK_SUPER)
	{
		/*
		 * Superblock is managed by the dedicated superblock table and must not
		 * be present in the regular MAP buffer cache.
		 */
		MapTerminateBufferIO(buf, false, MAPBUF_IO_ERROR);
		LWLockRelease(&buf->buffer_lock);
		elog(ERROR, "MAP superblock cannot be flushed via map buffer cache");
	}

	/*
	 * Flush by slot owner rnode without going through smgr/umopen again.
	 * MapReadBuffer() can call this while a data-fork read already has an AIO
	 * handle handed out, so reopening through smgr would recurse into Umbra
	 * map-state lookup on the read path.
	 */
	if (write_cache != NULL)
	{
		ctx = MapFlushContextFor(write_cache, rnode);
		rlocator = write_cache->rlocator;
	}
	else
	{
		rlocator.locator = rnode;
		rlocator.backend = INVALID_PROC_NUMBER;
		ctx = umfile_ctx_create_temporary(rlocator);
	}

	MapFlushWritePage(rlocator, ctx, map_blkno, (char *) page, page_lsn);

	MapTerminateBufferIO(buf, true, 0);
	LWLockRelease(&buf->buffer_lock);

	if (write_cache == NULL)
		umfile_ctx_destroy_temporary(ctx);
}

static bool
MapTablespaceSelected(Oid spcOid, int ntablespaces, const Oid *tablespace_ids)
{
	int			i;

	if (ntablespaces <= 0 || tablespace_ids == NULL)
		return true;

	for (i = 0; i < ntablespaces; i++)
	{
		if (tablespace_ids[i] == spcOid)
			return true;
	}

	return false;
}
