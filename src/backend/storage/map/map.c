/*-------------------------------------------------------------------------
 *
 * map.c
 *	  Umbra metadata fork cache and chunk-paired shift bitmap support
 *
 * This module owns metadata-fork layout helpers and shared cache support for
 * the chunk-paired shift bitmap.  Chunk-paired data placement is formula-based;
 * this file must not maintain a logical-block to arbitrary-physical-block
 * entry map.
 *
 * src/backend/storage/map/map.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "common/hashfn.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/mapsuper.h"
#include "storage/mapsuper_internal.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/sync.h"
#include "storage/umfile.h"
#include "utils/memutils.h"

/* Internal functions */
static bool MapTablespaceSelected(Oid spcOid, int ntablespaces,
								  const Oid *tablespace_ids);
static BlockNumber UmbraShiftPageIndexToBlkno(ForkNumber forknum,
											  BlockNumber shift_page_idx);
static void UmbraShiftLocation(ForkNumber forknum, BlockNumber lblkno,
							   BlockNumber *shift_blkno,
							   int *bit_idx);
static bool UmbraShiftDecodeBlkno(BlockNumber map_blkno, ForkNumber *forknum,
								  BlockNumber *shift_page_idx);
static bool UmbraShiftPageWithinLogicalRange(UmbraFileContext *map_ctx,
											RelFileLocator rnode,
											ForkNumber forknum,
											BlockNumber map_blkno);
static bool UmbraShiftBlknoIsShiftPage(BlockNumber map_blkno);

static BlockNumber
UmbraShiftPageIndexToBlkno(ForkNumber forknum, BlockNumber shift_page_idx)
{
	uint64 group_no;
	uint64 blkno64;

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "Umbra metadata fork should not call UmbraShiftPageIndexToBlkno");

	switch (forknum)
	{
		case FSM_FORKNUM:
			group_no = (uint64) shift_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) shift_page_idx;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) UMBRA_SHIFT_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
		{
			uint64 group_page_idx = (uint64) shift_page_idx;

			group_no = group_page_idx / (uint64) UMBRA_SHIFT_MAIN_PAGES;
			blkno64 = (uint64) MAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) MAP_GROUP_TOTAL_PAGES +
				(uint64) UMBRA_SHIFT_FSM_PAGES +
				(uint64) UMBRA_SHIFT_VM_PAGES +
				(group_page_idx % (uint64) UMBRA_SHIFT_MAIN_PAGES);
			break;
		}

		default:
			elog(ERROR, "unsupported fork number %d in shift bitmap lookup",
				 (int) forknum);
			return 0;
	}

	if (blkno64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address Umbra shift bitmap page %u for fork %d",
						shift_page_idx, forknum)));

	return (BlockNumber) blkno64;
}

static void
UmbraShiftLocation(ForkNumber forknum, BlockNumber lblkno,
				   BlockNumber *shift_blkno, int *bit_idx)
{
	BlockNumber	shift_page_idx;

	Assert(shift_blkno != NULL);
	Assert(bit_idx != NULL);

	shift_page_idx = lblkno / UMBRA_SHIFT_BITS_PER_PAGE;
	*shift_blkno = UmbraShiftPageIndexToBlkno(forknum, shift_page_idx);
	*bit_idx = lblkno % UMBRA_SHIFT_BITS_PER_PAGE;
}

static bool
UmbraShiftDecodeBlkno(BlockNumber map_blkno, ForkNumber *forknum,
					 BlockNumber *shift_page_idx)
{
	uint64 offset;
	uint64 group_no;
	uint64 in_group;

	if (map_blkno == MAP_BLOCK_SUPER || map_blkno < MAP_BLOCK_FIRST_GROUP)
		return false;

	offset = (uint64) (map_blkno - MAP_BLOCK_FIRST_GROUP);
	group_no = offset / (uint64) MAP_GROUP_TOTAL_PAGES;
	in_group = offset % (uint64) MAP_GROUP_TOTAL_PAGES;

	if (in_group < (uint64) UMBRA_SHIFT_FSM_PAGES)
	{
		*forknum = FSM_FORKNUM;
		*shift_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) UMBRA_SHIFT_FSM_PAGES;
	if (in_group < (uint64) UMBRA_SHIFT_VM_PAGES)
	{
		*forknum = VISIBILITYMAP_FORKNUM;
		*shift_page_idx = (BlockNumber) group_no;
		return true;
	}

	in_group -= (uint64) UMBRA_SHIFT_VM_PAGES;
	if (in_group < (uint64) UMBRA_SHIFT_MAIN_PAGES)
	{
		*forknum = MAIN_FORKNUM;
		*shift_page_idx = (BlockNumber)
			(group_no * (uint64) UMBRA_SHIFT_MAIN_PAGES + in_group);
		return true;
	}

	return false;
}

static bool
UmbraShiftPageWithinLogicalRange(UmbraFileContext *map_ctx, RelFileLocator rnode,
								ForkNumber forknum, BlockNumber map_blkno)
{
	BlockNumber n_lblknos;
	ForkNumber	page_forknum;
	BlockNumber	page_idx;
	uint64		page_first_lblk;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos))
		return true;

	if (!UmbraShiftDecodeBlkno(map_blkno, &page_forknum, &page_idx))
		return false;

	if (page_forknum != forknum)
		return false;

	page_first_lblk = (uint64) page_idx * (uint64) UMBRA_SHIFT_BITS_PER_PAGE;
	if (page_first_lblk >= (uint64) n_lblknos)
		return false;

	return true;
}

static bool
UmbraShiftBlknoIsShiftPage(BlockNumber map_blkno)
{
	ForkNumber	forknum;
	BlockNumber	page_idx;

	return UmbraShiftDecodeBlkno(map_blkno, &forknum, &page_idx);
}

/*
 * MapReadBuffer - read a shift bitmap page into the metadata buffer cache
 *
 * Returns the slot_id of the buffer, with the buffer pinned.
 */
int
MapReadBuffer(UmbraFileContext *map_ctx, RelFileLocator rnode,
			  ForkNumber forknum, BlockNumber map_blkno)
{
	int			slot_id;
	uint32_t	state;
	MapPage    *page;
	MapBufferDesc *buf;
	BlockNumber map_nblocks;
	int			old_page_number;
	ForkNumber	old_forknum;
	RelFileLocator old_rnode;

	if (map_blkno == MAP_BLOCK_SUPER)
		elog(ERROR, "MapReadBuffer cannot be used for MAP superblock");
	if (!UmbraShiftBlknoIsShiftPage(map_blkno))
		elog(ERROR, "MapReadBuffer cannot load legacy entry-map page %u",
			 map_blkno);

	for (;;)
	{
		int			existing_slot_id;
		bool		retry = false;

		slot_id = MapCacheLookup(rnode, forknum, map_blkno);
		if (slot_id >= 0)
		{
			buf = &MapBuffers[slot_id];

			MapPinBuffer(slot_id, true);
			LWLockAcquire(&buf->buffer_lock, LW_SHARED);

			if (buf->page_number == map_blkno &&
				buf->page_number >= 0 &&
				RelFileLocatorEquals(buf->rnode, rnode) &&
				buf->forknum == forknum)
			{
				LWLockRelease(&buf->buffer_lock);
				return slot_id;
			}

			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		slot_id = MapClockGetBuffer();
		buf = &MapBuffers[slot_id];
		MapPinBuffer(slot_id, false);

		LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);

		if (buf->page_number == map_blkno &&
			buf->page_number >= 0 &&
			RelFileLocatorEquals(buf->rnode, rnode) &&
			buf->forknum == forknum)
		{
			LWLockRelease(&buf->buffer_lock);
			return slot_id;
		}

		state = pg_atomic_read_u32(&buf->state);
		if (MAPBUF_GET_REFCOUNT(state) != 1)
		{
			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		if (state & MAPBUF_DIRTY)
		{
			LWLockRelease(&buf->buffer_lock);
			MapFlushBuffer(slot_id);

			LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
			if (buf->page_number == map_blkno &&
				buf->page_number >= 0 &&
				RelFileLocatorEquals(buf->rnode, rnode) &&
				buf->forknum == forknum)
			{
				LWLockRelease(&buf->buffer_lock);
				return slot_id;
			}

			state = pg_atomic_read_u32(&buf->state);
			if (MAPBUF_GET_REFCOUNT(state) != 1 ||
				(state & MAPBUF_DIRTY))
			{
				LWLockRelease(&buf->buffer_lock);
				MapUnpinBuffer(slot_id);
				continue;
			}
		}

		old_page_number = buf->page_number;
		old_forknum = buf->forknum;
		old_rnode = buf->rnode;

		existing_slot_id = MapCacheInsert(rnode, forknum, map_blkno, slot_id);
		if (existing_slot_id >= 0 && existing_slot_id != slot_id)
			retry = true;
		if (retry)
		{
			LWLockRelease(&buf->buffer_lock);
			MapUnpinBuffer(slot_id);
			continue;
		}

		if (old_page_number >= 0)
			MapCacheDelete(old_rnode, old_forknum,
						   (BlockNumber) old_page_number, slot_id);

		buf->page_number = map_blkno;
		buf->rnode = rnode;
		buf->forknum = forknum;
		buf->page_lsn = 0;
		MapBufferUpdateStateBits(buf, MAPBUF_USAGECOUNT_ONE, 0);

		page = MapGetPage(slot_id);
		if (umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		{
			map_nblocks = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM);
			if (map_blkno < map_nblocks &&
				UmbraShiftPageWithinLogicalRange(map_ctx, rnode, forknum,
												 map_blkno))
			{
				umfile_ctx_read(map_ctx, UMBRA_METADATA_FORKNUM, map_blkno,
								(char *) page, BLCKSZ);
				MapBufferUpdateStateBits(buf, 0, MAPBUF_NOT_MATERIALIZED);
			}
			else
			{
				MemSet(page, 0, BLCKSZ);
				if (map_blkno >= map_nblocks)
					MapBufferUpdateStateBits(buf, MAPBUF_NOT_MATERIALIZED, 0);
				else
					MapBufferUpdateStateBits(buf, 0, MAPBUF_NOT_MATERIALIZED);
			}
		}
		else
		{
			MemSet(page, 0, BLCKSZ);
			MapBufferUpdateStateBits(buf, MAPBUF_NOT_MATERIALIZED, 0);
		}

		LWLockRelease(&buf->buffer_lock);
		return slot_id;
	}
}

/*
 * MapDrop - drop mapping for a relation
 */
void
MapDrop(RelFileLocator rnode)
{
	RelFileLocatorBackend rnode_backend;

	rnode_backend.locator = rnode;
	rnode_backend.backend = INVALID_PROC_NUMBER;

	MapInvalidateRelation(rnode);
	umfile_ctx_unlinkfork(rnode_backend, UMBRA_METADATA_FORKNUM, false);
}

bool
UmbraShiftGet(UmbraFileContext *map_ctx, RelFileLocator rnode,
			  ForkNumber forknum, BlockNumber lblkno,
			  bool *shifted_to_shadow)
{
	BlockNumber	shift_blkno;
	int			bit_idx;
	int			slot_id;
	MapPage    *page;
	MapBufferDesc *buf;
	uint8	   *bytes;

	Assert(shifted_to_shadow != NULL);

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "UmbraShiftGet does not accept Umbra metadata fork");

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
	{
		*shifted_to_shadow = false;
		return false;
	}

	UmbraShiftLocation(forknum, lblkno, &shift_blkno, &bit_idx);

	slot_id = MapReadBuffer(map_ctx, rnode, forknum, shift_blkno);
	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);
	bytes = (uint8 *) page;

	LWLockAcquire(&buf->buffer_lock, LW_SHARED);
	*shifted_to_shadow =
		(bytes[bit_idx / BITS_PER_BYTE] &
		 (1U << (bit_idx % BITS_PER_BYTE))) != 0;
	LWLockRelease(&buf->buffer_lock);

	MapUnpinBuffer(slot_id);
	return true;
}

void
UmbraShiftSet(UmbraFileContext *map_ctx, RelFileLocator rnode,
			  ForkNumber forknum, BlockNumber lblkno,
			  bool shifted_to_shadow, XLogRecPtr map_lsn)
{
	BlockNumber	shift_blkno;
	int			bit_idx;
	uint8		mask;
	int			slot_id;
	MapPage    *page;
	MapBufferDesc *buf;
	uint8	   *bytes;

	if (forknum == UMBRA_METADATA_FORKNUM)
		elog(ERROR, "UmbraShiftSet does not accept Umbra metadata fork");

	UmbraShiftLocation(forknum, lblkno, &shift_blkno, &bit_idx);
	mask = (uint8) (1U << (bit_idx % BITS_PER_BYTE));

	slot_id = MapReadBuffer(map_ctx, rnode, forknum, shift_blkno);
	buf = &MapBuffers[slot_id];
	page = MapGetPage(slot_id);
	bytes = (uint8 *) page;

	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
	if (((bytes[bit_idx / BITS_PER_BYTE] & mask) != 0) == shifted_to_shadow)
	{
		LWLockRelease(&buf->buffer_lock);
		MapUnpinBuffer(slot_id);
		return;
	}

	if (shifted_to_shadow)
		bytes[bit_idx / BITS_PER_BYTE] |= mask;
	else
		bytes[bit_idx / BITS_PER_BYTE] &= (uint8) ~mask;

	if (map_lsn == InvalidXLogRecPtr)
	{
		if (InRecovery)
			map_lsn = GetXLogReplayRecPtr(NULL);
		else
			map_lsn = GetXLogWriteRecPtr();
	}
	MapMarkBufferDirty(map_ctx, buf, map_lsn);
	LWLockRelease(&buf->buffer_lock);

	MapUnpinBuffer(slot_id);
}

void
UmbraShiftTruncate(UmbraFileContext *map_ctx, RelFileLocator rnode,
				   ForkNumber forknum, BlockNumber n_lblknos,
				   XLogRecPtr map_lsn)
{
	BlockNumber old_n_lblknos = 0;
	BlockNumber start_page_idx;
	BlockNumber end_page_idx;
	int			start_bit_idx;
	int			end_bit_idx;
	BlockNumber page_idx;

	if (forknum == UMBRA_METADATA_FORKNUM)
		return;

	Assert(map_ctx != NULL);
	Assert(map_lsn != InvalidXLogRecPtr);

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		return;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum,
									   &old_n_lblknos))
		return;
	if (old_n_lblknos <= n_lblknos)
		return;

	start_page_idx = n_lblknos / UMBRA_SHIFT_BITS_PER_PAGE;
	start_bit_idx = n_lblknos % UMBRA_SHIFT_BITS_PER_PAGE;
	end_page_idx = (old_n_lblknos - 1) / UMBRA_SHIFT_BITS_PER_PAGE;
	end_bit_idx = (old_n_lblknos - 1) % UMBRA_SHIFT_BITS_PER_PAGE;

	for (page_idx = start_page_idx; page_idx <= end_page_idx; page_idx++)
	{
		BlockNumber	shift_blkno;
		int			begin_bit;
		int			last_bit;
		int			slot_id;
		MapPage    *page;
		MapBufferDesc *buf;
		uint8	   *bytes;

		shift_blkno = UmbraShiftPageIndexToBlkno(forknum, page_idx);
		if (shift_blkno >=
			umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM))
			break;

		begin_bit = (page_idx == start_page_idx) ? start_bit_idx : 0;
		last_bit = (page_idx == end_page_idx) ? end_bit_idx :
			(UMBRA_SHIFT_BITS_PER_PAGE - 1);

		slot_id = MapReadBuffer(map_ctx, rnode, forknum, shift_blkno);
		buf = &MapBuffers[slot_id];
		page = MapGetPage(slot_id);
		bytes = (uint8 *) page;

		LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
		for (int bit = begin_bit; bit <= last_bit; bit++)
			bytes[bit / BITS_PER_BYTE] &=
				(uint8) ~(1U << (bit % BITS_PER_BYTE));
		MapMarkBufferDirty(map_ctx, buf, map_lsn);
		LWLockRelease(&buf->buffer_lock);

		MapUnpinBuffer(slot_id);
	}
}

/*
 * MapInvalidateRelation - invalidate all map cache entries for one relation.
 */
void
MapInvalidateRelation(RelFileLocator rnode)
{
	int			slot_id;

	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		MapBufferDesc *buf = &MapBuffers[slot_id];
		int			page_number;
		ForkNumber	forknum;
		RelFileLocator slot_rnode;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		page_number = buf->page_number;
		forknum = buf->forknum;
		slot_rnode = buf->rnode;
		LWLockRelease(&buf->buffer_lock);

		if (page_number < 0 || !RelFileLocatorEquals(slot_rnode, rnode))
			continue;

		MapCacheDelete(slot_rnode, forknum, (BlockNumber) page_number, slot_id);
		MapInvalidateBuffer(slot_id, slot_rnode, forknum,
							(BlockNumber) page_number);
	}

	/* Remove dedicated superblock cache entry for this relation. */
	MapSuperDeleteEntry(rnode);
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

/*
 * MapInvalidateDatabaseTablespaces - invalidate MAP metadata/cache for a DB.
 *
 * If ntablespaces<=0, invalidate all tablespaces of that DB.
 * If ntablespaces>0, only invalidate entries whose spcOid is in the list.
 *
 * This is needed because database OIDs and relfilenodes can be reused after
 * DROP/CREATE churn. Without DB-scope invalidation, stale MAP buffer/cache/
 * super entries can survive and be incorrectly reused by relations in the
 * recreated DB.
 */
void
MapInvalidateDatabaseTablespaces(Oid dbid, int ntablespaces,
								 const Oid *tablespace_ids)
{
	int			slot_id;

	/* Invalidate per-buffer cached pages */
	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		MapBufferDesc *buf = &MapBuffers[slot_id];
		int			page_number;
		ForkNumber	forknum;
		RelFileLocator slot_rnode;

		LWLockAcquire(&buf->buffer_lock, LW_SHARED);
		page_number = buf->page_number;
		forknum = buf->forknum;
		slot_rnode = buf->rnode;
		LWLockRelease(&buf->buffer_lock);

		if (page_number < 0 ||
			slot_rnode.dbOid != dbid ||
			!MapTablespaceSelected(slot_rnode.spcOid, ntablespaces, tablespace_ids))
			continue;

		MapCacheDelete(slot_rnode, forknum, (BlockNumber) page_number, slot_id);
		MapInvalidateBuffer(slot_id, slot_rnode, forknum,
							(BlockNumber) page_number);
	}

	/* Invalidate dedicated superblock cache entries for matching relations */
	{
		RelFileLocator *targets;
		int			target_cap = 256;
		int			target_count = 0;
		int			i;

		targets = palloc(sizeof(RelFileLocator) * target_cap);

		for (slot_id = 0; slot_id < MapSuperCapacity; slot_id++)
		{
			MapSuperEntry *entry = MapSuperEntryBySlot(slot_id);
			RelFileLocator rnode;

			LWLockAcquire(&entry->lock, LW_SHARED);
			if (!entry->in_use ||
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
			MapSuperDeleteEntry(targets[i]);

		pfree(targets);
	}
}

/*
 * MapInvalidateDatabase - invalidate all MAP metadata/cache for one database.
 */
void
MapInvalidateDatabase(Oid dbid)
{
	MapInvalidateDatabaseTablespaces(dbid, 0, NULL);
}

/*
 * MapGetLogicalBlockCount - get logical block count from the MAP superblock
 */
BlockNumber
MapGetLogicalBlockCount(UmbraFileContext *map_ctx, RelFileLocator rnode, ForkNumber forknum)
{
	BlockNumber n_lblknos = 0;

	if (!MapSBlockTryGetLogicalNblocks(map_ctx, rnode, forknum, &n_lblknos))
		return 0;

	return n_lblknos;
}
