/*-------------------------------------------------------------------------
 *
 * mapinflight.c
 *	  MAP in-flight remap ownership tracking.
 *
 * Shared state is deliberately only a per-MAP-buffer bitmap. The bitmap
 * serializes remaps of the same logical MAP entry across backends; the pblk
 * reserved by the owner is backend-local, so other backends cannot borrow an
 * uncommitted physical target.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "utils/memutils.h"

typedef struct MapInflightLocalEntry
{
	RelFileLocator rnode;
	ForkNumber	forknum;
	BlockNumber	lblkno;
	BlockNumber	pblkno;
	int			slot_id;
	int			entry_idx;
} MapInflightLocalEntry;

static MemoryContext MapInflightLocalCxt = NULL;
static MapInflightLocalEntry *MapInflightLocalEntries = NULL;
static int	MapInflightLocalCount = 0;
static int	MapInflightLocalCapacity = 0;

static void MapInflightLocalEnsureContext(void);
static void MapInflightLocalEnsureCapacity(int needed);
static int	MapInflightLocalFind(RelFileLocator rnode, ForkNumber forknum,
								 BlockNumber lblkno);
static void MapInflightLocalForget(RelFileLocator rnode, ForkNumber forknum,
								   BlockNumber lblkno);
static void MapInflightLocalRememberPrepared(RelFileLocator rnode,
											 ForkNumber forknum,
											 BlockNumber lblkno,
											 BlockNumber pblkno,
											 int slot_id, int entry_idx);
static void MapInflightDecode(ForkNumber forknum, BlockNumber lblkno,
							  BlockNumber *map_blkno, int *entry_idx);
static inline uint64 MapInflightEntryMask(int entry_idx);
static bool MapInflightBufferTryClaim(RelFileLocator rnode, ForkNumber forknum,
									  BlockNumber map_blkno, int slot_id,
									  int entry_idx);
static void MapInflightBufferRelease(int slot_id, int entry_idx);

static void
MapInflightLocalEnsureContext(void)
{
	if (MapInflightLocalCxt == NULL)
	{
		MapInflightLocalCxt = AllocSetContextCreate(TopMemoryContext,
												   "MapInflightLocal",
												   ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(MapInflightLocalCxt, true);
	}
}

void
MapInflightBackendInit(void)
{
	MapInflightLocalEnsureContext();
}

static void
MapInflightLocalEnsureCapacity(int needed)
{
	int			new_capacity;

	MapInflightLocalEnsureContext();

	if (MapInflightLocalCapacity >= needed)
		return;

	new_capacity = (MapInflightLocalCapacity == 0) ? 16 : MapInflightLocalCapacity;
	while (new_capacity < needed)
		new_capacity *= 2;

	if (MapInflightLocalEntries == NULL)
		MapInflightLocalEntries =
			MemoryContextAlloc(MapInflightLocalCxt,
							   sizeof(MapInflightLocalEntry) * new_capacity);
	else
		MapInflightLocalEntries =
			repalloc(MapInflightLocalEntries,
					 sizeof(MapInflightLocalEntry) * new_capacity);

	MapInflightLocalCapacity = new_capacity;
}

static int
MapInflightLocalFind(RelFileLocator rnode, ForkNumber forknum,
					 BlockNumber lblkno)
{
	int			i;

	for (i = 0; i < MapInflightLocalCount; i++)
	{
		MapInflightLocalEntry *entry = &MapInflightLocalEntries[i];

		if (!RelFileLocatorEquals(entry->rnode, rnode))
			continue;
		if (entry->forknum != forknum)
			continue;
		if (entry->lblkno != lblkno)
			continue;
		return i;
	}

	return -1;
}

static void
MapInflightLocalRememberPrepared(RelFileLocator rnode, ForkNumber forknum,
								 BlockNumber lblkno, BlockNumber pblkno,
								 int slot_id, int entry_idx)
{
	Assert(MapInflightLocalFind(rnode, forknum, lblkno) < 0);
	Assert(MapInflightLocalCount < MapInflightLocalCapacity);

	MapInflightLocalEntries[MapInflightLocalCount].rnode = rnode;
	MapInflightLocalEntries[MapInflightLocalCount].forknum = forknum;
	MapInflightLocalEntries[MapInflightLocalCount].lblkno = lblkno;
	MapInflightLocalEntries[MapInflightLocalCount].pblkno = pblkno;
	MapInflightLocalEntries[MapInflightLocalCount].slot_id = slot_id;
	MapInflightLocalEntries[MapInflightLocalCount].entry_idx = entry_idx;
	MapInflightLocalCount++;
}

static void
MapInflightDecode(ForkNumber forknum, BlockNumber lblkno,
				  BlockNumber *map_blkno, int *entry_idx)
{
	BlockNumber	map_entry_no;

	Assert(map_blkno != NULL);
	Assert(entry_idx != NULL);

	map_entry_no = MapLblknoToMapBlkno(forknum, lblkno);
	*entry_idx = map_entry_no % MAP_ENTRIES_PER_PAGE;
	*map_blkno = map_entry_no / MAP_ENTRIES_PER_PAGE;
	Assert(*entry_idx >= 0 && *entry_idx < MAP_ENTRIES_PER_PAGE);
}

static inline uint64
MapInflightEntryMask(int entry_idx)
{
	Assert(entry_idx >= 0 && entry_idx < MAP_ENTRIES_PER_PAGE);
	return UINT64CONST(1) << (entry_idx % MAP_PENDING_BITS_PER_WORD);
}

static bool
MapInflightBufferTryClaim(RelFileLocator rnode, ForkNumber forknum,
						  BlockNumber map_blkno, int slot_id, int entry_idx)
{
	MapBufferDesc *buf = &MapBuffers[slot_id];
	int			word_idx = entry_idx / MAP_PENDING_BITS_PER_WORD;
	uint64		mask = MapInflightEntryMask(entry_idx);
	bool		claimed = false;

	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
	if (buf->page_number == map_blkno &&
		buf->forknum == forknum &&
		RelFileLocatorEquals(buf->rnode, rnode))
	{
		if ((buf->pending_bits[word_idx] & mask) == 0)
		{
			buf->pending_bits[word_idx] |= mask;
			buf->pending_count++;
			claimed = true;
		}
	}
	LWLockRelease(&buf->buffer_lock);

	return claimed;
}

static void
MapInflightBufferRelease(int slot_id, int entry_idx)
{
	MapBufferDesc *buf = &MapBuffers[slot_id];
	int			word_idx = entry_idx / MAP_PENDING_BITS_PER_WORD;
	uint64		mask = MapInflightEntryMask(entry_idx);

	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
	Assert(buf->pending_bits[word_idx] & mask);
	if ((buf->pending_bits[word_idx] & mask) != 0)
	{
		buf->pending_bits[word_idx] &= ~mask;
		Assert(buf->pending_count > 0);
		buf->pending_count--;
	}
	LWLockRelease(&buf->buffer_lock);

	MapUnpinBuffer(slot_id);
}

static void
MapInflightLocalForget(RelFileLocator rnode, ForkNumber forknum,
					   BlockNumber lblkno)
{
	int			idx;

	idx = MapInflightLocalFind(rnode, forknum, lblkno);
	if (idx < 0)
		return;

	MapInflightLocalCount--;
	if (idx != MapInflightLocalCount)
		MapInflightLocalEntries[idx] = MapInflightLocalEntries[MapInflightLocalCount];
}

bool
MapInflightLookupOwnedPblk(RelFileLocator rnode,
						   ForkNumber forknum,
						   BlockNumber lblkno,
						   BlockNumber *pblkno)
{
	int			idx;

	Assert(pblkno != NULL);

	idx = MapInflightLocalFind(rnode, forknum, lblkno);
	if (idx < 0)
		return false;
	if (MapInflightLocalEntries[idx].pblkno == InvalidBlockNumber)
		return false;

	*pblkno = MapInflightLocalEntries[idx].pblkno;
	return true;
}

bool
MapInflightTryClaimBarrier(UmbraFileContext *map_ctx,
						   RelFileLocator rnode,
						   ForkNumber forknum,
						   BlockNumber lblkno,
						   MapInflightBarrier *barrier)
{
	BlockNumber	map_blkno;
	int			entry_idx;
	int			slot_id;

	Assert(barrier != NULL);
	Assert(!barrier->valid);

	if (MapInflightLocalFind(rnode, forknum, lblkno) >= 0)
		elog(ERROR,
			 "cannot claim write barrier while owning in-flight remap for relation %u/%u/%u fork %d block %u",
			 rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum, lblkno);

	MapInflightDecode(forknum, lblkno, &map_blkno, &entry_idx);

	Assert(map_ctx != NULL);
	slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);

	if (!MapInflightBufferTryClaim(rnode, forknum, map_blkno, slot_id,
								   entry_idx))
	{
		MapUnpinBuffer(slot_id);
		return false;
	}

	barrier->valid = true;
	barrier->slot_id = slot_id;
	barrier->entry_idx = entry_idx;

	return true;
}

void
MapInflightReleaseBarrier(MapInflightBarrier *barrier)
{
	if (barrier == NULL || !barrier->valid)
		return;

	MapInflightBufferRelease(barrier->slot_id, barrier->entry_idx);
	barrier->valid = false;
	barrier->slot_id = -1;
	barrier->entry_idx = -1;
}

bool
MapInflightTryClaim(UmbraFileContext *map_ctx, RelFileLocator rnode,
					ForkNumber forknum, BlockNumber lblkno)
{
	BlockNumber	map_blkno;
	int			entry_idx;
	int			slot_id;

	if (MapInflightLocalFind(rnode, forknum, lblkno) >= 0)
		return false;

	/*
	 * Ensure backend-local storage before publishing any shared in-flight
	 * state, so the post-claim path cannot throw due to allocation.
	 */
	MapInflightLocalEnsureCapacity(MapInflightLocalCount + 1);
	MapInflightDecode(forknum, lblkno, &map_blkno, &entry_idx);

	Assert(map_ctx != NULL);
	slot_id = MapReadBuffer(map_ctx, rnode, forknum, map_blkno);

	if (!MapInflightBufferTryClaim(rnode, forknum, map_blkno, slot_id,
								   entry_idx))
	{
		MapUnpinBuffer(slot_id);
		return false;
	}

	MapInflightLocalRememberPrepared(rnode, forknum, lblkno,
									 InvalidBlockNumber,
									 slot_id, entry_idx);

	return true;
}

void
MapInflightFinishClaim(RelFileLocator rnode, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber pblkno)
{
	int			local_idx;

	Assert(pblkno != InvalidBlockNumber);

	local_idx = MapInflightLocalFind(rnode, forknum, lblkno);
	if (local_idx < 0)
		elog(ERROR,
			 "in-flight remap claim disappeared for relation %u/%u/%u fork %d block %u",
			 rnode.spcOid, rnode.dbOid, rnode.relNumber, forknum, lblkno);

	MapInflightLocalEntries[local_idx].pblkno = pblkno;
}

void
MapInflightRelease(RelFileLocator rnode, ForkNumber forknum,
				   BlockNumber lblkno)
{
	int			idx;
	int			slot_id;
	int			entry_idx;

	idx = MapInflightLocalFind(rnode, forknum, lblkno);
	if (idx < 0)
		return;

	slot_id = MapInflightLocalEntries[idx].slot_id;
	entry_idx = MapInflightLocalEntries[idx].entry_idx;
	MapInflightLocalForget(rnode, forknum, lblkno);
	MapInflightBufferRelease(slot_id, entry_idx);
}

bool
MapInflightBitIsSet(RelFileLocator rnode, ForkNumber forknum,
					BlockNumber lblkno)
{
	BlockNumber	map_blkno;
	int			entry_idx;
	int			slot_id;
	MapBufferDesc *buf;
	int			word_idx;
	uint64		mask;
	bool		exists = false;

	MapInflightDecode(forknum, lblkno, &map_blkno, &entry_idx);
	slot_id = MapCacheLookup(rnode, forknum, map_blkno);
	if (slot_id < 0)
		return false;

	buf = &MapBuffers[slot_id];
	word_idx = entry_idx / MAP_PENDING_BITS_PER_WORD;
	mask = MapInflightEntryMask(entry_idx);

	MapPinBuffer(slot_id, false);
	LWLockAcquire(&buf->buffer_lock, LW_SHARED);
	if (buf->page_number == map_blkno &&
		buf->forknum == forknum &&
		RelFileLocatorEquals(buf->rnode, rnode))
		exists = (buf->pending_bits[word_idx] & mask) != 0;
	LWLockRelease(&buf->buffer_lock);
	MapUnpinBuffer(slot_id);

	return exists;
}

void
MapInflightCleanupOwned(void)
{
	while (MapInflightLocalCount > 0)
	{
		MapInflightLocalEntry *entry = &MapInflightLocalEntries[0];

		MapInflightRelease(entry->rnode, entry->forknum, entry->lblkno);
	}
}
