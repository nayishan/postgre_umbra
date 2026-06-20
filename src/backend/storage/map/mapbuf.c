/*-------------------------------------------------------------------------
 *
 * mapbuf.c
 *	  MAP buffer state, pinning, and I/O helpers.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "utils/memutils.h"

/* local state for MapStartBufferIO and related functions */
static MapBufferDesc *InProgressMapBuf = NULL;
static int		   *MapPrivateRefCount = NULL;
static MemoryContext MapLocalCxt = NULL;

static void MapWaitIO(MapBufferDesc *buf);
static void MapEnsureBufferMaterialized(UmbraFileContext *map_ctx,
										MapBufferDesc *buf);

void
MapEnsurePrivateRefCount(void)
{
	if (MapPrivateRefCount == NULL)
	{
		if (MapLocalCxt == NULL)
		{
			MapLocalCxt = AllocSetContextCreate(TopMemoryContext,
												"MapLocal",
												ALLOCSET_DEFAULT_SIZES);
			MemoryContextAllowInCriticalSection(MapLocalCxt, true);
		}
		MapPrivateRefCount = MemoryContextAllocZero(MapLocalCxt,
													map_buffers * sizeof(int));
	}
}

void
MapBufferUpdateStateBits(MapBufferDesc *buf, uint32 set_bits, uint32 clear_bits)
{
	for (;;)
	{
		uint32		old_state;
		uint32		new_state;

		old_state = pg_atomic_read_u32(&buf->state);
		new_state = (old_state | set_bits) & ~clear_bits;
		if (pg_atomic_compare_exchange_u32(&buf->state, &old_state, new_state))
			return;
	}
}

static void
MapEnsureBufferMaterialized(UmbraFileContext *map_ctx, MapBufferDesc *buf)
{
	uint32		state;
	BlockNumber map_nblocks;
	BlockNumber map_blkno;

	Assert(map_ctx != NULL);
	Assert(buf != NULL);
	Assert(LWLockHeldByMeInMode(&buf->buffer_lock, LW_EXCLUSIVE));
	Assert(buf->page_number >= 0);
	Assert(buf->page_number != MAP_BLOCK_SUPER);

	state = pg_atomic_read_u32(&buf->state);
	if ((state & MAPBUF_NOT_MATERIALIZED) == 0)
		return;

	if (!umfile_ctx_fork_exists(map_ctx, UMBRA_METADATA_FORKNUM))
		elog(PANIC,
			 "cannot materialize MAP page %d for relation %u/%u/%u without MAP fork",
			 buf->page_number,
			 buf->rnode.spcOid,
			 buf->rnode.dbOid,
			 buf->rnode.relNumber);

	map_nblocks = umfile_ctx_get_nblocks(map_ctx, UMBRA_METADATA_FORKNUM);
	map_blkno = (BlockNumber) buf->page_number;

	if (map_blkno >= map_nblocks)
	{
		/*
		 * Mirror buffer-pool extension ownership: create the physical block
		 * at first dirtying, not during checkpoint flush.
		 */
		umfile_zeroextend(map_ctx, UMBRA_METADATA_FORKNUM,
						  map_nblocks,
						  (int) (map_blkno + 1 - map_nblocks),
						  false);
	}

	MapBufferUpdateStateBits(buf, 0, MAPBUF_NOT_MATERIALIZED);
}

void
MapMarkBufferDirty(UmbraFileContext *map_ctx, MapBufferDesc *buf,
				   XLogRecPtr page_lsn)
{
	Assert(buf != NULL);
	Assert(LWLockHeldByMeInMode(&buf->buffer_lock, LW_EXCLUSIVE));

	if (buf->page_number != MAP_BLOCK_SUPER)
		MapEnsureBufferMaterialized(map_ctx, buf);

	buf->page_lsn = page_lsn;
	MapBufferUpdateStateBits(buf, MAPBUF_DIRTY | MAPBUF_JUST_DIRTIED, 0);
}

/*
 * MapWaitIO -- Block until MAPBUF_IO_IN_PROGRESS is cleared.
 */
static void
MapWaitIO(MapBufferDesc *buf)
{
	for (;;)
	{
		uint32		state;

		state = pg_atomic_read_u32(&buf->state);
		if (!(state & MAPBUF_IO_IN_PROGRESS))
			break;

		LWLockAcquire(&buf->io_in_progress_lock, LW_SHARED);
		LWLockRelease(&buf->io_in_progress_lock);
	}
}

/*
 * MapStartBufferIO -- begin output I/O on this map buffer.
 *
 * Returns true if caller should perform I/O; false if page is already clean or
 * no longer has the caller-required state bits.
 */
bool
MapStartBufferIO(MapBufferDesc *buf, uint32 required_bits)
{
	uint32		state;

	Assert(!InProgressMapBuf);

	for (;;)
	{
		LWLockAcquire(&buf->io_in_progress_lock, LW_EXCLUSIVE);
		state = pg_atomic_read_u32(&buf->state);

		if (!(state & MAPBUF_IO_IN_PROGRESS))
			break;

		/*
		 * Another backend is finishing I/O (or recovering from an error); wait
		 * for the in-progress bit to clear before retrying.
		 */
		LWLockRelease(&buf->io_in_progress_lock);
		MapWaitIO(buf);
	}

	if ((state & MAPBUF_DIRTY) == 0 ||
		(state & required_bits) != required_bits)
	{
		LWLockRelease(&buf->io_in_progress_lock);
		return false;
	}

	for (;;)
	{
		uint32		new_state;
		uint32		expected;

		if ((state & MAPBUF_DIRTY) == 0 ||
			(state & required_bits) != required_bits)
		{
			LWLockRelease(&buf->io_in_progress_lock);
			return false;
		}

		expected = state;
		new_state = (state | MAPBUF_IO_IN_PROGRESS) &
			~(MAPBUF_IO_ERROR | MAPBUF_JUST_DIRTIED);
		if (pg_atomic_compare_exchange_u32(&buf->state, &expected, new_state))
			break;
		state = expected;
	}

	InProgressMapBuf = buf;
	return true;
}

/*
 * MapTerminateBufferIO -- complete output I/O state transition.
 *
 * Assumes this backend owns I/O on buf.
 */
void
MapTerminateBufferIO(MapBufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
	for (;;)
	{
		uint32		old_state;
		uint32		new_state;

		old_state = pg_atomic_read_u32(&buf->state);
		Assert(old_state & MAPBUF_IO_IN_PROGRESS);

		new_state = old_state & ~(MAPBUF_IO_IN_PROGRESS | MAPBUF_IO_ERROR);
		if (clear_dirty)
		{
			new_state &= ~MAPBUF_CHECKPOINT_NEEDED;
			if (!(old_state & MAPBUF_JUST_DIRTIED))
				new_state &= ~MAPBUF_DIRTY;
		}
		new_state |= set_flag_bits;

		if (pg_atomic_compare_exchange_u32(&buf->state, &old_state, new_state))
			break;
	}

	InProgressMapBuf = NULL;
	LWLockRelease(&buf->io_in_progress_lock);
}

/*
 * MapAbortBufferIO -- cleanup map buffer I/O after an ERROR.
 */
void
MapAbortBufferIO(void)
{
	MapBufferDesc *buf = InProgressMapBuf;
	uint32		state;

	if (buf == NULL)
		return;

	LWLockAcquire(&buf->io_in_progress_lock, LW_EXCLUSIVE);

	state = pg_atomic_read_u32(&buf->state);
	if (state & MAPBUF_IO_IN_PROGRESS)
		MapTerminateBufferIO(buf, false, MAPBUF_IO_ERROR);
	else
	{
		InProgressMapBuf = NULL;
		LWLockRelease(&buf->io_in_progress_lock);
	}
}

void
MapBackendExitCleanup(void)
{
	int			slot_id;

	/*
	 * First clear in-progress map I/O ownership, so other waiters can make
	 * progress even if current backend is leaving via ERROR/abort.
	 */
	MapAbortBufferIO();
	MapInflightCleanupOwned();

	if (MapPrivateRefCount == NULL)
		return;

	/* Release all map pins held by this backend. */
	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		while (MapPrivateRefCount[slot_id] > 0)
			MapUnpinBuffer(slot_id);
	}

	MapResetAllTruncatePreloads();

#ifdef USE_ASSERT_CHECKING
	Assert(InProgressMapBuf == NULL);
	for (slot_id = 0; slot_id < map_buffers; slot_id++)
	{
		Assert(MapPrivateRefCount[slot_id] == 0);
		Assert(!LWLockHeldByMe(&MapBuffers[slot_id].buffer_lock));
		Assert(!LWLockHeldByMe(&MapBuffers[slot_id].io_in_progress_lock));
	}
#endif
}

/*
 * MapPinBuffer - pin a map buffer
 *
 * Increments the refcount for the buffer. If adjust_usage is true,
 * also increments the usage_count (up to max 5).
 */
void
MapPinBuffer(int slot_id, bool adjust_usage)
{
	uint32_t	state;

	MapEnsurePrivateRefCount();

	/* Increment shared refcount first. */
	while (true)
	{
		uint32_t	old_state = pg_atomic_read_u32(&MapBuffers[slot_id].state);
		uint32_t	new_state = old_state + 1;

		if (MAPBUF_GET_REFCOUNT(old_state) >= MAPBUF_VALID_MASK)
			elog(ERROR, "map buffer reference count overflow");

		if (pg_atomic_compare_exchange_u32(&MapBuffers[slot_id].state,
										   &old_state, new_state))
		{
			state = new_state;
			break;
		}
	}

	MapPrivateRefCount[slot_id]++;
	Assert(MapPrivateRefCount[slot_id] > 0);

	/* Increment usage count if requested. */
	if (adjust_usage && MAPBUF_GET_USAGECOUNT(state) < 5)
	{
		while (true)
		{
			uint32_t	old_state = pg_atomic_read_u32(&MapBuffers[slot_id].state);
			uint32_t	new_state = old_state + MAPBUF_USAGECOUNT_ONE;

			if (pg_atomic_compare_exchange_u32(&MapBuffers[slot_id].state,
											   &old_state, new_state))
				break;
		}
	}
}

/*
 * MapUnpinBuffer - unpin a map buffer
 *
 * Decrements the refcount for the buffer.
 */
void
MapUnpinBuffer(int slot_id)
{
	MapEnsurePrivateRefCount();

	if (MapPrivateRefCount[slot_id] == 0)
		elog(ERROR, "map buffer private refcount underflow");

	while (true)
	{
		uint32_t	old_state = pg_atomic_read_u32(&MapBuffers[slot_id].state);
		uint32_t	new_state = old_state - 1;

		if (MAPBUF_GET_REFCOUNT(old_state) == 0)
			elog(ERROR, "map buffer refcount underflow");

		if (pg_atomic_compare_exchange_u32(&MapBuffers[slot_id].state,
										   &old_state, new_state))
			break;
	}

	MapPrivateRefCount[slot_id]--;
}

/*
 * MapInvalidateBuffer - invalidate a buffer slot for a specific mapping tag.
 *
 * This follows buffer-pool invalidation semantics:
 * - caller identifies expected tag and slot
 * - if slot tag changed while waiting, do nothing
 * - if slot is still pinned, wait/retry until safe to invalidate
 */
void
MapInvalidateBuffer(int slot_id, RelFileLocator expected_rnode,
					ForkNumber expected_forknum,
					BlockNumber expected_map_blkno)
{
	MapBufferDesc *buf = &MapBuffers[slot_id];
	uint32		state;

retry:
	LWLockAcquire(&buf->io_in_progress_lock, LW_EXCLUSIVE);

	LWLockAcquire(&buf->buffer_lock, LW_EXCLUSIVE);
	if (buf->page_number < 0 ||
		buf->page_number != expected_map_blkno ||
		buf->forknum != expected_forknum ||
		!RelFileLocatorEquals(buf->rnode, expected_rnode))
	{
		LWLockRelease(&buf->buffer_lock);
		LWLockRelease(&buf->io_in_progress_lock);
		return;
	}

	state = pg_atomic_read_u32(&buf->state);
	if (MAPBUF_GET_REFCOUNT(state) != 0)
	{
		LWLockRelease(&buf->buffer_lock);
		LWLockRelease(&buf->io_in_progress_lock);

		if (MapPrivateRefCount != NULL &&
			MapPrivateRefCount[slot_id] > 0)
			elog(ERROR, "map buffer is pinned in MapInvalidateBuffer");

		MapWaitIO(buf);
		goto retry;
	}

	if (buf->pending_count != 0)
	{
		LWLockRelease(&buf->buffer_lock);
		LWLockRelease(&buf->io_in_progress_lock);
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
		goto retry;
	}

	MemSet(buf->pending_bits, 0, sizeof(buf->pending_bits));

	buf->page_number = -1;
	buf->forknum = InvalidForkNumber;
	memset(&buf->rnode, 0, sizeof(RelFileLocator));
	buf->page_lsn = 0;
	LWLockRelease(&buf->buffer_lock);

	/* Reset full state before returning slot to free list. */
	pg_atomic_write_u32(&buf->state, 0);
	MapClockFreeBuffer(slot_id);
	LWLockRelease(&buf->io_in_progress_lock);
}
