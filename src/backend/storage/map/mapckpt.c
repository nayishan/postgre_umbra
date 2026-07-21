/*-------------------------------------------------------------------------
 *
 * mapckpt.c
 *	  Checkpoint epoch tracking for Umbra MAP pages.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/map_internal.h"
#include "storage/shmem.h"

#define MAP_CHECKPOINT_EPOCH_ACTIVE	((uint32) 0x80000000)
#define MAP_CHECKPOINT_EPOCH_MASK		((uint32) 0x7FFFFFFF)

typedef struct MapCheckpointEpochShared
{
	pg_atomic_uint32 epoch_state;
} MapCheckpointEpochShared;

static MapCheckpointEpochShared *MapCheckpointEpoch = NULL;

void
MapCheckpointEpochShmemRequest(void)
{
	ShmemRequestStruct(.name = "Map Checkpoint Epoch",
					   .size = sizeof(MapCheckpointEpochShared),
					   .ptr = (void **) &MapCheckpointEpoch,
		);
}

void
MapCheckpointEpochShmemInit(void)
{
	pg_atomic_init_u32(&MapCheckpointEpoch->epoch_state, 0);
}

uint32
MapCheckpointCurrentEpoch(void)
{
	uint32		epoch_state;

	epoch_state = pg_atomic_read_u32(&MapCheckpointEpoch->epoch_state);
	if ((epoch_state & MAP_CHECKPOINT_EPOCH_ACTIVE) == 0)
		return 0;

	return epoch_state & MAP_CHECKPOINT_EPOCH_MASK;
}

void
MapCheckpointBegin(void)
{
	uint32		epoch_state;
	uint32		epoch;

	epoch_state = pg_atomic_read_u32(&MapCheckpointEpoch->epoch_state);
	Assert((epoch_state & MAP_CHECKPOINT_EPOCH_ACTIVE) == 0);

	epoch = (epoch_state & MAP_CHECKPOINT_EPOCH_MASK) + 1;
	if (epoch == 0 || epoch > MAP_CHECKPOINT_EPOCH_MASK)
		epoch = 1;

	pg_atomic_write_u32(&MapCheckpointEpoch->epoch_state,
						MAP_CHECKPOINT_EPOCH_ACTIVE | epoch);
}

void
MapCheckpointEnd(void)
{
	uint32		epoch_state;

	epoch_state = pg_atomic_read_u32(&MapCheckpointEpoch->epoch_state);
	if ((epoch_state & MAP_CHECKPOINT_EPOCH_ACTIVE) == 0)
		return;

	pg_atomic_write_u32(&MapCheckpointEpoch->epoch_state,
						epoch_state & MAP_CHECKPOINT_EPOCH_MASK);
}

void
MapCheckpointAbort(void)
{
	MapCheckpointEnd();
}
