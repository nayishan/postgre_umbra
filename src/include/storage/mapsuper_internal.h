/*-------------------------------------------------------------------------
 *
 * mapsuper_internal.h
 *	  Shared-memory MAP superblock table internals.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPSUPER_INTERNAL_H
#define MAPSUPER_INTERNAL_H

#include "storage/lwlock.h"
#include "storage/mapsuper.h"
#include "storage/relfilelocator.h"
#include "storage/umfile.h"

#define MAPSUPER_FLAG_VALID		0x01
#define MAPSUPER_FLAG_DIRTY		0x02
#define MAPSUPER_FLAG_CORRUPT	0x04

#define MAPSUPER_RUNTIME_FLAG_PREALLOC_MAIN	0x01
#define MAPSUPER_RUNTIME_FLAG_PREALLOC_FSM	0x02
#define MAPSUPER_RUNTIME_FLAG_PREALLOC_VM	0x04
#define MAPSUPER_RUNTIME_FLAG_EXTENDING_MAIN	0x08
#define MAPSUPER_RUNTIME_FLAG_EXTENDING_FSM	0x10
#define MAPSUPER_RUNTIME_FLAG_EXTENDING_VM	0x20

typedef struct MapSuperTag
{
	RelFileLocator	rnode;
} MapSuperTag;

typedef struct MapSuperEntry
{
	MapSuperTag	key;
	MapSuperblock super;
	XLogRecPtr	page_lsn;
	uint32		flags;
	uint32		runtime_flags;
	BlockNumber	reserved_next_free_main;
	BlockNumber	reserved_next_free_fsm;
	BlockNumber	reserved_next_free_vm;
	BlockNumber	extending_target_main;
	BlockNumber	extending_target_fsm;
	BlockNumber	extending_target_vm;
	BlockNumber	reclaim_boundary_main;
	BlockNumber	reclaim_boundary_fsm;
	BlockNumber	reclaim_boundary_vm;
	int			next_free;
	bool		in_use;
	LWLock		lock;
} MapSuperEntry;

extern MapSuperEntry *MapSuperEntries;
extern int	MapSuperCapacity;

static inline MapSuperEntry *
MapSuperEntryBySlot(int slot_id)
{
	Assert(slot_id >= 0 && slot_id < MapSuperCapacity);
	return &MapSuperEntries[slot_id];
}

extern void MapSBlockReportCorrupt(RelFileLocator rnode, const char *reason);
extern bool MapForkHasMappedState(ForkNumber forknum);
extern BlockNumber MapNormalizeForkBlockCount(ForkNumber forknum,
											  BlockNumber raw);

static inline BlockNumber
MapSuperGetReservedNextFree(const MapSuperEntry *entry, ForkNumber forknum)
{
	Assert(entry != NULL);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			return entry->reserved_next_free_main;
		case FSM_FORKNUM:
			return entry->reserved_next_free_fsm;
		case VISIBILITYMAP_FORKNUM:
			return entry->reserved_next_free_vm;
		default:
			elog(ERROR, "unsupported fork number for reservation frontier: %d",
				 forknum);
	}

	pg_unreachable();
}

static inline void
MapSuperSetReservedNextFree(MapSuperEntry *entry, ForkNumber forknum,
							BlockNumber blkno)
{
	Assert(entry != NULL);

	blkno = MapNormalizeForkBlockCount(forknum, blkno);

	switch (forknum)
	{
		case MAIN_FORKNUM:
			entry->reserved_next_free_main = blkno;
			break;
		case FSM_FORKNUM:
			entry->reserved_next_free_fsm = blkno;
			break;
		case VISIBILITYMAP_FORKNUM:
			entry->reserved_next_free_vm = blkno;
			break;
		default:
			elog(ERROR, "unsupported fork number for reservation frontier: %d",
				 forknum);
	}
}

static inline void
MapSuperMaybeBumpReservedNextFree(MapSuperEntry *entry, ForkNumber forknum,
								  BlockNumber blkno)
{
	BlockNumber	current;

	Assert(entry != NULL);

	blkno = MapNormalizeForkBlockCount(forknum, blkno);
	current = MapSuperGetReservedNextFree(entry, forknum);
	if (current < blkno)
		MapSuperSetReservedNextFree(entry, forknum, blkno);
}

static inline void
MapSuperResetReservedNextFrees(MapSuperEntry *entry)
{
	Assert(entry != NULL);

	MapSuperSetReservedNextFree(entry, MAIN_FORKNUM,
								MapSuperblockGetNextFreePhysBlock(&entry->super,
																  MAIN_FORKNUM));
	MapSuperSetReservedNextFree(entry, FSM_FORKNUM,
								MapSuperblockGetNextFreePhysBlock(&entry->super,
																  FSM_FORKNUM));
	MapSuperSetReservedNextFree(entry, VISIBILITYMAP_FORKNUM,
								MapSuperblockGetNextFreePhysBlock(&entry->super,
																  VISIBILITYMAP_FORKNUM));
}

extern bool MapSuperFindEntryLocked(RelFileLocator rnode, LWLockMode mode,
									MapSuperEntry **entry);
extern bool MapSuperFindEntryTryLocked(RelFileLocator rnode, LWLockMode mode,
									   MapSuperEntry **entry);
extern MapSuperEntry *MapSuperEnsureEntryLocked(RelFileLocator rnode);
extern void MapSuperDeleteEntry(RelFileLocator rnode);
extern bool MapSuperForkExists(const MapSuperblock *super,
							   ForkNumber forknum);
extern uint32 MapSuperPreallocFlag(ForkNumber forknum);
extern BlockNumber MapSuperGetReclaimBoundary(const MapSuperEntry *entry,
											  ForkNumber forknum);
extern void MapSBlockBumpPhysicalState(UmbraFileContext *map_ctx,
									   RelFileLocator rnode,
									   ForkNumber forknum,
									   BlockNumber nblocks,
									   bool bump_next_free,
									   bool bump_capacity,
									   XLogRecPtr map_lsn);
extern bool MapSBlockTryGetReclaimBoundary(UmbraFileContext *map_ctx,
										   RelFileLocator rnode,
										   ForkNumber forknum,
										   BlockNumber *boundary_pblk);
extern void MapSBlockAdvanceReclaimBoundary(UmbraFileContext *map_ctx,
											RelFileLocator rnode,
											ForkNumber forknum,
											BlockNumber boundary_pblk);
extern void MapSuperTableShmemRequest(void);
extern void MapSuperTableShmemInit(void);
extern void MapSuperTableShmemAttach(void);

#endif							/* MAPSUPER_INTERNAL_H */
