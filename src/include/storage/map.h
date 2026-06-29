/*-------------------------------------------------------------------------
 *
 * map.h
 *	  Umbra metadata fork cache and chunk-paired active-slot support
 *
 * This header defines MAP metadata page layout, shared cache APIs, and address
 * translation helpers for Umbra relation-local metadata.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_H
#define MAP_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "port/atomics.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/mapsuper.h"
#include "storage/relfilelocator.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/umfile.h"

/* Forward declarations */
typedef struct MapSharedData MapSharedData;
typedef struct MapBufferDesc MapBufferDesc;

/* Map buffer configuration */
#define MAP_SUPERBLOCK_MIN_ENTRIES 50000

/*
 * Umbra metadata fork page layout:
 * - block 0: superblock (512-byte payload)
 * - blocks 1..: repeated proportional groups of chunk active-slot pages
 *
 * Each proportional group contains:
 * - 1 FSM chunk active-slot page
 * - 1 VM chunk active-slot page
 * - 256 MAIN chunk active-slot pages
 *
 * Two bits select the active slot of each formula-derived three-slot group.
 * There is no logical-block to arbitrary-physical-block entry map.
 */
#define MAP_BLOCK_SUPER        0
#define MAP_BLOCK_FIRST_GROUP  1
#define UMBRA_SHIFT_FSM_PAGES 1
#define UMBRA_SHIFT_VM_PAGES 1
#define UMBRA_ACTIVE_SLOT_BITS 2
#define UMBRA_SHIFT_BITS_PER_PAGE \
	((BLCKSZ * BITS_PER_BYTE) / UMBRA_ACTIVE_SLOT_BITS)
#define UMBRA_SHIFT_MAIN_PAGES 256
#define MAP_GROUP_TOTAL_PAGES \
	(UMBRA_SHIFT_FSM_PAGES + UMBRA_SHIFT_VM_PAGES + \
	 UMBRA_SHIFT_MAIN_PAGES)

/* Map buffer state bits */
#define MAPBUF_VALID_MASK  0x000001FF	/* refcount (max 511) */
#define MAPBUF_USAGE_COUNT_SHIFT  9
#define MAPBUF_USAGE_COUNT_MASK  0x00003E00
#define MAPBUF_DIRTY       0x00004000
#define MAPBUF_IO_IN_PROGRESS 0x00008000
#define MAPBUF_IO_ERROR    0x00010000
#define MAPBUF_JUST_DIRTIED 0x00020000
#define MAPBUF_NOT_MATERIALIZED 0x00040000
#define MAPBUF_CHECKPOINT_NEEDED 0x00080000

#define MAPBUF_GET_REFCOUNT(state) \
	((state) & MAPBUF_VALID_MASK)
#define MAPBUF_GET_USAGECOUNT(state) \
	(((state) & MAPBUF_USAGE_COUNT_MASK) >> MAPBUF_USAGE_COUNT_SHIFT)
#define MAPBUF_USAGECOUNT_ONE  0x00000200

typedef struct MapPage
{
	char		data[BLCKSZ];
} MapPage;

/* Shared memory control structure */
typedef struct MapSharedData
{
	/* clock sweep algorithm */
	pg_atomic_uint32 next_victim_buffer;
	slock_t		clock_lock;
	int			first_free_buffer;	/* head of free list, -1 if empty */
	int			mapwriter_procno;	/* procno to wake, -1 if none */

	/* statistics */
	pg_atomic_uint32 num_allocs;
	uint32		complete_passes;

	/* configuration */
	int			num_slots;
} MapSharedData;

/* Values for freeNext field */
#define FREENEXT_END_OF_LIST  (-1)
#define FREENEXT_NOT_IN_LIST  (-2)

/*
 * MapBufferDesc -- shared descriptor/state data for a single map buffer.
 */
typedef struct MapBufferDesc
{
	RelFileLocator rnode;	/* relation identifier */
	ForkNumber	forknum;	/* fork number */
	int			page_number; /* map page number in this slot, -1 if empty */
	XLogRecPtr	page_lsn;	/* LSN of last modification */
	int			id;			/* slot ID */
	pg_atomic_uint32 state; /* state flags */
	int			freeNext;	/* next buffer in free list */
	int			wait_backend_pid;	/* backend PID of pin-count waiter */
	LWLock		buffer_lock;		/* lock for buffer content access */
	LWLock		io_in_progress_lock; /* lock for buffer I/O state */
} MapBufferDesc;

extern void MapBackendInit(void);
extern const ShmemCallbacks MapShmemCallbacks;

/* Buffer management used by active-slot helpers. */
extern int	MapReadBuffer(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  ForkNumber forknum, BlockNumber map_blkno);

/* Chunk-paired active-slot helpers. */
extern bool UmbraShiftGet(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  ForkNumber forknum, BlockNumber lblkno,
						  uint8 *active_slot);
extern BlockNumber UmbraShiftGetRun(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									ForkNumber forknum,
									BlockNumber lblkno,
									BlockNumber maxblocks,
									uint8 *active_slot);
extern void UmbraShiftSet(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  ForkNumber forknum, BlockNumber lblkno,
						  uint8 active_slot, XLogRecPtr map_lsn);
extern void UmbraShiftTruncate(UmbraFileContext *map_ctx,
							   RelFileLocator rnode,
							   ForkNumber forknum,
							   BlockNumber n_lblknos,
							   XLogRecPtr map_lsn);

/* MAP superblock helpers */
extern void MapSBlockInit(UmbraFileContext *map_ctx, RelFileLocator rnode,
						  XLogRecPtr map_lsn);
extern bool MapSBlockEnsureLoaded(UmbraFileContext *map_ctx, RelFileLocator rnode);
extern bool MapSBlockTryGetLogicalNblocks(UmbraFileContext *map_ctx,
										  RelFileLocator rnode,
										  ForkNumber forknum,
										  BlockNumber *nblocks);
extern bool MapSBlockForkExists(UmbraFileContext *map_ctx,
								RelFileLocator rnode,
								ForkNumber forknum);
extern bool MapSBlockTryGetNextFreePhysBlock(UmbraFileContext *map_ctx,
											 RelFileLocator rnode,
											 ForkNumber forknum,
											 BlockNumber *next_free_pblk);
extern bool MapSBlockTryGetPhysicalNblocks(UmbraFileContext *map_ctx,
										   RelFileLocator rnode,
										   ForkNumber forknum,
										   BlockNumber *nblocks);
extern void MapSBlockBumpLogicalNblocks(UmbraFileContext *map_ctx,
										RelFileLocator rnode,
										ForkNumber forknum,
										BlockNumber nblocks,
										XLogRecPtr map_lsn);
extern void MapSBlockBumpPhysicalNblocks(UmbraFileContext *map_ctx,
										 RelFileLocator rnode,
										 ForkNumber forknum,
										 BlockNumber nblocks,
										 XLogRecPtr map_lsn);
extern bool MapSBlockEnsurePhysicalNblocks(UmbraFileContext *map_ctx,
										   RelFileLocator rnode,
										   ForkNumber forknum,
										   BlockNumber nblocks,
										   bool skipFsync);
extern bool MapSBlockEnsurePhysicalNblocksZeroFill(UmbraFileContext *map_ctx,
												   RelFileLocator rnode,
												   ForkNumber forknum,
												   BlockNumber nblocks,
												   bool skipFsync);
extern void MapSBlockBumpNextFreePhysBlock(UmbraFileContext *map_ctx,
										   RelFileLocator rnode,
										   ForkNumber forknum,
										   BlockNumber next_free_pblk,
										   XLogRecPtr map_lsn);
extern void MapSBlockSetLogicalNblocks(UmbraFileContext *map_ctx,
									   RelFileLocator rnode,
									   ForkNumber forknum,
									   BlockNumber nblocks,
									   XLogRecPtr map_lsn);
extern void MapSBlockSetSkipWalPending(UmbraFileContext *map_ctx,
									   RelFileLocator rnode,
									   bool pending,
									   XLogRecPtr map_lsn);
extern bool MapSBlockIsSkipWalPending(UmbraFileContext *map_ctx,
									  RelFileLocator rnode);

/* Checkpoint interface */
extern void MapPreCheckpoint(void);
extern void MapCheckpoint(void);
extern void MapCheckpointRelation(RelFileLocator rnode);
extern void MapCheckpointDatabaseTablespaces(Oid dbid, int ntablespaces,
											 const Oid *tablespace_ids);
extern void MapPostCheckpoint(void);
extern int	MapBgWriterFlush(int max_pages);
extern void MapAbortBufferIO(void);
extern void MapBackendExitCleanup(void);

/* Relation lifecycle */
extern void MapDrop(RelFileLocator rnode);
extern void MapInvalidateRelation(RelFileLocator rnode);
extern void MapInvalidateDatabaseTablespaces(Oid dbid, int ntablespaces,
											 const Oid *tablespace_ids);
extern void MapInvalidateDatabase(Oid dbid);

/* Scan helpers */
extern BlockNumber MapGetLogicalBlockCount(UmbraFileContext *map_ctx,
										   RelFileLocator rnode,
										   ForkNumber forknum);

/* Clock algorithm */
extern int	MapClockGetBuffer(void);
extern void MapClockFreeBuffer(int slot_id);
extern int	MapSyncStart(uint32 *complete_passes, uint32 *num_allocs);
extern void MapStrategyNotifyWriter(int mapwriter_procno);
extern void MapWakeWriter(void);

/* Map cache hash table (in mapclock.c) */
extern int	MapCacheLookup(RelFileLocator rnode, ForkNumber forknum,
						   BlockNumber map_blkno);
extern int	MapCacheInsert(RelFileLocator rnode, ForkNumber forknum,
						   BlockNumber map_blkno, int slot_id);
extern void MapCacheDelete(RelFileLocator rnode, ForkNumber forknum,
						   BlockNumber map_blkno, int slot_id);

/* Buffer pin/unpin */
extern void MapPinBuffer(int slot_id, bool adjust_usage);
extern void MapUnpinBuffer(int slot_id);
extern void MapInvalidateBuffer(int slot_id, RelFileLocator expected_rnode,
								ForkNumber expected_forknum,
								BlockNumber expected_map_blkno);

/* GUCs */
extern int	map_buffers;
extern int	map_superblocks;

/* Global data (defined in map.c) */
extern MapSharedData *MapShared;
extern MapBufferDesc *MapBuffers;
extern char *MapPageData;		/* actual page data (contiguous block) */

#define MapGetPage(slot_id) \
	((MapPage *) (MapPageData + ((slot_id) * BLCKSZ)))

#endif							/* MAP_H */
