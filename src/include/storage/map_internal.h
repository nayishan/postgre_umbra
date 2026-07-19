/*-------------------------------------------------------------------------
 *
 * map_internal.h
 *	  Internal declarations for the Umbra MAP metadata cache.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/storage/map_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_INTERNAL_H
#define MAP_INTERNAL_H

#include "port/atomics.h"
#include "storage/map.h"
#include "storage/spin.h"
#include "storage/ummap.h"
#include "utils/resowner.h"

#define MAP_PAGE_REFCOUNT_MASK	UINT64CONST(0x00000000FFFFFFFF)
#define MAP_PAGE_USAGE_SHIFT	32
#define MAP_PAGE_USAGE_MASK		UINT64CONST(0x0000000700000000)
#define MAP_PAGE_USAGE_ONE		UINT64CONST(0x0000000100000000)
#define MAP_PAGE_TAG_VALID		UINT64CONST(0x0000000800000000)
#define MAP_PAGE_VALID			UINT64CONST(0x0000001000000000)
#define MAP_PAGE_DIRTY			UINT64CONST(0x0000002000000000)
#define MAP_PAGE_IO_IN_PROGRESS UINT64CONST(0x0000004000000000)
#define MAP_PAGE_IO_ERROR		UINT64CONST(0x0000008000000000)
#define MAP_PAGE_NEEDS_FSYNC		UINT64CONST(0x0000010000000000)

#define MAP_PAGE_GET_REFCOUNT(state) ((uint32) ((state) & MAP_PAGE_REFCOUNT_MASK))
#define MAP_PAGE_GET_USAGE(state) \
	((uint32) (((state) & MAP_PAGE_USAGE_MASK) >> MAP_PAGE_USAGE_SHIFT))

#define MAP_PAGE_FREENEXT_END		(-1)
#define MAP_PAGE_FREENEXT_NOT_IN_LIST (-2)
#define MAP_PAGE_PENDING_WORDS \
	((BLCKSZ / sizeof(BlockNumber) + 63) / 64)

typedef struct MapPageTag
{
	RelFileLocatorBackend rlocator;
	BlockNumber map_blkno;
} MapPageTag;

typedef struct MapPagePendingRange
{
	/* Exact target hidden behind a transaction or recovery barrier. */
	UmbraMapRange range;
	ForkNumber	forknum;
	bool		physical_ready;
	bool		recovery_replay;
	bool		recovery_exact;
	bool		valid;
} MapPagePendingRange;

struct MapPageDesc
{
	MapPageTag	tag;
	LWLock		content_lock;
	LWLock		io_lock;
	pg_atomic_uint64 state;
	XLogRecPtr	wal_flush_lsn;
	uint64		pending_bits[MAP_PAGE_PENDING_WORDS];
	MapPagePendingRange pending_range;
	/* Record exact redo may coexist with a longer-lived scratch range. */
	MapPagePendingRange replay_range;
	/* content_lock protects refs; the global count tracks refs > 0 slots. */
	uint32		pending_pin_refs;
	int			slot_id;
	int			free_next;
};

typedef struct MapPagePoolCtl
{
	pg_atomic_uint64 next_victim;
	/* Distinct pending-pinned descriptors, capped to leave one victim. */
	pg_atomic_uint32 pending_reservations;
	slock_t		strategy_lock;
	int			first_free;
	int			nslots;
} MapPagePoolCtl;

extern MapPagePoolCtl *MapPagePoolCtlData;
extern MapPageDesc *MapPageDescriptors;
extern PGIOAlignedBlock *MapPageBlocks;
extern int	MapPageBufferCount;

#define MapPageGetBlock(slot_id) (MapPageBlocks[(slot_id)].data)

extern void MapPageEnsureInitialized(void);
extern void MapPagePoolShmemRequest(void);
extern void MapPagePoolShmemInit(void);
extern void MapPagePoolShmemAttach(void);
extern uint32 MapPageCacheHashCode(const MapPageTag *tag);
extern int	MapPageCachePartition(uint32 hashcode);
extern LWLock *MapPageCachePartitionLock(uint32 hashcode);
extern int	MapPageCacheLookup(const MapPageTag *tag, uint32 hashcode);
extern int	MapPageCacheInsert(const MapPageTag *tag, uint32 hashcode,
							   int slot_id);
extern void MapPageCacheDelete(const MapPageTag *tag, uint32 hashcode,
							   int slot_id);
extern LWLock *MapPageExtensionLock(RelFileLocatorBackend rlocator);
extern int	MapPageClockGetBuffer(void);
extern void MapPageClockFreeBuffer(int slot_id);
extern bool MapPageRegisterPendingPin(MapPageDesc *desc);
extern void MapPageUnregisterPendingPin(MapPageDesc *desc);
extern void MapPagePinBuffer(int slot_id, bool adjust_usage);
extern void MapPageUnpinBuffer(int slot_id);
extern void MapPageRememberPin(int slot_id);
extern void MapPageTransferBufferPin(MapPageBuffer buffer,
									 ResourceOwner old_owner,
									 ResourceOwner new_owner);
extern void MapPageLockBuffer(MapPageBuffer buffer, LWLockMode mode);
extern void MapPageUnlockBufferKeepPin(MapPageBuffer buffer);
extern void MapPageReleaseBufferOwned(MapPageBuffer buffer,
									  ResourceOwner owner);
extern void MapPageReleasePendingBufferOwned(MapPageBuffer buffer,
										 ResourceOwner owner);
extern bool MapPageTryClaimBuffer(int slot_id);
extern void MapPageReleaseClaimBuffer(int slot_id);
extern void MapPageUpdateState(MapPageDesc *desc, uint64 set_bits,
							   uint64 clear_bits);
extern bool MapPageFlushBuffer(int slot_id, const MapPageTag *expected_tag,
							   UmbraFileContext *ctx, bool conditional);
extern void MapPageFlushRelation(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator);
extern void MapPageFlushDatabase(Oid dbid, Oid spcOid);
extern void MapPageFlushAll(void);
extern void MapPageInvalidateRelation(RelFileLocatorBackend rlocator);
extern void MapPageInvalidateDatabase(Oid dbid, Oid spcOid);

#endif							/* MAP_INTERNAL_H */
