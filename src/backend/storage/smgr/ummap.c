/*-------------------------------------------------------------------------
 *
 * ummap.c
 *	  Umbra private map fork container.
 *
 * This file owns the private relation-local map fork used by Umbra.  Block 0
 * stores a relation-wide disk root, and ordinary MAP pages follow it.  MAP
 * canonical entries remain the authoritative logical-to-physical
 * translations; the root stores only logical and physical frontiers.
 *
 * WAL recovery may hold an exact mapping or a disposable scratch mapping
 * behind a pinned visibility barrier.  Neither is canonical: exact mappings
 * are published only after their whole WAL record completes, while scratch
 * mappings must be removed by later storage lifecycle WAL.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/ummap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/map.h"
#include "storage/map_internal.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#define UMMAP_ROOT_MAGIC		0x554D4252U	/* "UMBR" */
#define UMMAP_ROOT_VERSION		2U
#define UMMAP_ROOT_FLAG_FORMAT_V2 0x00000002U

#define UMMAP_BLOCK_ROOT			0
#define UMMAP_BLOCK_FIRST_GROUP	1

#define UMMAP_ENTRIES_PER_PAGE	(BLCKSZ / sizeof(BlockNumber))

int			map_prealloc_main_low = 512;
int			map_prealloc_main_hard = 128;
int			map_prealloc_main_batch = 1024;
int			map_prealloc_fsm_low = 64;
int			map_prealloc_fsm_hard = 16;
int			map_prealloc_fsm_batch = 128;
int			map_prealloc_vm_low = 64;
int			map_prealloc_vm_hard = 16;
int			map_prealloc_vm_batch = 128;

/*
 * Fixed map layout, following the rebase branch's proportional grouping:
 *
 *	block 0: disk root
 *	block 1..: [FSM map page][VM map page][8192 MAIN map pages]
 */
#define UMMAP_GROUP_FSM_PAGES	1
#define UMMAP_GROUP_VM_PAGES	1
#define UMMAP_GROUP_MAIN_PAGES	8192
#define UMMAP_GROUP_TOTAL_PAGES \
	(UMMAP_GROUP_FSM_PAGES + UMMAP_GROUP_VM_PAGES + UMMAP_GROUP_MAIN_PAGES)

StaticAssertDecl((BLCKSZ % sizeof(BlockNumber)) == 0,
					 "BLCKSZ must be a multiple of BlockNumber");
StaticAssertDecl(BLCKSZ >= UMMAP_ROOT_SECTOR_SIZE,
					 "BLCKSZ must hold an Umbra MAP root sector");
StaticAssertDecl(UMMAP_ROOT_SECTOR_SIZE >= UMMAP_ROOT_IMAGE_SIZE,
					 "Umbra MAP root sector must hold its payload");

/* The CRC covers this fixed 64-byte payload; block padding remains zero. */
typedef struct pg_attribute_packed() UmbraMapRootData
{
	uint32		magic;
	uint32		version;
	uint32		blcksz;
	uint32		flags;

	BlockNumber logical_eof_main;
	BlockNumber logical_eof_fsm;
	BlockNumber logical_eof_vm;
	BlockNumber physical_frontier_main;
	BlockNumber physical_frontier_fsm;
	BlockNumber physical_frontier_vm;

	/* Immutable identity of the MAIN CREATE record that owns this storage. */
	XLogRecPtr	generation_lsn;

	/* Addressable file ranges; allocation frontiers above remain independent. */
	BlockNumber physical_capacity_main;
	BlockNumber physical_capacity_fsm;
	BlockNumber physical_capacity_vm;
	pg_crc32c	crc;
} UmbraMapRootData;

StaticAssertDecl(sizeof(UmbraMapRootData) == UMMAP_ROOT_IMAGE_SIZE,
					 "Umbra MAP root payload size is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, generation_lsn) == 40,
					 "Umbra MAP root generation offset is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, physical_capacity_main) == 48,
					 "Umbra MAP root capacity offset is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, crc) == 60,
					 "Umbra MAP root CRC offset is wrong");

typedef struct UmbraMapPendingPage
{
	int			slot_id;
	BlockNumber first_lblkno;
	BlockNumber nblocks;
} UmbraMapPendingPage;

typedef struct UmbraMapPendingRange
{
	UmbraFileContext *ctx;
	RelFileLocatorBackend rlocator;
	ForkNumber	forknum;
	UmbraMapRange range;
	BlockNumber anchor_lblkno;
	SubTransactionId subxid;
	ResourceOwner pin_owner;
	bool		wal_only;
	bool		entries_installed;
	bool		barrier_installed;
	bool		wal_ready;
	XLogRecPtr	wal_lsn;
	bool		physical_ready;
	bool		map_published;
	bool		in_record;
	int			npages;
	int			npinned;
	struct UmbraMapPendingRange *next;
	UmbraMapPendingPage pages[FLEXIBLE_ARRAY_MEMBER];
} UmbraMapPendingRange;

typedef struct UmbraMapRecoveryPending
{
	UmbraFileContext *ctx;
	RelFileLocatorBackend rlocator;
	ForkNumber	forknum;
	UmbraMapRange range;
	BlockNumber old_pblkno;
	XLogRecPtr	lsn;
	ResourceOwner owner;
	int			slot_id;
	bool		exact;
	bool		physical_ready;
	bool		zero_baseline;
	bool		use_old_pblkno;
	bool		barrier_installed;
	bool		resource_remembered;
	struct UmbraMapRecoveryPending *next;
} UmbraMapRecoveryPending;

static UmbraMapPendingRange *ummap_pending_ranges = NULL;
static UmbraMapRecoveryPending *ummap_replay_ranges = NULL;
static UmbraMapRecoveryPending *ummap_recovery_scratch = NULL;
static ResourceOwner ummap_recovery_resowner = NULL;

static void ummap_root_init(char *image);
static void ummap_root_refresh_crc(UmbraMapRootData *root);
static bool ummap_root_is_valid(const UmbraMapRootData *root);
static void ummap_root_get_frontiers(const UmbraMapRootData *root,
									 ForkNumber forknum,
									 BlockNumber *logical_eof,
									 BlockNumber *physical_frontier);
static void ummap_root_set_frontiers(UmbraMapRootData *root,
										 ForkNumber forknum,
										 BlockNumber logical_eof,
										 BlockNumber physical_frontier);
static BlockNumber ummap_root_get_capacity(const UmbraMapRootData *root,
											ForkNumber forknum);
static void ummap_root_set_capacity(UmbraMapRootData *root,
									  ForkNumber forknum,
									  BlockNumber physical_capacity);
static bool ummap_prealloc_settings(ForkNumber forknum,
								   BlockNumber *soft_low,
								   BlockNumber *hard_low,
								   BlockNumber *batch_blocks);
static bool ummap_root_reserve_buffer(MapSuperBuffer buffer,
	ForkNumber forknum, BlockNumber physical_floor, BlockNumber nblocks,
	bool skipFsync, BlockNumber *first_pblkno);
static bool ummap_try_reserve_physical_block(
	RelFileLocatorBackend rlocator, MapSuperDesc *root_desc, ForkNumber forknum,
	BlockNumber *pblkno, XLogRecPtr *generation_lsn,
	LWLock **held_extension_lock);
static BlockNumber ummap_fork_page_index_to_map_blkno(ForkNumber forknum,
											  BlockNumber fork_page_idx);
static uint64 ummap_existing_fork_pages(ForkNumber forknum,
										BlockNumber map_nblocks);
static BlockNumber ummap_map_blkno(ForkNumber forknum, BlockNumber lblkno);
static int	ummap_entry_index(BlockNumber lblkno);
static BlockNumber ummap_page_run_limit(BlockNumber lblkno,
										BlockNumber maxblocks);
static BlockNumber ummap_page_get_entry(char *page, int entry_idx);
static void ummap_page_set_entry(char *page, int entry_idx,
								 BlockNumber pblkno);
static void ummap_page_clear_entry(char *page, int entry_idx);
static void ummap_replay_mapping_range(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	const UmbraMapRange *range, XLogRecPtr lsn);
static void ummap_truncate_locked(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	BlockNumber old_nblocks, BlockNumber new_nblocks,
	XLogRecPtr wal_flush_lsn, bool skipFsync);
static bool ummap_range_contains(const UmbraMapRange *range,
								BlockNumber lblkno);
static void ummap_validate_range(const UmbraMapRange *range);
static bool ummap_pending_replay_uses_slot(int slot_id);
static UmbraMapPendingRange *ummap_find_pending_range(
	RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber lblkno,
	UmbraMapPendingRange ***prev_next);
static bool ummap_prepare_pending_range(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	BlockNumber first_lblkno, BlockNumber first_pblkno, BlockNumber nblocks,
	BlockNumber anchor_lblkno, bool wal_only, bool physical_ready,
	bool install_entries);
static void ummap_release_pending_range(UmbraMapPendingRange *pending,
												bool publish);
static void ummap_release_pending_state(UmbraMapPendingRange *pending,
	bool publish);
static void ummap_publish_pending_range(UmbraMapPendingRange *pending);
static bool ummap_has_earlier_pending_range(UmbraMapPendingRange *pending);
static uint64 ummap_pending_pin_count(void);
static void ummap_publish_ready_ranges_internal(bool include_local);
static BlockNumber ummap_advance_local_pending_eof(
	RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber eof);
static BlockNumber ummap_scan_nblocks(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	BlockNumber logical_nblocks);
static bool ummap_lookup_local_pending(RelFileLocatorBackend rlocator,
	ForkNumber forknum, BlockNumber lblkno, BlockNumber maxblocks,
	BlockNumber *pblkno, BlockNumber *nblocks, bool *physical_ready);
static bool ummap_lookup_shared_pending(RelFileLocatorBackend rlocator,
	ForkNumber forknum, BlockNumber lblkno, BlockNumber maxblocks,
	BlockNumber *pblkno, BlockNumber *nblocks, bool *physical_ready);
static bool ummap_lookup_recovery_pending(RelFileLocatorBackend rlocator,
	ForkNumber forknum, BlockNumber lblkno, BlockNumber maxblocks,
	BlockNumber *pblkno, BlockNumber *nblocks);
static bool ummap_local_pending_contains(RelFileLocatorBackend rlocator,
	ForkNumber forknum, BlockNumber lblkno);
static BlockNumber ummap_lookup_run_internal(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber lblkno,
	BlockNumber maxblocks, BlockNumber *pblkno, bool allow_pending);
static ResourceOwner ummap_get_recovery_resowner(void);
static UmbraMapRecoveryPending *ummap_prepare_recovery_pending(
	UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
	ForkNumber forknum, const UmbraMapRange *range, BlockNumber old_pblkno,
	XLogRecPtr lsn, bool exact, bool physical_ready, bool zero_baseline);
static void ummap_release_recovery_pending(
	UmbraMapRecoveryPending *pending, bool owner_release);
static void ummap_release_recovery_resource(Datum res);
static void ummap_unlink_recovery_pending(UmbraMapRecoveryPending *pending);
static void ummap_update_recovery_barrier(
	UmbraMapRecoveryPending *pending);

static const ResourceOwnerDesc ummap_recovery_resowner_desc =
{
	.name = "Umbra recovery MAP pending range",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	/* Release after MAP I/O (100), but before the associated pin (200). */
	.release_priority = 150,
	.ReleaseResource = ummap_release_recovery_resource,
	.DebugPrint = NULL
};

RelPathStr
ummap_relpath(RelFileLocatorBackend rlocator)
{
	RelPathStr	base;
	RelPathStr	path;

	base = relpath(rlocator, MAIN_FORKNUM);
	snprintf(path.str, sizeof(path.str), "%s_map", base.str);
	return path;
}

bool
ummap_exists(UmbraFileContext *ctx)
{
	return umfile_exists(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_create(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			 bool isRedo)
{
	LWLock	   *root_lock;

	MapPageEnsureInitialized();
	root_lock = MapPageExtensionLock(rlocator);
	LWLockAcquire(root_lock, LW_EXCLUSIVE);
	umfile_create(ctx, UMBRA_MAP_FORKNUM, isRedo);
	if (umfile_nblocks(ctx, UMBRA_MAP_FORKNUM) == 0)
	{
		char		image[UMMAP_ROOT_IMAGE_SIZE];

		ummap_root_init(image);
		ummap_root_write_image(ctx, image, false);
	}
	LWLockRelease(root_lock);
}

bool
ummap_is_empty(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	char		empty_image[UMMAP_ROOT_IMAGE_SIZE];
	MapSuperBuffer buffer;
	bool		empty;

	if (umfile_nblocks(ctx, UMBRA_MAP_FORKNUM) != 1)
		return false;

	ummap_root_init(empty_image);
	buffer = MapSuperBufferRead(ctx, rlocator, LW_SHARED);
	empty = memcmp(MapSuperBufferGetData(buffer), empty_image,
				   UMMAP_ROOT_IMAGE_SIZE) == 0;
	MapSuperReleaseBuffer(buffer);
	return empty;
}

XLogRecPtr
ummap_get_generation_lsn(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;
	XLogRecPtr	generation_lsn;

	buffer = MapSuperBufferRead(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	generation_lsn = root->generation_lsn;
	MapSuperReleaseBuffer(buffer);
	return generation_lsn;
}

void
ummap_set_generation_lsn(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 XLogRecPtr generation_lsn)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;
	bool		immediate_sync = InRecovery;

	Assert(XLogRecPtrIsValid(generation_lsn));
	buffer = MapSuperBufferRead(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	if (XLogRecPtrIsValid(root->generation_lsn) &&
		root->generation_lsn != generation_lsn)
	{
		MapSuperReleaseBuffer(buffer);
		elog(PANIC, "Umbra MAP belongs to a different storage generation");
	}
	if (root->generation_lsn != generation_lsn)
	{
		root->generation_lsn = generation_lsn;
		ummap_root_refresh_crc(root);
		MapSuperMarkBufferDirty(buffer, false, generation_lsn);
		/* A replayed CREATE marker must survive another physical crash. */
		if (InRecovery)
			MapSuperFlushLocked(buffer.desc, ctx);
	}
	MapSuperReleaseBuffer(buffer);
	if (immediate_sync)
		ummap_immedsync_if_exists(ctx);
}

void
ummap_root_read_frontiers(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator, ForkNumber forknum,
						  BlockNumber *logical_eof,
						  BlockNumber *physical_frontier)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;

	Assert(logical_eof != NULL);
	Assert(physical_frontier != NULL);
	buffer = MapSuperBufferRead(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	ummap_root_get_frontiers(root, forknum,
							 logical_eof, physical_frontier);
	MapSuperReleaseBuffer(buffer);
}

void
ummap_root_read_capacity(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator, ForkNumber forknum,
						 BlockNumber *physical_frontier,
						 BlockNumber *physical_capacity)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;

	Assert(physical_frontier != NULL);
	Assert(physical_capacity != NULL);
	buffer = MapSuperBufferRead(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	ummap_root_get_frontiers(root, forknum, NULL, physical_frontier);
	*physical_capacity = ummap_root_get_capacity(root, forknum);
	MapSuperReleaseBuffer(buffer);
}

/* Advance both frontiers monotonically in the resident root. */
void
ummap_root_advance(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					   ForkNumber forknum, BlockNumber logical_end,
					   BlockNumber physical_end, XLogRecPtr wal_flush_lsn,
					   bool skipFsync)
{
	MapSuperBuffer buffer;
	MapSuperDesc *desc = NULL;
	MapSuperTag tag = {0};
	UmbraMapRootData *root;
	BlockNumber old_logical;
	BlockNumber old_physical;
	BlockNumber old_capacity;
	bool		dirty = false;

	Assert(BlockNumberIsValid(logical_end));
	Assert(BlockNumberIsValid(physical_end));
	if (CritSectionCount > 0)
	{
		/* Reservation already loaded this entry; publication must not do I/O. */
		tag.rlocator = rlocator;
		if (!MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE, &desc) ||
			!desc->valid)
		{
			if (desc != NULL)
				LWLockRelease(&desc->content_lock);
			elog(PANIC,
				 "Umbra resident MAP root disappeared during WAL publication");
		}
		buffer.desc = desc;
	}
	else
		buffer = MapSuperBufferRead(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	ummap_root_get_frontiers(root, forknum, &old_logical,
								 &old_physical);
	old_capacity = ummap_root_get_capacity(root, forknum);
	if (!BlockNumberIsValid(old_logical) || logical_end > old_logical)
	{
		old_logical = logical_end;
		dirty = true;
	}
	if (!BlockNumberIsValid(old_physical) || physical_end > old_physical)
	{
		old_physical = physical_end;
		dirty = true;
	}
	if (!BlockNumberIsValid(old_capacity) || physical_end > old_capacity)
	{
		old_capacity = physical_end;
		dirty = true;
	}
	if (dirty)
	{
		ummap_root_set_frontiers(root, forknum, old_logical, old_physical);
		ummap_root_set_capacity(root, forknum, old_capacity);
		ummap_root_refresh_crc(root);
		MapSuperMarkBufferDirty(buffer, skipFsync, wal_flush_lsn);
	}
	MapSuperReleaseBuffer(buffer);
}

void
ummap_root_advance_capacity(UmbraFileContext *ctx,
							RelFileLocatorBackend rlocator, ForkNumber forknum,
							BlockNumber physical_capacity, bool skipFsync)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;
	BlockNumber old_capacity;

	Assert(BlockNumberIsValid(physical_capacity));
	buffer = MapSuperBufferRead(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	old_capacity = ummap_root_get_capacity(root, forknum);
	if (!BlockNumberIsValid(old_capacity) || physical_capacity > old_capacity)
	{
		ummap_root_set_capacity(root, forknum, physical_capacity);
		ummap_root_refresh_crc(root);
		MapSuperMarkBufferDirty(buffer, skipFsync, InvalidXLogRecPtr);
	}
	MapSuperReleaseBuffer(buffer);
}

bool
ummap_maybe_preallocate(UmbraFileContext *ctx,
						RelFileLocatorBackend rlocator, ForkNumber forknum,
						bool background_mode)
{
	UmbraFileContext *volatile temporary_ctx = NULL;
	MapSuperTag tag = {0};
	MapSuperDesc *desc = NULL;
	UmbraMapRootData *root;
	BlockNumber soft_low;
	BlockNumber hard_low;
	BlockNumber batch_blocks;
	BlockNumber frontier;
	BlockNumber capacity;
	BlockNumber remaining;
	BlockNumber target = InvalidBlockNumber;
	XLogRecPtr	generation_lsn = InvalidXLogRecPtr;
	LWLock	   *extension_lock;
	uint64		target64;
	bool		preallocated = false;
	bool		wake_writer = false;

	if (InRecovery || RelFileLocatorBackendIsTemp(rlocator) ||
		!ummap_tracks_fork(forknum) ||
		!ummap_prealloc_settings(forknum, &soft_low, &hard_low,
								 &batch_blocks))
		return false;

	tag.rlocator = rlocator;
	extension_lock = MapPageExtensionLock(rlocator);

	PG_TRY();
	{
		UmbraFileContext *prealloc_ctx = ctx;

		/*
		 * Keep the root locked while native preallocation mutates the file.  Root
		 * invalidation is the lifecycle interlock for DROP/recreate, while the
		 * extension lock serializes ordinary reservations for this locator.
		 */
		LWLockAcquire(extension_lock, LW_EXCLUSIVE);
		if (MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE,
									&desc))
		{
			root = (UmbraMapRootData *) desc->data;
			if (desc->valid && ummap_root_is_valid(root))
			{
				ummap_root_get_frontiers(root, forknum, NULL, &frontier);
				capacity = ummap_root_get_capacity(root, forknum);
				if (BlockNumberIsValid(frontier) &&
					BlockNumberIsValid(capacity) && frontier >= soft_low)
				{
					remaining = capacity > frontier ? capacity - frontier : 0;
					if (remaining <= soft_low)
					{
						if (!background_mode && remaining > hard_low)
							wake_writer = true;
						else
						{
							target64 = Max((uint64) capacity + batch_blocks,
										   (uint64) frontier + batch_blocks);
							if (target64 < (uint64) InvalidBlockNumber)
							{
								target = (BlockNumber) target64;
								generation_lsn = root->generation_lsn;
							}
						}
					}
				}
			}

			if (BlockNumberIsValid(target) &&
				XLogRecPtrIsValid(generation_lsn))
			{
				if (prealloc_ctx == NULL)
				{
					temporary_ctx = umfile_open_temporary(rlocator);
					prealloc_ctx = temporary_ctx;
				}
				preallocated = umfile_preallocate(prealloc_ctx, forknum,
												 target, false);
				if (preallocated && desc->valid &&
					root->generation_lsn == generation_lsn &&
					target > ummap_root_get_capacity(root, forknum))
				{
					MapSuperBuffer buffer = {.desc = desc};

					ummap_root_set_capacity(root, forknum, target);
					ummap_root_refresh_crc(root);
					MapSuperMarkBufferDirty(buffer, false,
										InvalidXLogRecPtr);
				}
			}
			LWLockRelease(&desc->content_lock);
		}
		if (temporary_ctx != NULL)
		{
			umfile_destroy(temporary_ctx);
			temporary_ctx = NULL;
		}
		LWLockRelease(extension_lock);
	}
	PG_CATCH();
	{
		if (temporary_ctx != NULL)
			umfile_destroy(temporary_ctx);
		/* Standard ERROR cleanup releases the root and extension LWLocks. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (wake_writer)
		MapWakeWriter();
	return preallocated;
}

BlockNumber
ummap_reserve_physical_run(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator, ForkNumber forknum,
							   BlockNumber nblocks, bool skipFsync)
{
	BlockNumber first_pblkno = InvalidBlockNumber;
	BlockNumber physical_nblocks;
	uint64		required_nblocks;
	LWLock	   *extension_lock;

	Assert(ctx != NULL);
	Assert(ummap_tracks_fork(forknum));
	Assert(nblocks > 0);
	extension_lock = MapPageExtensionLock(rlocator);
	LWLockAcquire(extension_lock, LW_EXCLUSIVE);
	{
		MapSuperBuffer root_buffer;

		(void) MapReclaimRegisterReservation(rlocator, forknum);
		physical_nblocks = umfile_nblocks(ctx, forknum);
		root_buffer = MapSuperBufferRead(ctx, rlocator, LW_EXCLUSIVE);
		if (!ummap_root_reserve_buffer(root_buffer, forknum,
									   physical_nblocks, nblocks, skipFsync,
									   &first_pblkno))
		{
			MapSuperReleaseBuffer(root_buffer);
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cannot reserve another Umbra physical block")));
		}
		MapSuperReleaseBuffer(root_buffer);

		required_nblocks = (uint64) first_pblkno + nblocks;
		while ((uint64) physical_nblocks < required_nblocks)
		{
			int			chunk = (int) Min(required_nblocks - physical_nblocks,
										 (uint64) INT_MAX);

			umfile_zeroextend(ctx, forknum, physical_nblocks, chunk,
								  skipFsync);
			physical_nblocks += chunk;
		}
		ummap_root_advance_capacity(ctx, rlocator, forknum,
								physical_nblocks, skipFsync);
	}
	LWLockRelease(extension_lock);
	(void) ummap_maybe_preallocate(ctx, rlocator, forknum, false);

	return first_pblkno;
}

/* Reserve one P using only locks and an already-resident root. */
static bool
ummap_try_reserve_physical_block(RelFileLocatorBackend rlocator,
								 MapSuperDesc *root_desc, ForkNumber forknum,
								 BlockNumber *pblkno,
								 XLogRecPtr *generation_lsn,
								 LWLock **held_extension_lock)
{
	MapSuperBuffer root_buffer;
	UmbraMapRootData *root;
	LWLock	   *extension_lock;
	bool		reserved = false;

	Assert(ummap_tracks_fork(forknum));
	Assert(pblkno != NULL);
	Assert(generation_lsn != NULL);
	Assert(held_extension_lock != NULL);
	*generation_lsn = InvalidXLogRecPtr;
	*held_extension_lock = NULL;
	if (root_desc == NULL || !MapPagePoolIsInitialized())
		return false;

	extension_lock = MapPageExtensionLock(rlocator);
	if (!LWLockConditionalAcquire(extension_lock, LW_EXCLUSIVE))
		return false;
	if (LWLockConditionalAcquire(&root_desc->content_lock, LW_EXCLUSIVE))
	{
		if (root_desc->valid &&
			RelFileLocatorBackendEquals(root_desc->tag.rlocator, rlocator))
		{
			root_buffer.desc = root_desc;
			root = (UmbraMapRootData *) MapSuperBufferGetData(root_buffer);
			reserved = ummap_root_reserve_buffer(root_buffer, forknum,
											InvalidBlockNumber, 1, false, pblkno);
			if (reserved)
				*generation_lsn = root->generation_lsn;
		}
		LWLockRelease(&root_desc->content_lock);
	}
	if (reserved)
	{
		/* The caller keeps reclaim out until the shared pending range is visible. */
		*held_extension_lock = extension_lock;
		MapWakeWriter();
	}
	else
		LWLockRelease(extension_lock);
	return reserved;
}

/* content_lock and the relation extension lock serialize this allocation. */
static bool
ummap_root_reserve_buffer(MapSuperBuffer buffer, ForkNumber forknum,
						  BlockNumber physical_floor, BlockNumber nblocks,
						  bool skipFsync, BlockNumber *first_pblkno)
{
	UmbraMapRootData *root;
	BlockNumber logical_eof;
	BlockNumber physical_frontier;
	BlockNumber physical_capacity;
	uint64		physical_end;
	bool		dirty = false;

	Assert(buffer.desc != NULL);
	Assert(nblocks > 0);
	Assert(first_pblkno != NULL);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	ummap_root_get_frontiers(root, forknum, &logical_eof,
								 &physical_frontier);
	physical_capacity = ummap_root_get_capacity(root, forknum);
	if (!BlockNumberIsValid(logical_eof) ||
		!BlockNumberIsValid(physical_frontier))
		return false;
	if (BlockNumberIsValid(physical_floor) &&
		physical_floor > physical_capacity)
	{
		/* File growth beyond recorded capacity has unknown ownership. */
		physical_frontier = Max(physical_frontier, physical_floor);
		physical_capacity = physical_floor;
		dirty = true;
	}
	physical_end = (uint64) physical_frontier + nblocks;
	if (physical_end >= (uint64) InvalidBlockNumber)
		return false;

	*first_pblkno = physical_frontier;
	ummap_root_set_frontiers(root, forknum, logical_eof,
							  (BlockNumber) physical_end);
	if (dirty)
		ummap_root_set_capacity(root, forknum, physical_capacity);
	ummap_root_refresh_crc(root);
	MapSuperMarkBufferDirty(buffer, skipFsync, InvalidXLogRecPtr);
	return true;
}

/* Set logical EOF exactly, while keeping the physical frontier monotonic. */
void
ummap_root_set_logical(UmbraFileContext *ctx,
					   RelFileLocatorBackend rlocator, ForkNumber forknum,
					   BlockNumber logical_eof,
					   BlockNumber physical_floor,
					   XLogRecPtr wal_flush_lsn, bool skipFsync)
{
	MapSuperBuffer buffer;
	UmbraMapRootData *root;
	BlockNumber old_logical;
	BlockNumber old_physical;
	BlockNumber old_capacity;
	bool		dirty;

	Assert(BlockNumberIsValid(logical_eof));
	Assert(BlockNumberIsValid(physical_floor));
	buffer = MapSuperBufferRead(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) MapSuperBufferGetData(buffer);
	ummap_root_get_frontiers(root, forknum, &old_logical,
							 &old_physical);
	old_capacity = ummap_root_get_capacity(root, forknum);
	dirty = logical_eof != old_logical;
	if (!BlockNumberIsValid(old_physical) ||
		physical_floor > old_physical)
	{
		old_physical = physical_floor;
		dirty = true;
	}
	if (!BlockNumberIsValid(old_capacity) ||
		physical_floor > old_capacity)
	{
		old_capacity = physical_floor;
		dirty = true;
	}
	if (dirty)
	{
		ummap_root_set_frontiers(root, forknum, logical_eof, old_physical);
		ummap_root_set_capacity(root, forknum, old_capacity);
		ummap_root_refresh_crc(root);
		MapSuperMarkBufferDirty(buffer, skipFsync, wal_flush_lsn);
	}
	MapSuperReleaseBuffer(buffer);
}

static void
ummap_root_init(char *image)
{
	UmbraMapRootData *root;

	Assert(image != NULL);
	MemSet(image, 0, UMMAP_ROOT_IMAGE_SIZE);
	root = (UmbraMapRootData *) image;
	root->magic = UMMAP_ROOT_MAGIC;
	root->version = UMMAP_ROOT_VERSION;
	root->blcksz = BLCKSZ;
	root->flags = UMMAP_ROOT_FLAG_FORMAT_V2;
	root->logical_eof_main = 0;
	root->logical_eof_fsm = InvalidBlockNumber;
	root->logical_eof_vm = InvalidBlockNumber;
	root->physical_frontier_main = 0;
	root->physical_frontier_fsm = InvalidBlockNumber;
	root->physical_frontier_vm = InvalidBlockNumber;
	root->physical_capacity_main = 0;
	root->physical_capacity_fsm = InvalidBlockNumber;
	root->physical_capacity_vm = InvalidBlockNumber;
	ummap_root_refresh_crc(root);
}

static void
ummap_root_refresh_crc(UmbraMapRootData *root)
{
	pg_crc32c	crc;

	Assert(root != NULL);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, root, offsetof(UmbraMapRootData, crc));
	FIN_CRC32C(crc);
	root->crc = crc;
}

static bool
ummap_root_is_valid(const UmbraMapRootData *root)
{
	pg_crc32c	crc;

	Assert(root != NULL);
	if (root->magic != UMMAP_ROOT_MAGIC ||
		root->version != UMMAP_ROOT_VERSION ||
		root->blcksz != BLCKSZ ||
		root->flags != UMMAP_ROOT_FLAG_FORMAT_V2 ||
		!BlockNumberIsValid(root->logical_eof_main) ||
		!BlockNumberIsValid(root->physical_frontier_main) ||
		!BlockNumberIsValid(root->physical_capacity_main) ||
		BlockNumberIsValid(root->logical_eof_fsm) !=
		BlockNumberIsValid(root->physical_frontier_fsm) ||
		BlockNumberIsValid(root->physical_frontier_fsm) !=
		BlockNumberIsValid(root->physical_capacity_fsm) ||
		BlockNumberIsValid(root->logical_eof_vm) !=
		BlockNumberIsValid(root->physical_frontier_vm) ||
		BlockNumberIsValid(root->physical_frontier_vm) !=
		BlockNumberIsValid(root->physical_capacity_vm))
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, root, offsetof(UmbraMapRootData, crc));
	FIN_CRC32C(crc);
	return crc == root->crc;
}

void
ummap_root_load_image(UmbraFileContext *ctx, char *image)
{
	char		sector[UMMAP_ROOT_SECTOR_SIZE];
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(image != NULL);
	if (umfile_nblocks(ctx, UMBRA_MAP_FORKNUM) <= UMMAP_BLOCK_ROOT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("missing Umbra map superblock")));

	umfile_read_bytes(ctx, UMBRA_MAP_FORKNUM, UMMAP_BLOCK_ROOT,
					  sector, UMMAP_ROOT_SECTOR_SIZE);
	if (!pg_memory_is_all_zeros(sector + UMMAP_ROOT_IMAGE_SIZE,
								UMMAP_ROOT_SECTOR_SIZE -
								UMMAP_ROOT_IMAGE_SIZE) ||
		!ummap_root_is_valid((UmbraMapRootData *) sector))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Umbra map superblock is corrupted")));
	memcpy(image, sector, UMMAP_ROOT_IMAGE_SIZE);

	/*
	 * The disk root may safely lag physical writes.  Reconcile the resident
	 * allocation frontier before it can hand out a new physical block.
	 */
	root = (UmbraMapRootData *) image;
	for (ForkNumber forknum = MAIN_FORKNUM; forknum <= VISIBILITYMAP_FORKNUM;
		 forknum++)
	{
		BlockNumber physical_nblocks;
		BlockNumber logical_eof;
		BlockNumber physical_frontier;
		BlockNumber physical_capacity;

		ummap_root_get_frontiers(root, forknum, &logical_eof,
								 &physical_frontier);
		if (!BlockNumberIsValid(logical_eof) || !umfile_exists(ctx, forknum))
			continue;
		physical_nblocks = umfile_nblocks(ctx, forknum);
		physical_capacity = ummap_root_get_capacity(root, forknum);

		if (physical_nblocks > physical_capacity)
		{
			/* Unknown file growth cannot be handed out again after restart. */
			physical_frontier = Max(physical_frontier, physical_nblocks);
			ummap_root_set_frontiers(root, forknum, logical_eof,
									  physical_frontier);
			ummap_root_set_capacity(root, forknum, physical_nblocks);
			ummap_root_refresh_crc(root);
		}
	}
}

void
ummap_root_write_image(UmbraFileContext *ctx, const char *image,
						   bool skipFsync)
{
	char		sector[UMMAP_ROOT_SECTOR_SIZE];
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(image != NULL);
	MemSet(sector, 0, sizeof(sector));
	memcpy(sector, image, UMMAP_ROOT_IMAGE_SIZE);
	root = (UmbraMapRootData *) sector;
	ummap_root_refresh_crc(root);
	Assert(ummap_root_is_valid(root));
	umfile_write_bytes(ctx, UMBRA_MAP_FORKNUM, UMMAP_BLOCK_ROOT,
					   sector, UMMAP_ROOT_SECTOR_SIZE, skipFsync);
}

static void
ummap_root_get_frontiers(const UmbraMapRootData *root, ForkNumber forknum,
						 BlockNumber *logical_eof,
						 BlockNumber *physical_frontier)
{
	BlockNumber logical;
	BlockNumber physical;

	Assert(root != NULL);
	switch (forknum)
	{
		case MAIN_FORKNUM:
			logical = root->logical_eof_main;
			physical = root->physical_frontier_main;
			break;
		case FSM_FORKNUM:
			logical = root->logical_eof_fsm;
			physical = root->physical_frontier_fsm;
			break;
		case VISIBILITYMAP_FORKNUM:
			logical = root->logical_eof_vm;
			physical = root->physical_frontier_vm;
			break;
		default:
			elog(ERROR, "unsupported fork number for Umbra MAP root: %d",
				 (int) forknum);
	}
	if (logical_eof != NULL)
		*logical_eof = logical;
	if (physical_frontier != NULL)
		*physical_frontier = physical;
}

static void
ummap_root_set_frontiers(UmbraMapRootData *root, ForkNumber forknum,
						 BlockNumber logical_eof,
						 BlockNumber physical_frontier)
{
	Assert(root != NULL);
	Assert(BlockNumberIsValid(logical_eof));
	Assert(BlockNumberIsValid(physical_frontier));
	switch (forknum)
	{
		case MAIN_FORKNUM:
			root->logical_eof_main = logical_eof;
			root->physical_frontier_main = physical_frontier;
			break;
		case FSM_FORKNUM:
			root->logical_eof_fsm = logical_eof;
			root->physical_frontier_fsm = physical_frontier;
			break;
		case VISIBILITYMAP_FORKNUM:
			root->logical_eof_vm = logical_eof;
			root->physical_frontier_vm = physical_frontier;
			break;
		default:
			elog(ERROR, "unsupported fork number for Umbra MAP root: %d",
				 (int) forknum);
	}
}

static BlockNumber
ummap_root_get_capacity(const UmbraMapRootData *root, ForkNumber forknum)
{
	Assert(root != NULL);
	switch (forknum)
	{
		case MAIN_FORKNUM:
			return root->physical_capacity_main;
		case FSM_FORKNUM:
			return root->physical_capacity_fsm;
		case VISIBILITYMAP_FORKNUM:
			return root->physical_capacity_vm;
		default:
			elog(ERROR, "unsupported fork number for Umbra MAP capacity: %d",
				 (int) forknum);
	}
	pg_unreachable();
}

static void
ummap_root_set_capacity(UmbraMapRootData *root, ForkNumber forknum,
						BlockNumber physical_capacity)
{
	Assert(root != NULL);
	switch (forknum)
	{
		case MAIN_FORKNUM:
			root->physical_capacity_main = physical_capacity;
			break;
		case FSM_FORKNUM:
			root->physical_capacity_fsm = physical_capacity;
			break;
		case VISIBILITYMAP_FORKNUM:
			root->physical_capacity_vm = physical_capacity;
			break;
		default:
			elog(ERROR, "unsupported fork number for Umbra MAP capacity: %d",
				 (int) forknum);
	}
}

static bool
ummap_prealloc_settings(ForkNumber forknum, BlockNumber *soft_low,
							BlockNumber *hard_low, BlockNumber *batch_blocks)
{
	int			low;
	int			hard;
	int			batch;

	switch (forknum)
	{
		case MAIN_FORKNUM:
			low = map_prealloc_main_low;
			hard = map_prealloc_main_hard;
			batch = map_prealloc_main_batch;
			break;
		case FSM_FORKNUM:
			low = map_prealloc_fsm_low;
			hard = map_prealloc_fsm_hard;
			batch = map_prealloc_fsm_batch;
			break;
		case VISIBILITYMAP_FORKNUM:
			low = map_prealloc_vm_low;
			hard = map_prealloc_vm_hard;
			batch = map_prealloc_vm_batch;
			break;
		default:
			return false;
	}

	if (low <= 0 || hard <= 0 || batch <= 0)
		return false;
	*soft_low = (BlockNumber) low;
	*hard_low = (BlockNumber) hard;
	*batch_blocks = (BlockNumber) batch;
	return true;
}

void
ummap_close(UmbraFileContext *ctx)
{
	umfile_close(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_immedsync_if_exists(UmbraFileContext *ctx)
{
	if (umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		umfile_immedsync(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_registersync_if_exists(UmbraFileContext *ctx)
{
	if (umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		umfile_registersync(ctx, UMBRA_MAP_FORKNUM);
}

void
ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	MapInvalidateRelation(rlocator);
	umfile_unlink(rlocator, UMBRA_MAP_FORKNUM, isRedo);
}

bool
ummap_prepare_firstborn_range(UmbraFileContext *ctx,
								  RelFileLocatorBackend rlocator,
								  ForkNumber forknum, BlockNumber first_lblkno,
								  BlockNumber first_pblkno, BlockNumber nblocks,
								  BlockNumber anchor_lblkno, bool physical_ready)
{
	uint64		npages;
	uint64		pending_pins;
	bool		install_entries;

	MapPageEnsureInitialized();
	npages = ((uint64) ummap_entry_index(first_lblkno) + nblocks +
				  UMMAP_ENTRIES_PER_PAGE - 1) / UMMAP_ENTRIES_PER_PAGE;
	pending_pins = ummap_pending_pin_count();

	/*
	 * Bound this backend's installed pending state.  Descriptor registration is
	 * the authoritative admission check across backends and always leaves a
	 * replacement victim available.
	 */
	install_entries = physical_ready &&
		pending_pins + npages + 1 < (uint64) MapPageBufferCount;
	return ummap_prepare_pending_range(ctx, rlocator, forknum, first_lblkno,
									   first_pblkno, nblocks, anchor_lblkno, false,
									   physical_ready, install_entries);
}

void
ummap_prepare_wal_range(UmbraFileContext *ctx,
						RelFileLocatorBackend rlocator, ForkNumber forknum,
						BlockNumber first_lblkno, BlockNumber first_pblkno,
						BlockNumber nblocks, BlockNumber anchor_lblkno)
{
	uint64		npages;
	uint64		pending_pins;
	bool		install_entries;

	Assert(ummap_range_contains(&(UmbraMapRange) {
									.first_lblkno = first_lblkno,
									.first_pblkno = first_pblkno,
									.nblocks = nblocks}, anchor_lblkno));
	MapPageEnsureInitialized();
	npages = ((uint64) ummap_entry_index(first_lblkno) + nblocks +
				  UMMAP_ENTRIES_PER_PAGE - 1) / UMMAP_ENTRIES_PER_PAGE;
	pending_pins = ummap_pending_pin_count();
	install_entries = pending_pins + npages + 1 <
		(uint64) MapPageBufferCount;
	if (!ummap_prepare_pending_range(ctx, rlocator, forknum, first_lblkno,
									 first_pblkno, nblocks, anchor_lblkno, true, true,
									 install_entries))
		elog(PANIC, "Umbra WAL-only mapping preparation requested a retry");
}

/* A replay range has one shared descriptor slot, even if its entries are local. */
static bool
ummap_pending_replay_uses_slot(int slot_id)
{
	UmbraMapPendingRange *pending;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->npinned > 0 && pending->pages[0].slot_id == slot_id)
			return true;
	}
	return false;
}

bool
ummap_prepare_remap(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					MapSuperDesc *root_desc, ForkNumber forknum, BlockNumber lblkno,
					UmbraMapRemap *remap)
{
	MapPageBuffer buffer;
	BlockNumber current;
	BlockNumber map_blkno;
	LWLock	   *extension_lock;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	Assert(ctx != NULL);
	Assert(remap != NULL);
	Assert(!remap->prepared && !remap->map_pinned);
	(void) ctx;
	if (!ummap_tracks_fork(forknum))
		return false;

	/* A failed MAP claim leaves a monotonic physical hole, never a reused P. */
	if (!ummap_try_reserve_physical_block(rlocator, root_desc, forknum,
										  &remap->new_pblkno,
										  &remap->generation_lsn,
										  &extension_lock))
		return false;
	/* WAL insertion is in a critical section; tests preload this hook. */
	INJECTION_POINT_CACHED("umbra-remap-after-reserve-before-pending", NULL);

	map_blkno = ummap_map_blkno(forknum, lblkno);
	if (!MapPageBufferTryReadCached(rlocator, map_blkno, LW_EXCLUSIVE,
								&buffer))
	{
		LWLockRelease(extension_lock);
		return false;
	}
	remap->rlocator = rlocator;
	remap->forknum = forknum;
	remap->lblkno = lblkno;
	remap->map_slot_id = buffer.desc->slot_id;
	remap->map_pinned = true;

	entry_idx = ummap_entry_index(lblkno);
	word_idx = entry_idx / 64;
	entry_mask = UINT64CONST(1) << (entry_idx % 64);
	current = ummap_page_get_entry(MapPageBufferGetData(buffer), entry_idx);
	if (!BlockNumberIsValid(current) ||
		(buffer.desc->pending_bits[word_idx] & entry_mask) != 0 ||
		ummap_pending_replay_uses_slot(buffer.desc->slot_id) ||
		buffer.desc->pending_range.valid ||
		!MapPageRegisterPendingPin(buffer.desc))
	{
		MapPageReleaseBufferNoOwner(buffer);
		remap->map_pinned = false;
		LWLockRelease(extension_lock);
		return false;
	}
	buffer.desc->pending_bits[word_idx] |= entry_mask;
	buffer.desc->pending_range.range.first_lblkno = lblkno;
	buffer.desc->pending_range.range.first_pblkno = current;
	buffer.desc->pending_range.range.nblocks = 1;
	buffer.desc->pending_range.old_pblkno = InvalidBlockNumber;
	buffer.desc->pending_range.reserved_pblkno = remap->new_pblkno;
	buffer.desc->pending_range.forknum = forknum;
	buffer.desc->pending_range.physical_ready = true;
	buffer.desc->pending_range.reserved_pblkno_valid = true;
	buffer.desc->pending_range.recovery_replay = false;
	buffer.desc->pending_range.recovery_exact = false;
	buffer.desc->pending_range.valid = true;
	remap->pending_registered = true;
	remap->old_pblkno = current;
	MapPageUnlockBufferKeepPin(buffer);
	LWLockRelease(extension_lock);
	remap->prepared = true;
	return true;
}

void
ummap_abort_remap(UmbraMapRemap *remap)
{
	MapPageBuffer buffer;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	if (remap == NULL || !remap->map_pinned)
		return;

	if (remap->map_pinned)
	{
		buffer.desc = &MapPageDescriptors[remap->map_slot_id];
		entry_idx = ummap_entry_index(remap->lblkno);
		word_idx = entry_idx / 64;
		entry_mask = UINT64CONST(1) << (entry_idx % 64);
		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (buffer.desc->tag.map_blkno !=
			ummap_map_blkno(remap->forknum, remap->lblkno) ||
			!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
										 remap->rlocator))
			elog(PANIC, "Umbra prepared remap MAP buffer changed before abort");
		if (remap->pending_registered)
		{
			if ((buffer.desc->pending_bits[word_idx] & entry_mask) == 0)
				elog(PANIC, "Umbra prepared remap lost its MAP ownership");
			if (!buffer.desc->pending_range.valid ||
				buffer.desc->pending_range.forknum != remap->forknum ||
					buffer.desc->pending_range.range.first_lblkno != remap->lblkno ||
					buffer.desc->pending_range.range.first_pblkno !=
					remap->old_pblkno ||
					buffer.desc->pending_range.range.nblocks != 1 ||
					!buffer.desc->pending_range.reserved_pblkno_valid ||
					buffer.desc->pending_range.reserved_pblkno != remap->new_pblkno)
				elog(PANIC, "Umbra prepared remap barrier changed before abort");
			buffer.desc->pending_bits[word_idx] &= ~entry_mask;
			MemSet(&buffer.desc->pending_range, 0,
				   sizeof(buffer.desc->pending_range));
			MapPageReleasePendingBufferNoOwner(buffer);
			remap->pending_registered = false;
		}
		else
			MapPageReleaseBufferNoOwner(buffer);
		remap->map_pinned = false;
	}
	remap->prepared = false;
}

/* Release a raw WAL remap pin during backend exit without raising an error. */
void
ummap_release_remap_on_exit(UmbraMapRemap *remap)
{
	MapPageBuffer buffer;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	if (remap == NULL || !remap->map_pinned)
		return;
	if (!MapPagePoolIsInitialized() || remap->map_slot_id < 0 ||
		remap->map_slot_id >= MapPageBufferCount)
	{
		MemSet(remap, 0, sizeof(*remap));
		return;
	}

	buffer.desc = &MapPageDescriptors[remap->map_slot_id];
	if (LWLockHeldByMe(&buffer.desc->content_lock) &&
		!LWLockHeldByMeInMode(&buffer.desc->content_lock, LW_EXCLUSIVE))
		LWLockRelease(&buffer.desc->content_lock);
	if (!LWLockHeldByMe(&buffer.desc->content_lock))
		LWLockAcquire(&buffer.desc->content_lock, LW_EXCLUSIVE);

	entry_idx = ummap_entry_index(remap->lblkno);
	word_idx = entry_idx / 64;
	entry_mask = UINT64CONST(1) << (entry_idx % 64);
	if (remap->pending_registered &&
		RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									remap->rlocator) &&
		(buffer.desc->pending_bits[word_idx] & entry_mask) != 0 &&
		buffer.desc->pending_range.valid &&
		!buffer.desc->pending_range.recovery_replay &&
		buffer.desc->pending_range.forknum == remap->forknum &&
		buffer.desc->pending_range.range.first_lblkno == remap->lblkno &&
		buffer.desc->pending_range.range.first_pblkno == remap->old_pblkno &&
		buffer.desc->pending_range.range.nblocks == 1 &&
		buffer.desc->pending_range.reserved_pblkno_valid &&
		buffer.desc->pending_range.reserved_pblkno == remap->new_pblkno)
	{
		buffer.desc->pending_bits[word_idx] &= ~entry_mask;
		MemSet(&buffer.desc->pending_range, 0,
			   sizeof(buffer.desc->pending_range));
	}
	MapPageReleaseBufferOnExit(buffer, remap->pending_registered);
	MemSet(remap, 0, sizeof(*remap));
}

void
ummap_publish_remap(UmbraMapRemap *remap, XLogRecPtr lsn)
{
	MapPageBuffer buffer;
	char	   *page;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	Assert(remap != NULL);
	Assert(remap->prepared && remap->map_pinned);
	Assert(remap->pending_registered);
	Assert(XLogRecPtrIsValid(lsn));
	buffer.desc = &MapPageDescriptors[remap->map_slot_id];
	entry_idx = ummap_entry_index(remap->lblkno);
	word_idx = entry_idx / 64;
	entry_mask = UINT64CONST(1) << (entry_idx % 64);

	MapPageLockBuffer(buffer, LW_EXCLUSIVE);
	if (buffer.desc->tag.map_blkno !=
		ummap_map_blkno(remap->forknum, remap->lblkno) ||
		!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									 remap->rlocator) ||
		(buffer.desc->pending_bits[word_idx] & entry_mask) == 0)
		elog(PANIC, "Umbra prepared remap changed before publication");
	if (!buffer.desc->pending_range.valid ||
		buffer.desc->pending_range.forknum != remap->forknum ||
		buffer.desc->pending_range.range.first_lblkno != remap->lblkno ||
		buffer.desc->pending_range.range.first_pblkno != remap->old_pblkno ||
		buffer.desc->pending_range.range.nblocks != 1 ||
		!buffer.desc->pending_range.reserved_pblkno_valid ||
		buffer.desc->pending_range.reserved_pblkno != remap->new_pblkno)
		elog(PANIC, "Umbra prepared remap barrier changed before publication");
	page = MapPageBufferGetData(buffer);
	if (ummap_page_get_entry(page, entry_idx) != remap->old_pblkno)
		elog(PANIC,
			 "Umbra remap changed fork %d block %u from an unexpected physical block",
			 (int) remap->forknum, remap->lblkno);
	/* Age the source before making its last MAP reference disappear. */
	MapReclaimRetirePhysicalBlock(remap->rlocator, remap->forknum,
								 remap->old_pblkno,
								 remap->generation_lsn);
	ummap_page_set_entry(page, entry_idx, remap->new_pblkno);
	MapPageMarkBufferDirty(buffer, false, lsn);
	buffer.desc->pending_bits[word_idx] &= ~entry_mask;
	MemSet(&buffer.desc->pending_range, 0,
		   sizeof(buffer.desc->pending_range));
	MapPageReleasePendingBufferNoOwner(buffer);
	remap->map_pinned = false;
	remap->pending_registered = false;
	remap->prepared = false;
}

bool
ummap_pending_range_for_block(RelFileLocator rlocator, ForkNumber forknum,
								  BlockNumber lblkno, UmbraMapRange *range)
{
	RelFileLocatorBackend brlocator =
	{
		.locator = rlocator,
		.backend = INVALID_PROC_NUMBER,
	};
	UmbraMapPendingRange *pending;
	UmbraMapPendingRange *target = NULL;

	Assert(range != NULL);
	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->in_record || pending->wal_ready ||
			pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, brlocator))
			continue;
		if ((BlockNumberIsValid(pending->anchor_lblkno) &&
			 pending->anchor_lblkno == lblkno) ||
				 (!BlockNumberIsValid(pending->anchor_lblkno) &&
				  ummap_range_contains(&pending->range, lblkno)))
		{
			target = pending;
			break;
		}
	}
	if (target == NULL)
		return false;

	target->in_record = true;
	*range = target->range;
	if (target->wal_only)
		return true;

	/*
	 * Some access methods can extend and then leave zero pages unused, while
	 * index builds can defer all page WAL until log_newpage_range().  Attach the
	 * complete adjacent, materialized, anchorless group to one referenced page.
	 * This both covers gaps such as SP-GiST parity rejects and ensures that a
	 * local-only tail never loses its single foreign-backend barrier.
	 */
	for (;;)
	{
		UmbraMapPendingRange *adjacent = NULL;

		for (pending = ummap_pending_ranges; pending != NULL;
			 pending = pending->next)
		{
			if (pending->in_record || pending->wal_ready ||
				!pending->physical_ready ||
				BlockNumberIsValid(pending->anchor_lblkno) ||
				pending->forknum != forknum ||
				pending->wal_only ||
				pending->subxid != target->subxid ||
				!RelFileLocatorBackendEquals(pending->rlocator, brlocator))
				continue;
			if ((uint64) pending->range.first_lblkno +
				pending->range.nblocks == range->first_lblkno &&
				(uint64) pending->range.first_pblkno +
				pending->range.nblocks == range->first_pblkno)
			{
				adjacent = pending;
				break;
			}
			if ((uint64) range->first_lblkno + range->nblocks ==
				pending->range.first_lblkno &&
				(uint64) range->first_pblkno + range->nblocks ==
				pending->range.first_pblkno)
			{
				adjacent = pending;
				break;
			}
		}
		if (adjacent == NULL)
			break;

		adjacent->in_record = true;
		if (adjacent->range.first_lblkno < range->first_lblkno)
		{
			range->first_lblkno = adjacent->range.first_lblkno;
			range->first_pblkno = adjacent->range.first_pblkno;
		}
		range->nblocks += adjacent->range.nblocks;
	}

	return true;
}

bool
ummap_pending_range_for_target(RelFileLocatorBackend rlocator,
							   ForkNumber forknum, BlockNumber lblkno,
							   UmbraMapRange *range, bool *wal_ready)
{
	UmbraMapPendingRange *pending;

	Assert(range != NULL);
	Assert(wal_ready != NULL);
	pending = ummap_find_pending_range(rlocator, forknum, lblkno, NULL);
	if (pending == NULL || pending->wal_only)
		return false;
	*range = pending->range;
	*wal_ready = pending->wal_ready;
	return true;
}

bool
ummap_shared_pending_for_block(RelFileLocatorBackend rlocator,
								  ForkNumber forknum, BlockNumber lblkno,
								  BlockNumber maxblocks, BlockNumber *nblocks,
								  bool *physical_ready)
{
	BlockNumber pblkno;

	Assert(nblocks != NULL);
	Assert(physical_ready != NULL);
	return ummap_lookup_shared_pending(rlocator, forknum, lblkno, maxblocks,
										 &pblkno, nblocks, physical_ready);
}

void
ummap_reset_wal_attachments(void)
{
	UmbraMapPendingRange *pending;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (!pending->wal_ready)
			pending->in_record = false;
	}
}

void
ummap_publish_attached_ranges(XLogRecPtr lsn)
{
	UmbraMapPendingRange *pending;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (!pending->in_record)
			continue;

		Assert(!pending->wal_ready);
		pending->in_record = false;
		pending->wal_ready = true;
		pending->wal_lsn = lsn;
	}

	/* Pinned ranges can be published here without allocating or doing I/O. */
	ummap_publish_ready_ranges_internal(false);
}

void
ummap_mark_range_physical(RelFileLocatorBackend rlocator, ForkNumber forknum,
						 BlockNumber lblkno)
{
	UmbraMapPendingRange **prev_next;
	UmbraMapPendingRange *pending;

	pending = ummap_find_pending_range(rlocator, forknum, lblkno, &prev_next);
	if (pending == NULL || pending->wal_only)
		return;
	if ((uint64) umfile_nblocks(pending->ctx, forknum) <
		(uint64) pending->range.first_pblkno + pending->range.nblocks)
		return;
	if (!pending->entries_installed)
	{
		UmbraMapPendingPage *pending_page = &pending->pages[0];
		MapPageBuffer buffer =
		{
			.desc = &MapPageDescriptors[pending_page->slot_id],
		};
		BlockNumber map_blkno = ummap_map_blkno(pending->forknum,
										 pending_page->first_lblkno);

		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (buffer.desc->tag.map_blkno != map_blkno ||
			!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									 pending->rlocator) ||
			!buffer.desc->pending_range.valid ||
			buffer.desc->pending_range.forknum != pending->forknum ||
			buffer.desc->pending_range.range.first_lblkno !=
			pending->range.first_lblkno ||
			buffer.desc->pending_range.range.first_pblkno !=
			pending->range.first_pblkno ||
			buffer.desc->pending_range.range.nblocks != pending->range.nblocks)
			elog(PANIC, "Umbra shared pending MAP range changed before write");
		buffer.desc->pending_range.physical_ready = true;
		MapPageUnlockBufferKeepPin(buffer);
	}
	pending->physical_ready = true;
	(void) prev_next;
}

void
ummap_abort_pending_ranges(SubTransactionId subxid)
{
	UmbraMapPendingRange **prev_next = &ummap_pending_ranges;
	UmbraMapPendingRange *pending;

	while ((pending = *prev_next) != NULL)
	{
		if (subxid != InvalidSubTransactionId && pending->subxid != subxid)
		{
			prev_next = &pending->next;
			continue;
		}
		if (pending->wal_ready)
			elog(PANIC, "Umbra mapping WAL reached abort before publication");
		*prev_next = pending->next;
		ummap_release_pending_range(pending, false);
	}
}

void
ummap_reparent_pending_ranges(SubTransactionId mySubid,
							  SubTransactionId parentSubid)
{
	UmbraMapPendingRange *pending;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->subxid == mySubid)
		{
			if (pending->wal_ready)
				elog(PANIC, "Umbra mapping WAL reached subcommit before publication");
			pending->subxid = parentSubid;
		}
	}
}

bool
ummap_has_pending_ranges(void)
{
	return ummap_pending_ranges != NULL;
}

void
ummap_publish_ready_ranges(void)
{
	ummap_publish_ready_ranges_internal(true);
}

static void
ummap_publish_ready_ranges_internal(bool include_local)
{
	UmbraMapPendingRange **prev_next = &ummap_pending_ranges;
	UmbraMapPendingRange *pending;
	bool		progress;

	do
	{
		progress = false;
		prev_next = &ummap_pending_ranges;
		while ((pending = *prev_next) != NULL)
		{
			UmbraMapPendingRange *next;
			BlockNumber logical_end;
			BlockNumber physical_end;

			if (!pending->wal_ready || !pending->physical_ready ||
				ummap_has_earlier_pending_range(pending))
			{
				prev_next = &pending->next;
				continue;
			}
			if (!pending->map_published)
			{
				if (!include_local && !pending->entries_installed)
				{
					prev_next = &pending->next;
					continue;
				}
				ummap_publish_pending_range(pending);
			}

			/*
			 * Keep the shared barrier visible until both authorities agree.  A
			 * reader must see either the old EOF plus the barrier, or the new EOF
			 * and canonical MAP, never a canonical MAP hidden behind an old EOF.
			 */
			logical_end = pending->range.first_lblkno +
				pending->range.nblocks;
			physical_end = pending->range.first_pblkno +
				pending->range.nblocks;
			if (!include_local)
				Assert(CritSectionCount > 0);
			ummap_root_advance(pending->ctx, pending->rlocator,
							 pending->forknum, logical_end, physical_end,
							 pending->wal_lsn, false);

			next = pending->next;
			ummap_release_pending_state(pending, true);
			*prev_next = next;
			pfree(pending);
			progress = true;
		}
	}
	while (progress);
}

static bool
ummap_has_earlier_pending_range(UmbraMapPendingRange *pending)
{
	UmbraMapPendingRange *candidate;

	for (candidate = ummap_pending_ranges; candidate != NULL;
		 candidate = candidate->next)
	{
		if (candidate != pending && candidate->forknum == pending->forknum &&
			RelFileLocatorBackendEquals(candidate->rlocator,
										 pending->rlocator) &&
			candidate->range.first_lblkno < pending->range.first_lblkno)
			return true;
	}
	return false;
}

static uint64
ummap_pending_pin_count(void)
{
	UmbraMapPendingRange *pending;
	uint64		npins = 0;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
		npins += pending->npinned;

	return npins;
}

BlockNumber
ummap_next_physical_block(UmbraFileContext *ctx,
							  RelFileLocatorBackend rlocator, ForkNumber forknum)
{
	UmbraMapPendingRange *pending;
	BlockNumber logical_eof;
	BlockNumber physical_frontier;
	uint64		next_pblkno;

	ummap_root_read_frontiers(ctx, rlocator, forknum, &logical_eof,
							   &physical_frontier);
	if (!BlockNumberIsValid(logical_eof) ||
		!BlockNumberIsValid(physical_frontier))
		elog(ERROR, "missing Umbra root frontier for fork %d",
			 (int) forknum);
	next_pblkno = physical_frontier;

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;

		if (pending->wal_only || pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		pending_end = (uint64) pending->range.first_pblkno +
			pending->range.nblocks;
		if (pending_end > next_pblkno)
			next_pblkno = pending_end;
	}
	if (next_pblkno >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot allocate another Umbra physical block")));

	return (BlockNumber) next_pblkno;
}

static void
ummap_replay_mapping_range(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator, ForkNumber forknum,
							   const UmbraMapRange *range, XLogRecPtr lsn)
{
	BlockNumber done = 0;

	Assert(range != NULL);
	Assert(range->nblocks > 0);
	Assert(BlockNumberIsValid(range->first_lblkno));
	Assert(BlockNumberIsValid(range->first_pblkno));
	if ((uint64) range->first_lblkno + range->nblocks >
		(uint64) InvalidBlockNumber ||
		(uint64) range->first_pblkno + range->nblocks >
		(uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("invalid Umbra WAL mapping range")));

	/* Recovery before consistency may overwrite a later failed replay's MAP. */
	while (done < range->nblocks)
	{
		BlockNumber lblkno = range->first_lblkno + done;
		BlockNumber count = ummap_page_run_limit(
			lblkno, range->nblocks - done);
		BlockNumber map_blkno = ummap_map_blkno(forknum, lblkno);
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx = ummap_entry_index(lblkno);

		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, true, false,
								   LW_EXCLUSIVE);
		page = MapPageBufferGetData(buffer);
		for (BlockNumber i = 0; i < count; i++)
		{
			BlockNumber expected = range->first_pblkno + done + i;

			ummap_page_set_entry(page, entry_idx + i,
								 expected);
		}
		MapPageMarkBufferDirty(buffer, false, lsn);
		MapPageReleaseBuffer(buffer);
		done += count;
	}
}

void
ummap_prepare_replay_range(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator, ForkNumber forknum,
							   const UmbraMapRange *range,
							   BlockNumber old_pblkno, XLogRecPtr lsn,
							   bool physical_ready, bool zero_baseline)
{
	UmbraMapRecoveryPending *pending;

	Assert(InRecovery);
	Assert(XLogRecPtrIsValid(lsn));
	ummap_validate_range(range);
	if (BlockNumberIsValid(old_pblkno) &&
		(range->nblocks != 1 || old_pblkno == range->first_pblkno))
		elog(PANIC, "invalid Umbra existing-page replay mapping");
	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;
		uint64		range_end;

		Assert(pending->exact);
		if (pending->lsn != lsn)
			elog(PANIC, "Umbra replay mapping survived its WAL record");
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		if (pending->range.first_lblkno == range->first_lblkno &&
			pending->range.first_pblkno == range->first_pblkno &&
			pending->range.nblocks == range->nblocks &&
			pending->old_pblkno == old_pblkno)
		{
			if (pending->zero_baseline != zero_baseline)
				elog(PANIC,
					 "Umbra replay mapping changed its zero-baseline contract");
			return;
		}
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		range_end = (uint64) range->first_lblkno + range->nblocks;
		if ((uint64) range->first_lblkno < pending_end &&
			(uint64) pending->range.first_lblkno < range_end)
			elog(PANIC, "overlapping Umbra replay mapping ranges");
	}
	for (pending = ummap_recovery_scratch; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;
		uint64		range_end;

		Assert(!pending->exact);
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		range_end = (uint64) range->first_lblkno + range->nblocks;
		if ((uint64) range->first_lblkno < pending_end &&
			(uint64) pending->range.first_lblkno < range_end)
			elog(PANIC,
				 "Umbra exact replay mapping overlaps recovery scratch");
	}
	pending = ummap_prepare_recovery_pending(ctx, rlocator, forknum, range,
										 old_pblkno, lsn, true,
										 physical_ready, zero_baseline);
	pending->next = ummap_replay_ranges;
	ummap_replay_ranges = pending;
}

void
ummap_switch_replay_remap(RelFileLocatorBackend rlocator, ForkNumber forknum,
						  BlockNumber lblkno, BlockNumber old_pblkno,
						  BlockNumber new_pblkno)
{
	UmbraMapRecoveryPending *pending;

	Assert(InRecovery);
	Assert(BlockNumberIsValid(old_pblkno));
	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		MapPageBuffer buffer;

		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator) ||
			pending->range.first_lblkno != lblkno ||
			pending->range.first_pblkno != new_pblkno ||
			pending->range.nblocks != 1 ||
			pending->old_pblkno != old_pblkno)
			continue;
		if (!pending->use_old_pblkno)
			return;

		buffer.desc = &MapPageDescriptors[pending->slot_id];
		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (!buffer.desc->replay_range.valid ||
			!buffer.desc->replay_range.recovery_replay ||
			!buffer.desc->replay_range.recovery_exact ||
			buffer.desc->replay_range.forknum != forknum ||
			buffer.desc->replay_range.range.first_lblkno != lblkno ||
			buffer.desc->replay_range.range.first_pblkno != new_pblkno ||
			buffer.desc->replay_range.old_pblkno != old_pblkno ||
			!buffer.desc->replay_range.use_old_pblkno)
			elog(PANIC, "Umbra replay remap changed before target switch");
		buffer.desc->replay_range.use_old_pblkno = false;
		pending->use_old_pblkno = false;
		MapPageUnlockBufferKeepPin(buffer);
		return;
	}
	elog(PANIC, "Umbra replay remap disappeared before target switch");
}

bool
ummap_replay_range_for_extension(RelFileLocatorBackend rlocator,
									 ForkNumber forknum, BlockNumber first_lblkno,
									 BlockNumber nblocks, UmbraMapRange *range,
									 bool *physical_ready, bool *zero_baseline)
{
	UmbraMapRecoveryPending *pending;
	uint64		request_end;

	Assert(nblocks > 0);
	Assert(range != NULL);
	Assert(physical_ready != NULL);
	Assert(zero_baseline != NULL);
	request_end = (uint64) first_lblkno + nblocks;
	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;

		Assert(pending->exact);
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		if ((uint64) first_lblkno >= pending_end ||
			(uint64) pending->range.first_lblkno >= request_end)
			continue;
		*range = pending->range;
		*physical_ready = pending->physical_ready;
		*zero_baseline = pending->zero_baseline;
		return true;
	}
	return false;
}

void
ummap_mark_replay_range_physical(RelFileLocatorBackend rlocator,
								 ForkNumber forknum, const UmbraMapRange *range)
{
	UmbraMapRecoveryPending *pending;
	MapPageBuffer buffer;

	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator) ||
			pending->range.first_lblkno != range->first_lblkno ||
			pending->range.first_pblkno != range->first_pblkno ||
			pending->range.nblocks != range->nblocks)
			continue;
		buffer.desc = &MapPageDescriptors[pending->slot_id];
		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (!buffer.desc->replay_range.valid ||
			!buffer.desc->replay_range.recovery_replay ||
			!buffer.desc->replay_range.recovery_exact ||
			buffer.desc->replay_range.forknum != forknum ||
			buffer.desc->replay_range.range.first_lblkno !=
			range->first_lblkno ||
			buffer.desc->replay_range.range.first_pblkno !=
			range->first_pblkno ||
			buffer.desc->replay_range.range.nblocks != range->nblocks)
			elog(PANIC,
				 "Umbra exact replay barrier changed before materialization");
		buffer.desc->replay_range.physical_ready = true;
		pending->physical_ready = true;
		MapPageUnlockBufferKeepPin(buffer);
		return;
	}
	elog(PANIC, "Umbra exact replay mapping disappeared before materialization");
}

void
ummap_publish_replay_ranges(void)
{
	UmbraMapRecoveryPending *pending;

	Assert(InRecovery);
	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		BlockNumber logical_end = pending->range.first_lblkno +
			pending->range.nblocks;
		BlockNumber physical_end = pending->range.first_pblkno +
			pending->range.nblocks;

		if (!pending->physical_ready)
			elog(PANIC,
				 "Umbra exact replay mapping reached publication before physical materialization");
		if (BlockNumberIsValid(pending->old_pblkno) &&
			pending->use_old_pblkno)
			elog(PANIC, "Umbra replay remap reached publication on old P");

		ummap_replay_mapping_range(pending->ctx, pending->rlocator,
								   pending->forknum, &pending->range,
								   pending->lsn);
		ummap_root_advance(pending->ctx, pending->rlocator,
							 pending->forknum, logical_end, physical_end,
							 pending->lsn, false);
	}

	while ((pending = ummap_replay_ranges) != NULL)
	{
		ummap_replay_ranges = pending->next;
		ummap_release_recovery_pending(pending, false);
	}
}

bool
ummap_has_replay_ranges(void)
{
	return ummap_replay_ranges != NULL;
}

void
ummap_prepare_recovery_scratch_range(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator,
								 ForkNumber forknum,
								 const UmbraMapRange *range)
{
	UmbraMapRecoveryPending *pending;
	uint64		range_end;

	Assert(InRecovery);
	ummap_validate_range(range);
	range_end = (uint64) range->first_lblkno + range->nblocks;
	for (pending = ummap_recovery_scratch; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;
		int64		pending_delta;
		int64		range_delta;

		Assert(!pending->exact);
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		pending_delta = (int64) pending->range.first_pblkno -
			(int64) pending->range.first_lblkno;
		range_delta = (int64) range->first_pblkno -
			(int64) range->first_lblkno;

		if ((uint64) range->first_lblkno >=
			(uint64) pending->range.first_lblkno &&
			range_end <= pending_end && pending_delta == range_delta)
			return;
		if ((uint64) range->first_lblkno <= pending_end &&
			(uint64) range->first_lblkno >=
			(uint64) pending->range.first_lblkno &&
			pending_delta == range_delta)
		{
			if (range_end > pending_end)
			{
				pending->range.nblocks = range_end -
					pending->range.first_lblkno;
				ummap_update_recovery_barrier(pending);
			}
			return;
		}
		if ((uint64) range->first_lblkno < pending_end &&
			(uint64) pending->range.first_lblkno < range_end)
			elog(PANIC, "overlapping Umbra recovery scratch mappings");
	}
	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		uint64		pending_end;

		Assert(pending->exact);
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			continue;
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		if ((uint64) range->first_lblkno < pending_end &&
			(uint64) pending->range.first_lblkno < range_end)
			elog(PANIC,
				 "Umbra recovery scratch overlaps exact replay mapping");
	}

	pending = ummap_prepare_recovery_pending(ctx, rlocator, forknum, range,
										 InvalidBlockNumber,
										 InvalidXLogRecPtr, false, true, false);
	pending->next = ummap_recovery_scratch;
	ummap_recovery_scratch = pending;
}

void
ummap_forget_recovery_scratch(RelFileLocatorBackend rlocator,
							  ForkNumber forknum, BlockNumber min_lblkno)
{
	UmbraMapRecoveryPending **prev_next = &ummap_recovery_scratch;
	UmbraMapRecoveryPending *pending;

	while ((pending = *prev_next) != NULL)
	{
		uint64		pending_end;

		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator))
		{
			prev_next = &pending->next;
			continue;
		}
		pending_end = (uint64) pending->range.first_lblkno +
			pending->range.nblocks;
		if (pending->range.first_lblkno >= min_lblkno)
		{
			*prev_next = pending->next;
			ummap_release_recovery_pending(pending, false);
			continue;
		}
		if (pending_end > min_lblkno)
		{
			pending->range.nblocks = min_lblkno -
				pending->range.first_lblkno;
			ummap_update_recovery_barrier(pending);
		}
		prev_next = &pending->next;
	}
}

void
ummap_forget_recovery_scratch_database(Oid dbid, Oid spcOid)
{
	UmbraMapRecoveryPending **prev_next = &ummap_recovery_scratch;
	UmbraMapRecoveryPending *pending;

	while ((pending = *prev_next) != NULL)
	{
		if (pending->rlocator.locator.dbOid != dbid ||
			(OidIsValid(spcOid) &&
			 pending->rlocator.locator.spcOid != spcOid))
		{
			prev_next = &pending->next;
			continue;
		}
		*prev_next = pending->next;
		ummap_release_recovery_pending(pending, false);
	}
}

bool
ummap_has_recovery_scratch(void)
{
	return ummap_recovery_scratch != NULL;
}

bool
ummap_has_recovery_scratch_relation(RelFileLocatorBackend rlocator)
{
	UmbraMapRecoveryPending *pending;

	for (pending = ummap_recovery_scratch; pending != NULL;
		 pending = pending->next)
	{
		if (RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			return true;
	}
	return false;
}

void
ummap_recovery_end(void)
{
	if (ummap_replay_ranges != NULL || ummap_recovery_scratch != NULL)
		elog(PANIC, "Umbra recovery ended with pending MAP ranges");
	if (ummap_recovery_resowner != NULL)
	{
		ResourceOwnerDelete(ummap_recovery_resowner);
		ummap_recovery_resowner = NULL;
	}
}

static ResourceOwner
ummap_get_recovery_resowner(void)
{
	Assert(InRecovery);
	Assert(AuxProcessResourceOwner != NULL);
	if (ummap_recovery_resowner == NULL)
		ummap_recovery_resowner =
			ResourceOwnerCreate(AuxProcessResourceOwner,
								"Umbra recovery MAP pending");
	return ummap_recovery_resowner;
}

static UmbraMapRecoveryPending *
ummap_prepare_recovery_pending(UmbraFileContext *ctx,
							   RelFileLocatorBackend rlocator,
								   ForkNumber forknum,
								   const UmbraMapRange *range,
								   BlockNumber old_pblkno,
								   XLogRecPtr lsn, bool exact,
							   bool physical_ready, bool zero_baseline)
{
	UmbraMapRecoveryPending *pending;
	MapPageBuffer buffer = {0};
	MapPagePendingRange *shared_pending = NULL;
	ResourceOwner owner;
	MemoryContext oldcontext;
	BlockNumber map_blkno;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	Assert(ctx != NULL);
	Assert(InRecovery);
	Assert(ummap_tracks_fork(forknum));
	Assert(exact || !zero_baseline);
	Assert(!zero_baseline || !physical_ready);
	ummap_validate_range(range);
	owner = ummap_get_recovery_resowner();
	ResourceOwnerEnlarge(owner);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	pending = palloc0(sizeof(UmbraMapRecoveryPending));
	MemoryContextSwitchTo(oldcontext);
	pending->ctx = ctx;
	pending->rlocator = rlocator;
	pending->forknum = forknum;
	pending->range = *range;
	pending->old_pblkno = old_pblkno;
	pending->lsn = lsn;
	pending->owner = owner;
	pending->slot_id = -1;
	pending->exact = exact;
	pending->physical_ready = physical_ready;
	pending->zero_baseline = zero_baseline;
	pending->use_old_pblkno = exact && BlockNumberIsValid(old_pblkno);
	ResourceOwnerRemember(owner, PointerGetDatum(pending),
						  &ummap_recovery_resowner_desc);
	pending->resource_remembered = true;
	if (!exact)
		return pending;

	map_blkno = ummap_map_blkno(forknum, range->first_lblkno);
	entry_idx = ummap_entry_index(range->first_lblkno);
	word_idx = entry_idx / 64;
	entry_mask = UINT64CONST(1) << (entry_idx % 64);

	/* Reserve the recovery owner's pin slot before taking the content lock. */
	ResourceOwnerEnlarge(owner);
	buffer = MapPageBufferRead(ctx, rlocator, map_blkno, true, false,
							   LW_EXCLUSIVE);
	shared_pending = exact ? &buffer.desc->replay_range :
		&buffer.desc->pending_range;
	if ((buffer.desc->pending_bits[word_idx] & entry_mask) != 0 ||
		shared_pending->valid)
	{
		MapPageReleaseBuffer(buffer);
		ereport(ERROR,
				(errmsg("Umbra MAP page already has a pending visibility barrier")));
	}
	if (!MapPageRegisterPendingPin(buffer.desc))
	{
		MapPageReleaseBuffer(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("no Umbra MAP recovery pending slot is available")));
	}
	buffer.desc->pending_bits[word_idx] |= entry_mask;
	shared_pending->range = *range;
	shared_pending->old_pblkno = old_pblkno;
	shared_pending->forknum = forknum;
	shared_pending->physical_ready = physical_ready;
	shared_pending->use_old_pblkno = pending->use_old_pblkno;
	shared_pending->recovery_replay = true;
	shared_pending->recovery_exact = exact;
	shared_pending->valid = true;
	pending->slot_id = buffer.desc->slot_id;
	pending->barrier_installed = true;
	MapPageTransferBufferPin(buffer, CurrentResourceOwner, owner);
	MapPageUnlockBufferKeepPin(buffer);

	return pending;
}

static void
ummap_release_recovery_pending(UmbraMapRecoveryPending *pending,
							   bool owner_release)
{
	MapPageBuffer buffer;
	MapPagePendingRange *shared_pending;
	BlockNumber map_blkno;
	int			entry_idx;
	int			word_idx;
	uint64		entry_mask;

	Assert(pending != NULL);
	if (!pending->barrier_installed)
	{
		if (!owner_release && pending->resource_remembered)
			ResourceOwnerForget(pending->owner, PointerGetDatum(pending),
								&ummap_recovery_resowner_desc);
		pending->resource_remembered = false;
		pfree(pending);
		return;
	}
	buffer.desc = &MapPageDescriptors[pending->slot_id];
	shared_pending = pending->exact ? &buffer.desc->replay_range :
		&buffer.desc->pending_range;
	map_blkno = ummap_map_blkno(pending->forknum,
								 pending->range.first_lblkno);
	entry_idx = ummap_entry_index(pending->range.first_lblkno);
	word_idx = entry_idx / 64;
	entry_mask = UINT64CONST(1) << (entry_idx % 64);

	if (owner_release)
	{
		bool		lock_acquired = false;

		/* ResourceOwner callbacks must not throw, including during lock cleanup. */
		if (LWLockHeldByMe(&buffer.desc->content_lock) &&
			!LWLockHeldByMeInMode(&buffer.desc->content_lock, LW_EXCLUSIVE))
			LWLockRelease(&buffer.desc->content_lock);
		if (!LWLockHeldByMe(&buffer.desc->content_lock))
		{
			LWLockAcquire(&buffer.desc->content_lock, LW_EXCLUSIVE);
			lock_acquired = true;
		}
		if (buffer.desc->tag.map_blkno == map_blkno &&
			RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
										pending->rlocator) &&
			shared_pending->valid &&
			shared_pending->recovery_replay &&
			shared_pending->recovery_exact == pending->exact &&
			shared_pending->forknum == pending->forknum &&
			shared_pending->range.first_lblkno ==
			pending->range.first_lblkno &&
			shared_pending->range.first_pblkno ==
			pending->range.first_pblkno &&
			shared_pending->old_pblkno == pending->old_pblkno &&
			shared_pending->use_old_pblkno == pending->use_old_pblkno)
		{
			buffer.desc->pending_bits[word_idx] &= ~entry_mask;
			MemSet(shared_pending, 0, sizeof(*shared_pending));
		}
		MapPageUnregisterPendingPin(buffer.desc);
		if (lock_acquired)
			LWLockRelease(&buffer.desc->content_lock);
		pending->barrier_installed = false;
		pending->resource_remembered = false;
		pfree(pending);
		return;
	}

	MapPageLockBuffer(buffer, LW_EXCLUSIVE);
	if (buffer.desc->tag.map_blkno != map_blkno ||
		!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									 pending->rlocator) ||
		(buffer.desc->pending_bits[word_idx] & entry_mask) == 0 ||
		!shared_pending->valid ||
		!shared_pending->recovery_replay ||
		shared_pending->recovery_exact != pending->exact ||
		shared_pending->forknum != pending->forknum ||
		shared_pending->range.first_lblkno !=
		pending->range.first_lblkno ||
		shared_pending->range.first_pblkno !=
		pending->range.first_pblkno ||
		shared_pending->range.nblocks != pending->range.nblocks ||
		shared_pending->old_pblkno != pending->old_pblkno ||
		shared_pending->use_old_pblkno != pending->use_old_pblkno)
		elog(PANIC, "Umbra recovery MAP barrier changed before release");
	buffer.desc->pending_bits[word_idx] &= ~entry_mask;
	MemSet(shared_pending, 0, sizeof(*shared_pending));
	pending->barrier_installed = false;
	if (pending->resource_remembered)
	{
		ResourceOwnerForget(pending->owner, PointerGetDatum(pending),
							&ummap_recovery_resowner_desc);
		pending->resource_remembered = false;
	}
	MapPageReleasePendingBufferOwned(buffer, pending->owner);
	pfree(pending);
}

static void
ummap_release_recovery_resource(Datum res)
{
	UmbraMapRecoveryPending *pending =
		(UmbraMapRecoveryPending *) DatumGetPointer(res);

	ummap_unlink_recovery_pending(pending);
	ummap_release_recovery_pending(pending, true);
}

static void
ummap_unlink_recovery_pending(UmbraMapRecoveryPending *pending)
{
	UmbraMapRecoveryPending **prev_next;

	prev_next = pending->exact ? &ummap_replay_ranges :
		&ummap_recovery_scratch;
	while (*prev_next != NULL && *prev_next != pending)
		prev_next = &(*prev_next)->next;
	if (*prev_next == pending)
		*prev_next = pending->next;
}

static void
ummap_update_recovery_barrier(UmbraMapRecoveryPending *pending)
{
	MapPageBuffer buffer;

	Assert(!pending->exact);
	if (!pending->barrier_installed)
		return;
	buffer.desc = &MapPageDescriptors[pending->slot_id];
	MapPageLockBuffer(buffer, LW_EXCLUSIVE);
	if (!buffer.desc->pending_range.valid ||
		!buffer.desc->pending_range.recovery_replay ||
		buffer.desc->pending_range.recovery_exact ||
		buffer.desc->pending_range.forknum != pending->forknum ||
		buffer.desc->pending_range.range.first_lblkno !=
		pending->range.first_lblkno ||
		buffer.desc->pending_range.range.first_pblkno !=
		pending->range.first_pblkno)
		elog(PANIC, "Umbra recovery scratch barrier changed before update");
	buffer.desc->pending_range.range.nblocks = pending->range.nblocks;
	MapPageUnlockBufferKeepPin(buffer);
}

static void
ummap_validate_range(const UmbraMapRange *range)
{
	if (range == NULL || range->nblocks == 0 ||
		!BlockNumberIsValid(range->first_lblkno) ||
		!BlockNumberIsValid(range->first_pblkno) ||
		(uint64) range->first_lblkno + range->nblocks >
		(uint64) InvalidBlockNumber ||
		(uint64) range->first_pblkno + range->nblocks >
		(uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("invalid Umbra recovery mapping range")));
}

static bool
ummap_range_contains(const UmbraMapRange *range, BlockNumber lblkno)
{
	return (uint64) lblkno >= (uint64) range->first_lblkno &&
		(uint64) lblkno < (uint64) range->first_lblkno + range->nblocks;
}

static BlockNumber
ummap_advance_local_pending_eof(RelFileLocatorBackend rlocator,
								   ForkNumber forknum, BlockNumber eof)
{
	for (;;)
	{
		UmbraMapRecoveryPending *recovery_pending;
		UmbraMapPendingRange *pending;
		bool		advanced = false;

		/*
		 * Exact replay ranges describe the WAL record currently being applied,
		 * not the relation state that preceded it.  In particular, treating a
		 * first-born replay range as part of EOF would make redo believe that
		 * the range already existed before it had been materialized.  Canonical
		 * MAP entries still contribute through the caller's scan, while recovery
		 * scratch from earlier records remains a visible extension here.
		 */
		for (recovery_pending = ummap_recovery_scratch;
			 recovery_pending != NULL;
			 recovery_pending = recovery_pending->next)
		{
			if (recovery_pending->forknum == forknum &&
				recovery_pending->range.first_lblkno == eof &&
				RelFileLocatorBackendEquals(recovery_pending->rlocator,
										 rlocator))
			{
				eof += recovery_pending->range.nblocks;
				advanced = true;
				break;
			}
		}
		if (advanced)
			continue;
		for (pending = ummap_pending_ranges; pending != NULL;
			 pending = pending->next)
		{
			if (pending->forknum == forknum &&
				pending->range.first_lblkno == eof &&
				RelFileLocatorBackendEquals(pending->rlocator, rlocator))
			{
				eof += pending->range.nblocks;
				advanced = true;
				break;
			}
		}
		if (!advanced)
			return eof;
	}
}

static bool
ummap_lookup_recovery_pending(RelFileLocatorBackend rlocator,
							  ForkNumber forknum, BlockNumber lblkno,
							  BlockNumber maxblocks, BlockNumber *pblkno,
							  BlockNumber *nblocks)
{
	UmbraMapRecoveryPending *pending;

	for (pending = ummap_replay_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator) ||
			!ummap_range_contains(&pending->range, lblkno))
			continue;
		if (!pending->physical_ready)
			elog(ERROR,
				 "Umbra exact replay mapping for fork %d block %u is not physically ready",
				 (int) forknum, lblkno);
		if (pending->use_old_pblkno)
		{
			Assert(pending->range.nblocks == 1);
			*pblkno = pending->old_pblkno;
			*nblocks = 1;
		}
		else
		{
			*pblkno = pending->range.first_pblkno +
				(lblkno - pending->range.first_lblkno);
			*nblocks = Min(maxblocks, pending->range.nblocks -
							  (lblkno - pending->range.first_lblkno));
		}
		return true;
	}
	for (pending = ummap_recovery_scratch; pending != NULL;
		 pending = pending->next)
	{
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator) ||
			!ummap_range_contains(&pending->range, lblkno))
			continue;
		*pblkno = pending->range.first_pblkno +
			(lblkno - pending->range.first_lblkno);
		*nblocks = Min(maxblocks, pending->range.nblocks -
						  (lblkno - pending->range.first_lblkno));
		return true;
	}
	return false;
}

static bool
ummap_local_pending_contains(RelFileLocatorBackend rlocator,
							 ForkNumber forknum, BlockNumber lblkno)
{
	UmbraMapRecoveryPending *recovery_pending;

	for (recovery_pending = ummap_replay_ranges;
		 recovery_pending != NULL;
		 recovery_pending = recovery_pending->next)
	{
		if (recovery_pending->forknum == forknum &&
			RelFileLocatorBackendEquals(recovery_pending->rlocator, rlocator) &&
			ummap_range_contains(&recovery_pending->range, lblkno))
			return true;
	}
	for (recovery_pending = ummap_recovery_scratch;
		 recovery_pending != NULL;
		 recovery_pending = recovery_pending->next)
	{
		if (recovery_pending->forknum == forknum &&
			RelFileLocatorBackendEquals(recovery_pending->rlocator, rlocator) &&
			ummap_range_contains(&recovery_pending->range, lblkno))
			return true;
	}
	return ummap_find_pending_range(rlocator, forknum, lblkno, NULL) != NULL;
}

static bool
ummap_lookup_local_pending(RelFileLocatorBackend rlocator, ForkNumber forknum,
							   BlockNumber lblkno, BlockNumber maxblocks,
							   BlockNumber *pblkno, BlockNumber *nblocks,
							   bool *physical_ready)
{
	UmbraMapPendingRange *pending;

	Assert(physical_ready != NULL);

	for (pending = ummap_pending_ranges; pending != NULL;
		 pending = pending->next)
	{
		if (pending->forknum != forknum ||
			!RelFileLocatorBackendEquals(pending->rlocator, rlocator) ||
			!ummap_range_contains(&pending->range, lblkno))
			continue;
		*pblkno = pending->range.first_pblkno +
			(lblkno - pending->range.first_lblkno);
		*nblocks = Min(maxblocks,
						   pending->range.nblocks -
						   (lblkno - pending->range.first_lblkno));
		*physical_ready = pending->physical_ready;
		return true;
	}
	return false;
}

/*
 * A local-only range keeps its exact mapping on the pinned barrier descriptor
 * so another backend can write an already-dirty data buffer without waiting
 * for the owning backend to generate WAL.  The descriptor pin keeps the tag
 * stable, and its content lock publishes or clears the range atomically.
 */
static bool
ummap_lookup_shared_pending(RelFileLocatorBackend rlocator,
							ForkNumber forknum, BlockNumber lblkno,
							BlockNumber maxblocks, BlockNumber *pblkno,
							BlockNumber *nblocks, bool *physical_ready)
{
	Assert(pblkno != NULL);
	Assert(nblocks != NULL);
	Assert(physical_ready != NULL);
	MapPageEnsureInitialized();

	for (int slot_id = 0; slot_id < MapPageBufferCount; slot_id++)
	{
		MapPageDesc *desc = &MapPageDescriptors[slot_id];
		MapPagePendingRange *shared_pending = NULL;
		BlockNumber map_blkno = ummap_map_blkno(forknum, lblkno);
		uint64		state;
		bool		found = false;

		LWLockAcquire(&desc->content_lock, LW_SHARED);
		state = pg_atomic_read_u64(&desc->state);
		if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
			(MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) &&
			RelFileLocatorBackendEquals(desc->tag.rlocator, rlocator))
		{
			if (desc->replay_range.valid &&
				desc->replay_range.forknum == forknum &&
				ummap_range_contains(&desc->replay_range.range, lblkno))
				shared_pending = &desc->replay_range;
			else if (desc->pending_range.valid &&
					 desc->pending_range.forknum == forknum &&
					 ummap_range_contains(&desc->pending_range.range,
										 lblkno))
				shared_pending = &desc->pending_range;
		}
		if (shared_pending != NULL)
		{
			BlockNumber offset = lblkno - shared_pending->range.first_lblkno;

			if (shared_pending->use_old_pblkno)
			{
				Assert(shared_pending->range.nblocks == 1 && offset == 0);
				*pblkno = shared_pending->old_pblkno;
				*nblocks = 1;
			}
			else
			{
				*pblkno = shared_pending->range.first_pblkno + offset;
				*nblocks = Min(maxblocks,
							   shared_pending->range.nblocks - offset);
			}
			*physical_ready = shared_pending->physical_ready;
			found = true;
		}
		else if ((state & (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID)) ==
				 (MAP_PAGE_TAG_VALID | MAP_PAGE_VALID) &&
				 desc->tag.map_blkno == map_blkno &&
				 RelFileLocatorBackendEquals(desc->tag.rlocator, rlocator))
		{
			int			entry_idx = ummap_entry_index(lblkno);
			int			word_idx = entry_idx / 64;
			uint64		entry_mask = UINT64CONST(1) << (entry_idx % 64);

			/*
			 * Small first-born ranges install their entries in-place and use the
			 * per-entry pending bit as the publication barrier.  They do not need
			 * the descriptor-wide range used by local-only large ranges, but a
			 * foreign EOF probe must still wait for their owner.
			 */
			if ((desc->pending_bits[word_idx] & entry_mask) != 0)
			{
				*pblkno = ummap_page_get_entry(MapPageGetBlock(slot_id), entry_idx);
				*nblocks = 1;
				*physical_ready = BlockNumberIsValid(*pblkno);
				found = true;
			}
		}
		LWLockRelease(&desc->content_lock);
		if (found)
			return true;
	}

	return false;
}

static UmbraMapPendingRange *
ummap_find_pending_range(RelFileLocatorBackend rlocator, ForkNumber forknum,
						 BlockNumber lblkno,
						 UmbraMapPendingRange ***prev_next)
{
	UmbraMapPendingRange **cursor = &ummap_pending_ranges;
	UmbraMapPendingRange *pending;

	while ((pending = *cursor) != NULL)
	{
		if (pending->forknum == forknum &&
			RelFileLocatorBackendEquals(pending->rlocator, rlocator) &&
			ummap_range_contains(&pending->range, lblkno))
		{
			if (prev_next != NULL)
				*prev_next = cursor;
			return pending;
		}
		cursor = &pending->next;
	}
	if (prev_next != NULL)
		*prev_next = cursor;
	return NULL;
}

static bool
ummap_prepare_pending_range(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator, ForkNumber forknum,
								BlockNumber first_lblkno,
								BlockNumber first_pblkno, BlockNumber nblocks,
								BlockNumber anchor_lblkno, bool wal_only,
								bool physical_ready, bool install_entries)
{
	UmbraMapPendingRange *candidate;
	UmbraMapPendingRange *merge_candidate = NULL;
	UmbraMapPendingRange *pending;
	MemoryContext oldcontext;
	SubTransactionId current_subxid;
	uint64		npages64;
	uint64		pending_end = 0;
	int			alloc_npages;
	Size		alloc_size;
	BlockNumber done = 0;
	bool		have_pending = false;
	volatile bool retry = false;

	Assert(ctx != NULL);
	Assert(ummap_tracks_fork(forknum));
	current_subxid = GetCurrentSubTransactionId();
	if (nblocks == 0 || !BlockNumberIsValid(first_lblkno) ||
		!BlockNumberIsValid(first_pblkno) ||
		(uint64) first_lblkno + nblocks > (uint64) InvalidBlockNumber ||
		(uint64) first_pblkno + nblocks > (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("invalid Umbra pending mapping range")));

	for (candidate = ummap_pending_ranges; candidate != NULL;
		 candidate = candidate->next)
	{
		uint64		first_end;
		uint64		candidate_end;

		if (candidate->forknum != forknum ||
			!RelFileLocatorBackendEquals(candidate->rlocator, rlocator))
			continue;
		if (candidate->wal_only != wal_only)
			ereport(ERROR,
					(errmsg("cannot mix Umbra first-born and WAL-only pending ranges")));
		first_end = (uint64) first_lblkno + nblocks;
		candidate_end = (uint64) candidate->range.first_lblkno +
			candidate->range.nblocks;
		have_pending = true;
		pending_end = Max(pending_end, candidate_end);
		if ((uint64) first_lblkno >= candidate_end ||
			(uint64) candidate->range.first_lblkno >= first_end)
			continue;
		if (candidate->range.first_lblkno == first_lblkno &&
			candidate->range.first_pblkno == first_pblkno &&
			candidate->range.nblocks == nblocks &&
			candidate->anchor_lblkno == anchor_lblkno &&
			candidate->wal_only == wal_only)
			return true;
		ereport(ERROR,
				(errmsg("overlapping Umbra pending mapping ranges for fork %d block %u",
						(int) forknum, first_lblkno)));
	}
	if (have_pending && (uint64) first_lblkno != pending_end)
		ereport(ERROR,
				(errmsg("Umbra pending mapping ranges are not a contiguous logical prefix")));

	/*
	 * Once the pin budget switches a delayed-WAL build to local-only state,
	 * keep extending that state behind its original barrier.  This bounds both
	 * MAP pins and backend memory independently of the number of data blocks.
	 */
	if (!wal_only && physical_ready &&
		!BlockNumberIsValid(anchor_lblkno))
	{
		for (candidate = ummap_pending_ranges; candidate != NULL;
			 candidate = candidate->next)
		{
			if (candidate->forknum != forknum || candidate->wal_only ||
				candidate->entries_installed || !candidate->barrier_installed ||
				candidate->wal_ready || candidate->in_record ||
				!candidate->physical_ready ||
				BlockNumberIsValid(candidate->anchor_lblkno) ||
				candidate->subxid != current_subxid ||
				!RelFileLocatorBackendEquals(candidate->rlocator, rlocator))
				continue;
			if ((uint64) candidate->range.first_lblkno +
				candidate->range.nblocks == first_lblkno &&
				(uint64) candidate->range.first_pblkno +
				candidate->range.nblocks == first_pblkno)
			{
				merge_candidate = candidate;
				break;
			}
		}
	}
	if (merge_candidate != NULL)
	{
		UmbraMapPendingPage *pending_page = &merge_candidate->pages[0];
		MapPageBuffer buffer =
		{
			.desc = &MapPageDescriptors[pending_page->slot_id],
		};
		BlockNumber old_nblocks = merge_candidate->range.nblocks;
		BlockNumber map_blkno = ummap_map_blkno(merge_candidate->forknum,
										 pending_page->first_lblkno);

		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (buffer.desc->tag.map_blkno != map_blkno ||
			!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									 merge_candidate->rlocator) ||
			!buffer.desc->pending_range.valid ||
			buffer.desc->pending_range.forknum != merge_candidate->forknum ||
			buffer.desc->pending_range.range.first_lblkno !=
			merge_candidate->range.first_lblkno ||
			buffer.desc->pending_range.range.first_pblkno !=
			merge_candidate->range.first_pblkno ||
			buffer.desc->pending_range.range.nblocks != old_nblocks ||
			!buffer.desc->pending_range.physical_ready)
			elog(PANIC, "Umbra shared pending MAP range changed before extension");
		merge_candidate->range.nblocks += nblocks;
		buffer.desc->pending_range.range.nblocks =
			merge_candidate->range.nblocks;
		MapPageUnlockBufferKeepPin(buffer);
		return true;
	}

	if (install_entries)
	{
		npages64 = ((uint64) ummap_entry_index(first_lblkno) + nblocks +
					  UMMAP_ENTRIES_PER_PAGE - 1) / UMMAP_ENTRIES_PER_PAGE;
		if (npages64 > INT_MAX ||
			npages64 > (MaxAllocSize - offsetof(UmbraMapPendingRange, pages)) /
			sizeof(UmbraMapPendingPage))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Umbra pending mapping range is too large")));
		alloc_npages = (int) npages64;
	}
	else
	{
		/* A large/local range only pins its EOF barrier page. */
		npages64 = 0;
		alloc_npages = 1;
		}
		alloc_size = offsetof(UmbraMapPendingRange, pages) +
			(Size) alloc_npages * sizeof(UmbraMapPendingPage);
	Assert(TopTransactionContext != NULL);
	Assert(TopTransactionResourceOwner != NULL);
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	pending = palloc0(alloc_size);
	MemoryContextSwitchTo(oldcontext);
	pending->ctx = ctx;
	pending->rlocator = rlocator;
	pending->forknum = forknum;
	pending->range.first_lblkno = first_lblkno;
	pending->range.first_pblkno = first_pblkno;
	pending->range.nblocks = nblocks;
	pending->anchor_lblkno = anchor_lblkno;
	pending->subxid = current_subxid;
	pending->pin_owner = TopTransactionResourceOwner;
	pending->wal_only = wal_only;
	pending->entries_installed = install_entries;
	pending->wal_lsn = InvalidXLogRecPtr;
	pending->physical_ready = physical_ready;
	pending->npages = install_entries ? (int) npages64 : 0;
	/* Transaction abort owns every partially prepared range. */
	pending->next = ummap_pending_ranges;
	ummap_pending_ranges = pending;

	if (!install_entries)
	{
		BlockNumber map_blkno = ummap_map_blkno(forknum, first_lblkno);
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx = ummap_entry_index(first_lblkno);
		int			word_idx = entry_idx / 64;
		uint64		entry_mask = UINT64CONST(1) << (entry_idx % 64);

		{
			ResourceOwnerEnlarge(pending->pin_owner);
			buffer = MapPageBufferRead(ctx, rlocator, map_blkno, true, false,
									   LW_EXCLUSIVE);
			page = MapPageBufferGetData(buffer);
			if ((buffer.desc->pending_bits[word_idx] & entry_mask) != 0 ||
				(wal_only ? ummap_page_get_entry(page, entry_idx) != first_pblkno :
				 BlockNumberIsValid(ummap_page_get_entry(page, entry_idx))))
			{
				MapPageReleaseBuffer(buffer);
				if (wal_only)
					ereport(ERROR,
							(errmsg("unexpected Umbra MAP entry for fork %d block %u",
									(int) forknum, first_lblkno)));
				retry = true;
			}

			/*
			 * A descriptor publishes one exact foreign-visible pending range.
			 * Replacing it would strand its owner and expose the wrong P range.
			 */
			if (!retry && buffer.desc->pending_range.valid)
			{
				MapPageReleaseBuffer(buffer);
				if (wal_only)
					ereport(ERROR,
							(errmsg("Umbra MAP page already has a shared pending range")));
				retry = true;
			}

			if (!retry)
			{
				if (!MapPageRegisterPendingPin(buffer.desc))
				{
					MapPageReleaseBuffer(buffer);
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
							 errmsg("no Umbra MAP pending slot is available")));
				}
				MapPageTransferBufferPin(buffer, CurrentResourceOwner,
										 pending->pin_owner);
				buffer.desc->pending_bits[word_idx] |= entry_mask;
				buffer.desc->pending_range.range = pending->range;
				buffer.desc->pending_range.forknum = forknum;
				buffer.desc->pending_range.physical_ready = physical_ready;
				buffer.desc->pending_range.recovery_replay = false;
				buffer.desc->pending_range.recovery_exact = false;
				buffer.desc->pending_range.valid = true;
				pending->pages[0].slot_id = buffer.desc->slot_id;
				pending->pages[0].first_lblkno = first_lblkno;
				pending->pages[0].nblocks = 1;
				pending->barrier_installed = true;
				pending->npinned = 1;
				MapPageUnlockBufferKeepPin(buffer);
			}
		}
		if (retry)
		{
			Assert(ummap_pending_ranges == pending);
			ummap_pending_ranges = pending->next;
			ummap_release_pending_range(pending, false);
			return false;
		}

		return true;
	}

	{
		while (done < nblocks)
		{
			BlockNumber lblkno = first_lblkno + done;
			BlockNumber count = ummap_page_run_limit(lblkno,
										 nblocks - done);
			BlockNumber map_blkno = ummap_map_blkno(forknum, lblkno);
			MapPageBuffer buffer;
			char	   *page;
			int			entry_idx = ummap_entry_index(lblkno);

			ResourceOwnerEnlarge(pending->pin_owner);
			buffer = MapPageBufferRead(ctx, rlocator, map_blkno, !wal_only,
									   false, LW_EXCLUSIVE);
			page = MapPageBufferGetData(buffer);
			for (BlockNumber i = 0; i < count; i++)
			{
				BlockNumber current = ummap_page_get_entry(page, entry_idx + i);
				BlockNumber expected = first_pblkno + done + i;
				int			word_idx = (entry_idx + i) / 64;
				uint64		entry_mask = UINT64CONST(1) <<
					((entry_idx + i) % 64);

				if ((buffer.desc->pending_bits[word_idx] & entry_mask) != 0)
				{
					MapPageReleaseBuffer(buffer);
					if (wal_only)
						ereport(ERROR,
								(errmsg("Umbra MAP entry is already pending for fork %d block %u",
										(int) forknum, lblkno + i)));
					retry = true;
					break;
				}
				if ((!wal_only && BlockNumberIsValid(current)) ||
					(wal_only && current != expected))
				{
					MapPageReleaseBuffer(buffer);
					if (wal_only)
						ereport(ERROR,
								(errmsg("unexpected Umbra MAP entry for fork %d block %u",
										(int) forknum, lblkno + i)));
					retry = true;
					break;
				}
			}
			if (retry)
				break;

			if (!MapPageRegisterPendingPin(buffer.desc))
			{
				MapPageReleaseBuffer(buffer);
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("no Umbra MAP pending slot is available")));
			}
			MapPageTransferBufferPin(buffer, CurrentResourceOwner,
									 pending->pin_owner);
			for (BlockNumber i = 0; i < count; i++)
			{
				int			current_idx = entry_idx + i;
				int			word_idx = current_idx / 64;
				uint64		entry_mask = UINT64CONST(1) << (current_idx % 64);

				buffer.desc->pending_bits[word_idx] |= entry_mask;
				if (!wal_only)
					ummap_page_set_entry(page, current_idx,
									 first_pblkno + done + i);
			}
			pending->pages[pending->npinned].slot_id = buffer.desc->slot_id;
			pending->pages[pending->npinned].first_lblkno = lblkno;
			pending->pages[pending->npinned].nblocks = count;
			pending->npinned++;
			MapPageUnlockBufferKeepPin(buffer);
			done += count;
		}
	}
	if (retry)
	{
		Assert(ummap_pending_ranges == pending);
		ummap_pending_ranges = pending->next;
		ummap_release_pending_range(pending, false);
		return false;
	}

	Assert(pending->npinned == pending->npages);
	return true;
}

/*
 * Make a ready pending range canonical.  The WAL insertion path calls this
 * for already-pinned pages while in a critical section, so that path must
 * neither allocate nor perform I/O.  Its caller releases the pending state
 * immediately after this returns.
 */
static void
ummap_publish_pending_range(UmbraMapPendingRange *pending)
{
	if (!pending->entries_installed)
	{
		ummap_publish_mapping_run(pending->ctx, pending->rlocator,
								  pending->forknum,
								  pending->range.first_lblkno,
								  pending->range.first_pblkno,
								  pending->range.nblocks,
								  pending->wal_lsn, false);
		pending->map_published = true;
		return;
	}

	for (int page_no = 0; page_no < pending->npinned; page_no++)
	{
		UmbraMapPendingPage *pending_page = &pending->pages[page_no];
		MapPageBuffer buffer =
		{
			.desc = &MapPageDescriptors[pending_page->slot_id],
		};
		BlockNumber map_blkno = ummap_map_blkno(pending->forknum,
											 pending_page->first_lblkno);
		char	   *page;
		int			entry_idx = ummap_entry_index(pending_page->first_lblkno);

		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (buffer.desc->tag.map_blkno != map_blkno ||
			!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
									 pending->rlocator))
			elog(PANIC, "Umbra pending MAP buffer changed before publication");
		page = MapPageBufferGetData(buffer);
		for (BlockNumber i = 0; i < pending_page->nblocks; i++)
		{
			BlockNumber lblkno = pending_page->first_lblkno + i;
			BlockNumber expected = pending->range.first_pblkno +
				(lblkno - pending->range.first_lblkno);
			BlockNumber current = ummap_page_get_entry(page, entry_idx + i);
			int			word_idx = (entry_idx + i) / 64;
			uint64		entry_mask = UINT64CONST(1) <<
				((entry_idx + i) % 64);

			if ((buffer.desc->pending_bits[word_idx] & entry_mask) == 0)
				elog(PANIC,
					 "Umbra pending MAP bit disappeared before publication");
			if (current != expected)
				elog(PANIC,
					 "Umbra pending MAP entry changed for fork %d block %u",
					 (int) pending->forknum, lblkno);
		}
		MapPageMarkBufferDirty(buffer, false, pending->wal_lsn);
		MapPageUnlockBufferKeepPin(buffer);
	}
	pending->map_published = true;
}

static void
ummap_release_pending_range(UmbraMapPendingRange *pending, bool publish)
{
	ummap_release_pending_state(pending, publish);
	pfree(pending);
}

static void
ummap_release_pending_state(UmbraMapPendingRange *pending, bool publish)
{
	Assert(!publish || pending->map_published);
	if (!pending->entries_installed)
	{
		if (pending->barrier_installed)
		{
			UmbraMapPendingPage *pending_page = &pending->pages[0];
			MapPageBuffer buffer =
			{
				.desc = &MapPageDescriptors[pending_page->slot_id],
			};
			BlockNumber map_blkno = ummap_map_blkno(pending->forknum,
											 pending_page->first_lblkno);
			int			entry_idx = ummap_entry_index(
				pending_page->first_lblkno);
			int			word_idx = entry_idx / 64;
			uint64		entry_mask = UINT64CONST(1) << (entry_idx % 64);

			MapPageLockBuffer(buffer, LW_EXCLUSIVE);
			if (buffer.desc->tag.map_blkno != map_blkno ||
				!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
										 pending->rlocator) ||
				(buffer.desc->pending_bits[word_idx] & entry_mask) == 0 ||
				!buffer.desc->pending_range.valid ||
				buffer.desc->pending_range.forknum != pending->forknum ||
				buffer.desc->pending_range.range.first_lblkno !=
				pending->range.first_lblkno ||
				buffer.desc->pending_range.range.first_pblkno !=
				pending->range.first_pblkno ||
				buffer.desc->pending_range.range.nblocks != pending->range.nblocks)
				elog(PANIC, "Umbra pending MAP barrier changed before completion");
			buffer.desc->pending_bits[word_idx] &= ~entry_mask;
			MemSet(&buffer.desc->pending_range, 0,
				   sizeof(buffer.desc->pending_range));
			MapPageReleasePendingBufferOwned(buffer, pending->pin_owner);
		}
		return;
	}

	for (int page_no = 0; page_no < pending->npinned; page_no++)
	{
		UmbraMapPendingPage *pending_page = &pending->pages[page_no];
		MapPageBuffer buffer =
		{
			.desc = &MapPageDescriptors[pending_page->slot_id],
		};
		BlockNumber map_blkno = ummap_map_blkno(pending->forknum,
												 pending_page->first_lblkno);
		char	   *page;
		int			entry_idx = ummap_entry_index(pending_page->first_lblkno);

		MapPageLockBuffer(buffer, LW_EXCLUSIVE);
		if (buffer.desc->tag.map_blkno != map_blkno ||
			!RelFileLocatorBackendEquals(buffer.desc->tag.rlocator,
										 pending->rlocator))
			elog(PANIC, "Umbra pending MAP buffer changed before completion");
		page = MapPageBufferGetData(buffer);
		for (BlockNumber i = 0; i < pending_page->nblocks; i++)
		{
			BlockNumber lblkno = pending_page->first_lblkno + i;
			BlockNumber expected = pending->range.first_pblkno +
				(lblkno - pending->range.first_lblkno);
			BlockNumber current = ummap_page_get_entry(page, entry_idx + i);
			int			word_idx = (entry_idx + i) / 64;
			uint64		entry_mask = UINT64CONST(1) <<
				((entry_idx + i) % 64);

			if ((buffer.desc->pending_bits[word_idx] & entry_mask) == 0)
				elog(PANIC, "Umbra pending MAP bit disappeared before completion");
			if (current != expected)
				elog(PANIC,
					 "Umbra pending MAP entry changed for fork %d block %u",
					 (int) pending->forknum, lblkno);
			if (!publish && !pending->wal_only)
				ummap_page_clear_entry(page, entry_idx + i);
			buffer.desc->pending_bits[word_idx] &= ~entry_mask;
		}
		MapPageReleasePendingBufferOwned(buffer, pending->pin_owner);
	}
}

BlockNumber
ummap_nblocks(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				  ForkNumber forknum)
{
	BlockNumber logical_eof;
	BlockNumber local_eof;
	BlockNumber physical_frontier;

	if (!ummap_tracks_fork(forknum))
		return umfile_nblocks(ctx, forknum);
	/* Redo deliberately reconstructs mappings without trusting a disk EOF. */
	if (InRecovery)
		return ummap_scan_nblocks(ctx, rlocator, forknum, 0);
	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d", (int) forknum);

retry:
	ummap_root_read_frontiers(ctx, rlocator, forknum, &logical_eof,
								   &physical_frontier);
	if (!BlockNumberIsValid(logical_eof) ||
		!BlockNumberIsValid(physical_frontier))
		elog(ERROR, "missing Umbra map root state for fork %d", (int) forknum);

	/* This backend may additionally own an unpublished contiguous extension. */
	local_eof = ummap_advance_local_pending_eof(rlocator, forknum, logical_eof);
	if (local_eof != logical_eof)
		return local_eof;

	{
		BlockNumber current_eof;
		BlockNumber current_physical;

		ummap_root_read_frontiers(ctx, rlocator, forknum, &current_eof,
								   &current_physical);
		if (current_eof != logical_eof ||
			current_physical != physical_frontier)
			goto retry;
	}

	return logical_eof;
}

/* Recovery scans canonical and scratch mappings without a logical-EOF gate. */
static BlockNumber
ummap_scan_nblocks(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 ForkNumber forknum, BlockNumber logical_nblocks)
{
	BlockNumber scan_start = logical_nblocks;
	BlockNumber map_nblocks;

	if (!ummap_tracks_fork(forknum))
		return umfile_nblocks(ctx, forknum);

	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d",
			 (int) forknum);

	map_nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	for (;;)
	{
		BlockNumber map_blkno;
		BlockNumber pending_eof;
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx;
		bool		resume_after_pending = false;

		map_blkno = ummap_map_blkno(forknum, logical_nblocks);
		if (map_blkno >= map_nblocks)
		{
			pending_eof = ummap_advance_local_pending_eof(rlocator, forknum,
											 logical_nblocks);
			if (pending_eof == logical_nblocks)
				return logical_nblocks;
			logical_nblocks = pending_eof;
			continue;
		}

		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, false, false,
								   LW_SHARED);
		page = MapPageBufferGetData(buffer);
		entry_idx = ummap_entry_index(logical_nblocks);
		for (int i = entry_idx; i < UMMAP_ENTRIES_PER_PAGE; i++)
		{
			BlockNumber lblkno = logical_nblocks + (i - entry_idx);
			int			word_idx = i / 64;
			uint64		entry_mask = UINT64CONST(1) << (i % 64);

			if ((buffer.desc->pending_bits[word_idx] & entry_mask) != 0 &&
				!ummap_local_pending_contains(rlocator, forknum, lblkno))
			{
				MapPageReleaseBuffer(buffer);
				CHECK_FOR_INTERRUPTS();
				pg_usleep(1000L);
				logical_nblocks = scan_start;
				resume_after_pending = true;
				break;
			}
			if (!BlockNumberIsValid(ummap_page_get_entry(page, i)))
			{
				pending_eof = ummap_advance_local_pending_eof(rlocator,
												 forknum, lblkno);
				MapPageReleaseBuffer(buffer);
				if (pending_eof == lblkno)
					return lblkno;
				logical_nblocks = pending_eof;
				resume_after_pending = true;
				break;
			}
		}
		if (resume_after_pending)
			continue;
		MapPageReleaseBuffer(buffer);

		if ((uint64) logical_nblocks +
			(UMMAP_ENTRIES_PER_PAGE - entry_idx) >=
			(uint64) InvalidBlockNumber)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cannot represent Umbra logical relation size")));
		logical_nblocks += UMMAP_ENTRIES_PER_PAGE - entry_idx;
	}
}

BlockNumber
ummap_lookup_block(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				   ForkNumber forknum,
				   BlockNumber lblkno)
{
	BlockNumber pblkno;

	(void) ummap_lookup_run(ctx, rlocator, forknum, lblkno, 1, &pblkno);
	return pblkno;
}

BlockNumber
ummap_lookup_run(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 ForkNumber forknum,
					 BlockNumber lblkno, BlockNumber maxblocks,
					 BlockNumber *pblkno)
{
	return ummap_lookup_run_internal(ctx, rlocator, forknum, lblkno,
									 maxblocks, pblkno, false);
}

BlockNumber
ummap_lookup_write_run(UmbraFileContext *ctx,
					   RelFileLocatorBackend rlocator, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber maxblocks,
					   BlockNumber *pblkno)
{
	return ummap_lookup_run_internal(ctx, rlocator, forknum, lblkno,
									 maxblocks, pblkno, true);
}

static BlockNumber
ummap_lookup_run_internal(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator, ForkNumber forknum,
						  BlockNumber lblkno, BlockNumber maxblocks,
						  BlockNumber *pblkno, bool allow_pending)
{
	BlockNumber map_nblocks;
	BlockNumber run_blocks = 0;
	BlockNumber pending_blocks;
	bool		pending_physical_ready;
	bool		retried_missing = false;

	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	if (!ummap_tracks_fork(forknum))
	{
		*pblkno = lblkno;
		return maxblocks;
	}

	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d block %u",
			 (int) forknum, lblkno);

	if (ummap_lookup_recovery_pending(rlocator, forknum, lblkno, maxblocks,
									pblkno, &pending_blocks))
		return pending_blocks;

	if (ummap_lookup_local_pending(rlocator, forknum, lblkno, maxblocks,
									   pblkno, &pending_blocks,
									   &pending_physical_ready))
	{
		if (pending_physical_ready)
			return pending_blocks;
		elog(ERROR,
			 "Umbra pending mapping for fork %d block %u is not physically ready",
			 (int) forknum, lblkno);
	}

retry:
	run_blocks = 0;
	if (ummap_lookup_shared_pending(rlocator, forknum, lblkno, maxblocks,
										pblkno, &pending_blocks,
										&pending_physical_ready))
	{
		if (allow_pending && pending_physical_ready)
			return pending_blocks;
		if (allow_pending && !pending_physical_ready)
			elog(ERROR,
				 "Umbra pending mapping for fork %d block %u is not physically ready",
				 (int) forknum, lblkno);
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
		goto retry;
	}
	map_nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	while (run_blocks < maxblocks)
	{
		BlockNumber page_lblkno = lblkno + run_blocks;
		BlockNumber map_blkno;
		BlockNumber page_limit;
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx;

		map_blkno = ummap_map_blkno(forknum, page_lblkno);
		if (map_blkno >= map_nblocks)
		{
			if (run_blocks > 0)
				return run_blocks;
			elog(ERROR,
				 "missing Umbra map page %u for fork %d block %u",
				 map_blkno, (int) forknum, page_lblkno);
		}

		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, false, false,
								   LW_SHARED);
		page = MapPageBufferGetData(buffer);
		entry_idx = ummap_entry_index(page_lblkno);
		page_limit = ummap_page_run_limit(page_lblkno,
										  maxblocks - run_blocks);

		for (BlockNumber i = 0; i < page_limit; i++)
		{
			BlockNumber entry_lblkno = page_lblkno + i;
			BlockNumber entry_pblkno;
			int			current_idx = entry_idx + i;
			int			word_idx = current_idx / 64;
			uint64		entry_mask = UINT64CONST(1) << (current_idx % 64);
			bool		entry_pending =
				(buffer.desc->pending_bits[word_idx] & entry_mask) != 0;

			if (entry_pending && !allow_pending &&
				ummap_find_pending_range(rlocator, forknum, entry_lblkno,
									 NULL) == NULL)
			{
				MapPageReleaseBuffer(buffer);
				CHECK_FOR_INTERRUPTS();
				pg_usleep(1000L);
				goto retry;
			}
			entry_pblkno = ummap_page_get_entry(page, current_idx);
			if (!BlockNumberIsValid(entry_pblkno))
			{
				MapPageReleaseBuffer(buffer);
				if (run_blocks > 0)
					return run_blocks;
				if (entry_pending)
				{
					CHECK_FOR_INTERRUPTS();
					pg_usleep(1000L);
					goto retry;
				}
				/* Publication may have completed after this page was read. */
				if (!retried_missing)
				{
					retried_missing = true;
					goto retry;
				}
				elog(ERROR, "missing Umbra map entry for fork %d block %u",
					 (int) forknum, entry_lblkno);
			}

			if (run_blocks == 0)
				*pblkno = entry_pblkno;
			else if ((uint64) entry_pblkno !=
					 (uint64) *pblkno + run_blocks)
			{
				MapPageReleaseBuffer(buffer);
				return run_blocks;
			}

			run_blocks++;
		}
		MapPageReleaseBuffer(buffer);
	}

	return run_blocks;
}

BlockNumber
ummap_identity_run_limit(ForkNumber forknum, BlockNumber lblkno,
						 BlockNumber maxblocks, BlockNumber *pblkno)
{
	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	*pblkno = lblkno;
	if (!ummap_tracks_fork(forknum))
		return maxblocks;

	return ummap_page_run_limit(lblkno, maxblocks);
}

/*
 * Publish an exact contiguous L -> P run.
 *
 * Normal extension may fill the current EOF or repeat the same mapping, but
 * this patch does not permit replacing a visible mapping.  Redo is different:
 * the MAP page may contain a later on-disk state, so recovery must install the
 * physical blocks named by the WAL record without guessing identity.
 */
void
ummap_publish_mapping_run(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator, ForkNumber forknum,
						  BlockNumber first_lblkno,
						  BlockNumber first_pblkno, BlockNumber nblocks,
						  XLogRecPtr wal_flush_lsn, bool skipFsync)
{
	BlockNumber done = 0;

	Assert(ctx != NULL);
	Assert(nblocks > 0);
	Assert(BlockNumberIsValid(first_lblkno));
	Assert(BlockNumberIsValid(first_pblkno));
	Assert(ummap_tracks_fork(forknum));

	if ((uint64) first_lblkno + nblocks > (uint64) InvalidBlockNumber ||
		(uint64) first_pblkno + nblocks > (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot publish Umbra map range beyond block %u",
						MaxBlockNumber)));

	while (done < nblocks)
	{
		BlockNumber lblkno = first_lblkno + done;
		BlockNumber count = ummap_page_run_limit(lblkno, nblocks - done);
		BlockNumber map_blkno = ummap_map_blkno(forknum, lblkno);
		MapPageBuffer buffer;
		char	   *page;
		int			entry_idx = ummap_entry_index(lblkno);

		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, true, skipFsync,
								   LW_EXCLUSIVE);
		page = MapPageBufferGetData(buffer);
		for (BlockNumber i = 0; i < count; i++)
		{
			BlockNumber entry_lblkno = lblkno + i;
			BlockNumber entry_pblkno = first_pblkno + done + i;
			BlockNumber current;

			current = ummap_page_get_entry(page, entry_idx + i);
			if (BlockNumberIsValid(current) && current != entry_pblkno)
			{
				MapPageReleaseBuffer(buffer);
				elog(ERROR,
					 "Umbra map entry for fork %d block %u already points to physical block %u",
					 (int) forknum, entry_lblkno, current);
			}
			if (current != entry_pblkno)
				ummap_page_set_entry(page, entry_idx + i, entry_pblkno);
		}
		MapPageMarkBufferDirty(buffer, skipFsync, wal_flush_lsn);
		MapPageReleaseBuffer(buffer);
		done += count;
	}
}

BlockNumber
ummap_set_identity_block(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator, ForkNumber forknum,
						 BlockNumber lblkno, bool skipFsync)
{
	BlockNumber pblkno;

	(void) ummap_set_identity_run(ctx, rlocator, forknum, lblkno, 1, &pblkno,
								  skipFsync);
	return pblkno;
}

BlockNumber
ummap_set_identity_run(UmbraFileContext *ctx,
					   RelFileLocatorBackend rlocator, ForkNumber forknum,
					   BlockNumber lblkno, BlockNumber maxblocks,
					   BlockNumber *pblkno, bool skipFsync)
{
	BlockNumber run_limit;

	Assert(pblkno != NULL);
	Assert(maxblocks > 0);

	if (!ummap_tracks_fork(forknum))
	{
		*pblkno = lblkno;
		return maxblocks;
	}

	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d block %u",
			 (int) forknum, lblkno);

	run_limit = ummap_identity_run_limit(forknum, lblkno, maxblocks, pblkno);
	ummap_publish_mapping_run(ctx, rlocator, forknum, lblkno, lblkno,
								  run_limit, InvalidXLogRecPtr, skipFsync);
	ummap_root_advance(ctx, rlocator, forknum, lblkno + run_limit,
					   lblkno + run_limit,
					   InvalidXLogRecPtr, skipFsync);
	return run_limit;
}

void
ummap_truncate(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			   ForkNumber forknum, BlockNumber old_nblocks,
			   BlockNumber new_nblocks, XLogRecPtr wal_flush_lsn,
			   bool skipFsync)
{
	LWLock	   *extension_lock;

	if (!ummap_tracks_fork(forknum))
		return;
	extension_lock = MapPageExtensionLock(rlocator);
	LWLockAcquire(extension_lock, LW_EXCLUSIVE);
	ummap_truncate_locked(ctx, rlocator, forknum, old_nblocks,
						  new_nblocks, wal_flush_lsn, skipFsync);
	LWLockRelease(extension_lock);
}

/* Clear every stale mapping that used to be visible above the new EOF. */
static void
ummap_truncate_locked(UmbraFileContext *ctx,
					  RelFileLocatorBackend rlocator,
					  ForkNumber forknum, BlockNumber old_nblocks,
					  BlockNumber new_nblocks, XLogRecPtr wal_flush_lsn,
					  bool skipFsync)
{
	BlockNumber end_lblkno;
	BlockNumber map_nblocks;
	XLogRecPtr	generation_lsn;
	uint64		logical_capacity;

	Assert(ctx != NULL);
	Assert(ummap_tracks_fork(forknum));
	(void) old_nblocks;
	if (!umfile_exists(ctx, UMBRA_MAP_FORKNUM))
		elog(ERROR, "missing Umbra map fork for fork %d",
			 (int) forknum);
	generation_lsn = ummap_get_generation_lsn(ctx, rlocator);

	map_nblocks = umfile_nblocks(ctx, UMBRA_MAP_FORKNUM);
	logical_capacity = ummap_existing_fork_pages(forknum, map_nblocks) *
		(uint64) UMMAP_ENTRIES_PER_PAGE;
	end_lblkno = logical_capacity >= (uint64) InvalidBlockNumber ?
		InvalidBlockNumber : (BlockNumber) logical_capacity;
	/*
	 * Clear every materialized MAP page tail, not just the current contiguous
	 * prefix.  A prior truncate attempt may have persisted a low cleared page
	 * before a higher one and crashed, so logical EOF alone cannot bound the
	 * remaining work.  Scanning the physical MAP extent makes redo idempotent
	 * even after such a second crash.  High-to-low order additionally avoids
	 * lowering the in-memory EOF until the final page is handled.
	 */
	while (end_lblkno > new_nblocks)
	{
		BlockNumber page_first = end_lblkno - 1 -
			((end_lblkno - 1) % UMMAP_ENTRIES_PER_PAGE);
		BlockNumber first_lblkno = Max(new_nblocks, page_first);
		BlockNumber count = end_lblkno - first_lblkno;
		BlockNumber map_blkno = ummap_map_blkno(forknum, first_lblkno);
		MapPageBuffer buffer;
		char	   *page;
		bool		dirty = false;
		int			entry_idx = ummap_entry_index(first_lblkno);

		if (map_blkno >= map_nblocks)
			elog(ERROR,
				 "missing Umbra map page %u while truncating fork %d",
				 map_blkno, (int) forknum);
		buffer = MapPageBufferRead(ctx, rlocator, map_blkno, false, false,
								   LW_EXCLUSIVE);
		page = MapPageBufferGetData(buffer);
		for (BlockNumber i = 0; i < count; i++)
		{
			int			current_idx = entry_idx + i;
			int			word_idx = current_idx / 64;
			uint64		entry_mask = UINT64CONST(1) << (current_idx % 64);

			if ((buffer.desc->pending_bits[word_idx] & entry_mask) != 0)
			{
				MapPageReleaseBuffer(buffer);
				ereport(ERROR,
						(errmsg("cannot truncate pending Umbra MAP entry for fork %d block %u",
								(int) forknum, first_lblkno + i)));
			}
			{
				BlockNumber old_pblkno =
					ummap_page_get_entry(page, current_idx);

				if (!BlockNumberIsValid(old_pblkno))
					continue;
				MapReclaimRetirePhysicalBlock(rlocator, forknum, old_pblkno,
										 generation_lsn);
				ummap_page_clear_entry(page, current_idx);
				dirty = true;
			}
		}
		if (dirty)
			MapPageMarkBufferDirty(buffer, skipFsync, wal_flush_lsn);
		MapPageReleaseBuffer(buffer);
		end_lblkno = first_lblkno;
	}

	{
		BlockNumber logical_eof;
		BlockNumber physical_frontier;

		ummap_root_read_frontiers(ctx, rlocator, forknum, &logical_eof,
								   &physical_frontier);
		if (!BlockNumberIsValid(logical_eof) ||
			!BlockNumberIsValid(physical_frontier))
			elog(ERROR, "missing Umbra root frontier for fork %d",
				 (int) forknum);
		ummap_root_set_logical(ctx, rlocator, forknum, new_nblocks,
							   physical_frontier, wal_flush_lsn, skipFsync);
	}
}

bool
ummap_tracks_fork(ForkNumber forknum)
{
	return forknum == MAIN_FORKNUM ||
		forknum == FSM_FORKNUM ||
		forknum == VISIBILITYMAP_FORKNUM;
}

static uint64
ummap_existing_fork_pages(ForkNumber forknum, BlockNumber map_nblocks)
{
	uint64		ordinary_pages;
	uint64		full_groups;
	uint64		partial_pages;

	if (map_nblocks <= UMMAP_BLOCK_FIRST_GROUP)
		return 0;
	ordinary_pages = (uint64) map_nblocks - UMMAP_BLOCK_FIRST_GROUP;
	full_groups = ordinary_pages / UMMAP_GROUP_TOTAL_PAGES;
	partial_pages = ordinary_pages % UMMAP_GROUP_TOTAL_PAGES;

	switch (forknum)
	{
		case FSM_FORKNUM:
			return full_groups + (partial_pages > 0 ? 1 : 0);
		case VISIBILITYMAP_FORKNUM:
			return full_groups + (partial_pages > 1 ? 1 : 0);
		case MAIN_FORKNUM:
			return full_groups * UMMAP_GROUP_MAIN_PAGES +
				Min(partial_pages > 2 ? partial_pages - 2 : 0,
					(uint64) UMMAP_GROUP_MAIN_PAGES);
		default:
			elog(ERROR, "unsupported fork number %d in Umbra map lookup",
				 (int) forknum);
	}
	pg_unreachable();
}

static BlockNumber
ummap_fork_page_index_to_map_blkno(ForkNumber forknum,
								   BlockNumber fork_page_idx)
{
	uint64		group_no;
	uint64		blkno64;

	switch (forknum)
	{
		case FSM_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES;
			break;

		case VISIBILITYMAP_FORKNUM:
			group_no = (uint64) fork_page_idx;
			blkno64 = UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
				(uint64) UMMAP_GROUP_FSM_PAGES;
			break;

		case MAIN_FORKNUM:
			group_no = (uint64) fork_page_idx /
				(uint64) UMMAP_GROUP_MAIN_PAGES;
			blkno64 = UMMAP_BLOCK_FIRST_GROUP +
				group_no * (uint64) UMMAP_GROUP_TOTAL_PAGES +
				(uint64) UMMAP_GROUP_FSM_PAGES +
				(uint64) UMMAP_GROUP_VM_PAGES +
				((uint64) fork_page_idx %
				 (uint64) UMMAP_GROUP_MAIN_PAGES);
			break;

		default:
			elog(ERROR, "unsupported fork number %d in Umbra map lookup",
				 (int) forknum);
			pg_unreachable();
	}

	if (blkno64 > (uint64) MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot address map page %u for fork %d",
						fork_page_idx, (int) forknum)));

	return (BlockNumber) blkno64;
}

static BlockNumber
ummap_map_blkno(ForkNumber forknum, BlockNumber lblkno)
{
	return ummap_fork_page_index_to_map_blkno(forknum,
											  lblkno / UMMAP_ENTRIES_PER_PAGE);
}

static int
ummap_entry_index(BlockNumber lblkno)
{
	return lblkno % UMMAP_ENTRIES_PER_PAGE;
}

static BlockNumber
ummap_page_run_limit(BlockNumber lblkno, BlockNumber maxblocks)
{
	int			entry_idx = ummap_entry_index(lblkno);

	return Min(maxblocks,
			   (BlockNumber) UMMAP_ENTRIES_PER_PAGE - entry_idx);
}

static BlockNumber
ummap_page_get_entry(char *page, int entry_idx)
{
	BlockNumber *entries = (BlockNumber *) page;
	BlockNumber pblkno;

	Assert(entry_idx >= 0 && entry_idx < UMMAP_ENTRIES_PER_PAGE);

	pblkno = entries[entry_idx];
	if (!BlockNumberIsValid(pblkno))
		return InvalidBlockNumber;

	return pblkno;
}

static void
ummap_page_set_entry(char *page, int entry_idx, BlockNumber pblkno)
{
	BlockNumber *entries = (BlockNumber *) page;

	Assert(entry_idx >= 0 && entry_idx < UMMAP_ENTRIES_PER_PAGE);
	Assert(BlockNumberIsValid(pblkno));

	entries[entry_idx] = pblkno;
}

static void
ummap_page_clear_entry(char *page, int entry_idx)
{
	BlockNumber *entries = (BlockNumber *) page;

	Assert(entry_idx >= 0 && entry_idx < UMMAP_ENTRIES_PER_PAGE);
	entries[entry_idx] = InvalidBlockNumber;
}
