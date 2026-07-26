/*-------------------------------------------------------------------------
 *
 * ummap.c
 *	  Umbra-private relation metadata root.
 *
 * The _map file has a fixed, CRC-protected root.  The shared cache below
 * owns residency, locking, deferred writeback, and invalidation.  Only this
 * module interprets the 64-byte payload or transfers its padded 512-byte
 * sector on disk.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/map.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

#define UMMAP_ROOT_MAGIC		0x554D4252U	/* "UMBR" */
#define UMMAP_ROOT_VERSION		3U
#define UMMAP_ROOT_FLAGS_V1	0x00000001U
#define UMMAP_ROOT_FLAG_MAIN_SLOT0 0x00000002U
#define UMMAP_ROOT_FLAG_FSM_SLOT0 0x00000004U
#define UMMAP_ROOT_FLAG_VM_SLOT0 0x00000008U
#define UMMAP_ROOT_FLAGS_KNOWN \
	(UMMAP_ROOT_FLAGS_V1 | UMMAP_ROOT_FLAG_MAIN_SLOT0 | \
	 UMMAP_ROOT_FLAG_FSM_SLOT0 | UMMAP_ROOT_FLAG_VM_SLOT0)
#define UMMAP_ROOT_IMAGE_SIZE	64
#define UMMAP_ROOT_SECTOR_SIZE	512

#define UMMAP_ROOT_DSA_INIT_SIZE (2 * 1024 * 1024)
#define UMMAP_ROOT_DSA_MAX_SIZE  (64 * 1024 * 1024)

/*
 * InvalidBlockNumber means that an auxiliary fork has not entered the chunk
 * mapping policy.  Physical capacity is derived from each logical EOF rather
 * than stored independently.
 */
typedef struct pg_attribute_packed() UmbraMapRootData
{
	uint32		magic;
	uint32		version;
	uint32		blcksz;
	uint32		chunk_pages;
	uint32		flags;
	BlockNumber logical_eof_main;
	BlockNumber logical_eof_fsm;
	BlockNumber logical_eof_vm;
	uint8		reserved[28];
	pg_crc32c	crc;
} UmbraMapRootData;

typedef struct UmbraMapRootTag
{
	RelFileLocatorBackend rlocator;
} UmbraMapRootTag;

typedef struct UmbraMapRootEntry
{
	UmbraMapRootTag tag;
	LWLock      content_lock;
	bool        valid;
	bool        dirty;
	bool        needs_fsync;
	XLogRecPtr  wal_flush_lsn;
	char        image[UMMAP_ROOT_IMAGE_SIZE];
} UmbraMapRootEntry;

typedef struct UmbraMapRootCacheCtl
{
	void       *raw_dsa_area;
	dshash_table_handle hash_handle;
} UmbraMapRootCacheCtl;

StaticAssertDecl(sizeof(UmbraMapRootData) == UMMAP_ROOT_IMAGE_SIZE,
				 "Umbra MAP root payload size is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, reserved) == 32,
				 "Umbra MAP root reserved payload offset is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, crc) == 60,
				 "Umbra MAP root CRC offset is wrong");
StaticAssertDecl(BLCKSZ >= UMMAP_ROOT_SECTOR_SIZE,
				 "Umbra MAP root sector must fit in a block");
StaticAssertDecl(UMMAP_ROOT_DSA_INIT_SIZE <= UMMAP_ROOT_DSA_MAX_SIZE,
				 "Umbra MAP root cache initial size exceeds its limit");

static UmbraMapRootCacheCtl *UmbraMapRootCacheCtlData = NULL;
static dsa_area *UmbraMapRootDSA = NULL;
static dshash_table *UmbraMapRootHash = NULL;
static bool ummap_root_cache_exit_handler_registered = false;

static const dshash_parameters UmbraMapRootHashParams = {
	.key_size = sizeof(UmbraMapRootTag),
	.entry_size = sizeof(UmbraMapRootEntry),
	.compare_function = dshash_memcmp,
	.hash_function = dshash_memhash,
	.copy_function = dshash_memcpy,
	.tranche_id = LWTRANCHE_UMBRA_ROOT_MAPPING,
};

static void ummap_root_init(char *image);
static void ummap_root_refresh_crc(UmbraMapRootData *root);
static bool ummap_root_is_valid(const UmbraMapRootData *root);
static void ummap_root_load_image(UmbraFileContext *ctx, char *image);
static bool ummap_root_read_valid_image(UmbraFileContext *ctx, char *image);
static void ummap_root_write_image(UmbraFileContext *ctx, const char *image,
							 bool skipFsync);
static Size ummap_root_cache_shmem_size(void);
static void ummap_root_cache_request(void *arg);
static void ummap_root_cache_init(void *arg);
static void ummap_root_cache_attach(void);
static void ummap_root_cache_detach(int code, Datum arg);
static void ummap_root_cache_ensure_initialized(void);
static bool ummap_root_cache_find_locked(const UmbraMapRootTag *tag,
									 LWLockMode mode,
									 UmbraMapRootEntry **entry);
static UmbraMapRootEntry *ummap_root_cache_ensure_entry_locked(
	const UmbraMapRootTag *tag);
static UmbraMapRootEntry *ummap_root_cache_get(UmbraFileContext *ctx,
	RelFileLocatorBackend rlocator, LWLockMode mode);
static void ummap_root_cache_load_locked(UmbraFileContext *ctx,
	UmbraMapRootEntry *entry);
static void ummap_root_cache_flush_locked(UmbraMapRootEntry *entry,
	UmbraFileContext *ctx);
static void ummap_root_cache_flush_prepared_locked(UmbraMapRootEntry *entry,
	UmbraFileContext *ctx);
static void ummap_root_cache_flush_internal_locked(UmbraMapRootEntry *entry,
	UmbraFileContext *ctx, bool prepared);
static void ummap_prepare_root_flush(UmbraFileContext *ctx, uint32 flags);
static int ummap_root_cache_collect_tags(Oid dbid, Oid spcOid,
	UmbraMapRootTag **tags);
static void ummap_root_cache_flush_matching(Oid dbid, Oid spcOid);
static void ummap_root_cache_delete_entry(const UmbraMapRootTag *tag);
static void ummap_root_cache_invalidate_matching(Oid dbid, Oid spcOid);
static uint32 ummap_aux_slot0_flag(ForkNumber forknum);
static bool ummap_root_aux_slot0_active(const UmbraMapRootData *root,
									 ForkNumber forknum);
static BlockNumber ummap_root_get_aux_frontier(const UmbraMapRootData *root,
											  ForkNumber forknum);
static void ummap_root_set_aux_frontier(UmbraMapRootData *root,
									ForkNumber forknum,
									BlockNumber logical_eof);
static bool ummap_root_aux_frontier_valid(const UmbraMapRootData *root,
										ForkNumber forknum);

const ShmemCallbacks UmbraMapRootShmemCallbacks = {
	.request_fn = ummap_root_cache_request,
	.init_fn = ummap_root_cache_init,
};

bool
ummap_exists(UmbraFileContext *ctx)
{
	return umfile_exists(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_create(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
			 bool isRedo)
{
	char		image[UMMAP_ROOT_IMAGE_SIZE];
	UmbraMapRootEntry *entry;
	UmbraMapRootTag tag = {0};
	BlockNumber	nblocks;

	Assert(ctx != NULL);
	umfile_create(ctx, UMBRA_METADATA_FORKNUM, isRedo);
	nblocks = umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);

	/*
	 * Redo callers repair only a missing or invalid root.  Preserve an
	 * existing valid root: it can belong to later lifecycle WAL at the same
	 * locator and must not be reset by an older record.
	 */
	if (nblocks == 0 || (isRedo && !ummap_root_read_valid_image(ctx, image)))
	{
		ummap_root_init(image);
		ummap_root_write_image(ctx, image, false);
		tag.rlocator = rlocator;
		if (UmbraMapRootCacheCtlData != NULL)
			ummap_root_cache_delete_entry(&tag);
	}

	/* Validation and all later access enter through the resident root. */
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	LWLockRelease(&entry->content_lock);
}

bool
ummap_main_slot0_active(UmbraFileContext *ctx,
						RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	bool		active;

	Assert(ctx != NULL);
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	active = (root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) != 0;
	LWLockRelease(&entry->content_lock);
	return active;
}

bool
ummap_try_main_slot0_active(UmbraFileContext *ctx,
							RelFileLocatorBackend rlocator, bool *active)
{
	char		image[UMMAP_ROOT_IMAGE_SIZE];
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(active != NULL);
	(void) rlocator;
	if (!ummap_root_read_valid_image(ctx, image))
		return false;
	root = (UmbraMapRootData *) image;
	*active = (root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) != 0;
	return true;
}

void
ummap_activate_main_slot0(UmbraFileContext *ctx,
						  RelFileLocatorBackend rlocator,
						  XLogRecPtr activation_lsn)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	bool		immediate_sync = InRecovery;

	Assert(ctx != NULL);
	Assert(XLogRecPtrIsValid(activation_lsn));
	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		root->flags |= UMMAP_ROOT_FLAG_MAIN_SLOT0;
		root->logical_eof_main = 0;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		/* The activation LSN is cache-local WAL-before-root ordering state. */
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, activation_lsn);
	}
	LWLockRelease(&entry->content_lock);

	/* Recovery must retain this mapping birth across another crash. */
	if (immediate_sync)
		ummap_immedsync_if_exists(ctx, rlocator);
}

BlockNumber
ummap_get_main_frontier(UmbraFileContext *ctx,
						RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber logical_eof;

	Assert(ctx != NULL);
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontier requires slot-0 activation");
	}
	logical_eof = root->logical_eof_main;
	LWLockRelease(&entry->content_lock);
	return logical_eof;
}

void
ummap_set_main_frontier(UmbraFileContext *ctx,
						RelFileLocatorBackend rlocator,
						BlockNumber logical_eof)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	physical_capacity;

	Assert(ctx != NULL);
	if (!UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity))
		elog(PANIC, "invalid Umbra MAIN mapped frontier");

	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontier requires slot-0 activation");
	}
	if (root->logical_eof_main != logical_eof)
	{
		root->logical_eof_main = logical_eof;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	LWLockRelease(&entry->content_lock);
}

void
ummap_prepare_main_frontier(UmbraFileContext *ctx,
							RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	uint32		flags;

	Assert(ctx != NULL);
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontier requires slot-0 activation");
	}
	flags = root->flags;
	LWLockRelease(&entry->content_lock);

	ummap_prepare_root_flush(ctx, flags);
}

void
ummap_publish_prepared_main_frontier(UmbraFileContext *ctx,
								 RelFileLocatorBackend rlocator,
								 BlockNumber logical_eof)
{
	UmbraMapRootTag tag = {0};
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	physical_capacity;

	Assert(ctx != NULL);
	if (!UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity))
		elog(PANIC, "invalid Umbra MAIN mapped frontier");

	tag.rlocator = rlocator;
	if (!ummap_root_cache_find_locked(&tag, LW_EXCLUSIVE, &entry) ||
		!entry->valid)
		elog(PANIC, "Umbra MAIN mapped frontier was not prepared");
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontier requires slot-0 activation");
	}
	if (root->logical_eof_main != logical_eof)
	{
		root->logical_eof_main = logical_eof;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	ummap_root_cache_flush_prepared_locked(entry, ctx);
	LWLockRelease(&entry->content_lock);

	/* The prepared metadata fork is one short segment and needs no allocation. */
	umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

bool
ummap_aux_slot0_active(UmbraFileContext *ctx, ForkNumber forknum,
						RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	bool		active;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	active = ummap_root_aux_slot0_active(root, forknum);
	LWLockRelease(&entry->content_lock);
	return active;
}

bool
ummap_try_aux_slot0_active(UmbraFileContext *ctx, ForkNumber forknum,
							RelFileLocatorBackend rlocator, bool *active)
{
	char		image[UMMAP_ROOT_IMAGE_SIZE];
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	Assert(active != NULL);
	(void) rlocator;
	if (!ummap_root_read_valid_image(ctx, image))
		return false;
	root = (UmbraMapRootData *) image;
	*active = ummap_root_aux_slot0_active(root, forknum);
	return true;
}

void
ummap_activate_aux_slot0(UmbraFileContext *ctx, ForkNumber forknum,
						 RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra auxiliary slot-0 activation requires MAIN mapping");
	}
	if (!ummap_root_aux_slot0_active(root, forknum))
	{
		root->flags |= ummap_aux_slot0_flag(forknum);
		ummap_root_set_aux_frontier(root, forknum, 0);
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	LWLockRelease(&entry->content_lock);

	/* Auxiliary redo can immediately issue page I/O through the new root. */
	if (InRecovery)
		ummap_immedsync_if_exists(ctx, rlocator);
}

BlockNumber
ummap_get_aux_frontier(UmbraFileContext *ctx, ForkNumber forknum,
						RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber logical_eof;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if (!ummap_root_aux_slot0_active(root, forknum))
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra auxiliary frontier requires slot-0 activation");
	}
	logical_eof = ummap_root_get_aux_frontier(root, forknum);
	LWLockRelease(&entry->content_lock);
	return logical_eof;
}

void
ummap_set_aux_frontier(UmbraFileContext *ctx, ForkNumber forknum,
						RelFileLocatorBackend rlocator,
						BlockNumber logical_eof)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	physical_capacity;
	BlockNumber	current_logical_eof;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	if (!UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity))
		elog(PANIC, "invalid Umbra auxiliary mapped frontier");

	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if (!ummap_root_aux_slot0_active(root, forknum))
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra auxiliary frontier requires slot-0 activation");
	}
	current_logical_eof = ummap_root_get_aux_frontier(root, forknum);
	if (current_logical_eof != logical_eof)
	{
		ummap_root_set_aux_frontier(root, forknum, logical_eof);
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	LWLockRelease(&entry->content_lock);
}

void
ummap_prepare_aux_frontier(UmbraFileContext *ctx, ForkNumber forknum,
						  RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	uint32		flags;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if (!ummap_root_aux_slot0_active(root, forknum))
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra auxiliary frontier requires slot-0 activation");
	}
	flags = root->flags;
	LWLockRelease(&entry->content_lock);

	ummap_prepare_root_flush(ctx, flags);
}

void
ummap_publish_prepared_aux_frontier(UmbraFileContext *ctx,
									ForkNumber forknum,
									RelFileLocatorBackend rlocator,
									BlockNumber logical_eof)
{
	UmbraMapRootTag tag = {0};
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	physical_capacity;
	BlockNumber	current_logical_eof;

	Assert(ctx != NULL);
	Assert(UmbraIsMappedAuxiliaryFork(forknum));
	if (!UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity))
		elog(PANIC, "invalid Umbra auxiliary mapped frontier");

	tag.rlocator = rlocator;
	if (!ummap_root_cache_find_locked(&tag, LW_EXCLUSIVE, &entry) ||
		!entry->valid)
		elog(PANIC, "Umbra auxiliary mapped frontier was not prepared");
	root = (UmbraMapRootData *) entry->image;
	if (!ummap_root_aux_slot0_active(root, forknum))
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra auxiliary frontier requires slot-0 activation");
	}
	current_logical_eof = ummap_root_get_aux_frontier(root, forknum);
	if (current_logical_eof != logical_eof)
	{
		ummap_root_set_aux_frontier(root, forknum, logical_eof);
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	ummap_root_cache_flush_prepared_locked(entry, ctx);
	LWLockRelease(&entry->content_lock);

	/* The metadata fork is one short segment and needs no allocation. */
	umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_validate_if_exists(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;

	Assert(ctx != NULL);
	if (!ummap_exists(ctx))
		return;

	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	LWLockRelease(&entry->content_lock);
}

void
ummap_flush_relation(UmbraFileContext *ctx, RelFileLocatorBackend rlocator)
{
	UmbraMapRootTag tag = {0};
	UmbraMapRootEntry *entry;

	Assert(ctx != NULL);
	MapFlushRelation(ctx, rlocator);
	if (UmbraMapRootCacheCtlData == NULL)
		return;
	ummap_root_cache_ensure_initialized();
	tag.rlocator = rlocator;
	if (!ummap_root_cache_find_locked(&tag, LW_EXCLUSIVE, &entry))
		return;
	ummap_root_cache_flush_locked(entry, ctx);
	LWLockRelease(&entry->content_lock);
}

void
ummap_flush_database_tablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapFlushDatabaseTablespace(dbid, spcOid);
	ummap_root_cache_flush_matching(dbid, spcOid);
}

void
ummap_invalidate_database(Oid dbid)
{
	Assert(OidIsValid(dbid));
	MapInvalidateDatabase(dbid);
	ummap_root_cache_invalidate_matching(dbid, InvalidOid);
}

void
ummap_invalidate_database_tablespace(Oid dbid, Oid spcOid)
{
	Assert(OidIsValid(dbid));
	Assert(OidIsValid(spcOid));
	MapInvalidateDatabaseTablespace(dbid, spcOid);
	ummap_root_cache_invalidate_matching(dbid, spcOid);
}

void
ummap_checkpoint(void)
{
	MapCheckpoint();
	ummap_root_cache_flush_matching(InvalidOid, InvalidOid);
	MapCheckpoint();
}

void
ummap_immedsync_if_exists(UmbraFileContext *ctx,
					  RelFileLocatorBackend rlocator)
{
	Assert(ctx != NULL);
	if (ummap_exists(ctx))
	{
		ummap_flush_relation(ctx, rlocator);
		umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
	}
}

void
ummap_registersync_if_exists(UmbraFileContext *ctx,
					   RelFileLocatorBackend rlocator)
{
	Assert(ctx != NULL);
	if (ummap_exists(ctx))
	{
		ummap_flush_relation(ctx, rlocator);
		umfile_registersync(ctx, UMBRA_METADATA_FORKNUM);
	}
}

void
ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	UmbraMapRootTag tag = {0};

	MapInvalidateRelation(rlocator);
	tag.rlocator = rlocator;
	if (UmbraMapRootCacheCtlData != NULL)
		ummap_root_cache_delete_entry(&tag);
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

static uint32
ummap_aux_slot0_flag(ForkNumber forknum)
{
	switch (forknum)
	{
		case FSM_FORKNUM:
			return UMMAP_ROOT_FLAG_FSM_SLOT0;
		case VISIBILITYMAP_FORKNUM:
			return UMMAP_ROOT_FLAG_VM_SLOT0;
		default:
			elog(PANIC, "invalid Umbra mapped auxiliary fork %d",
				 (int) forknum);
	}
	return 0;
}

static bool
ummap_root_aux_slot0_active(const UmbraMapRootData *root,
							 ForkNumber forknum)
{
	Assert(root != NULL);
	return (root->flags & ummap_aux_slot0_flag(forknum)) != 0;
}

static BlockNumber
ummap_root_get_aux_frontier(const UmbraMapRootData *root,
							ForkNumber forknum)
{
	Assert(root != NULL);
	switch (forknum)
	{
		case FSM_FORKNUM:
			return root->logical_eof_fsm;
		case VISIBILITYMAP_FORKNUM:
			return root->logical_eof_vm;
		default:
			elog(PANIC, "invalid Umbra mapped auxiliary fork %d",
				 (int) forknum);
	}
	return InvalidBlockNumber;
}

static void
ummap_root_set_aux_frontier(UmbraMapRootData *root, ForkNumber forknum,
							BlockNumber logical_eof)
{
	Assert(root != NULL);
	switch (forknum)
	{
		case FSM_FORKNUM:
			root->logical_eof_fsm = logical_eof;
			break;
		case VISIBILITYMAP_FORKNUM:
			root->logical_eof_vm = logical_eof;
			break;
		default:
			elog(PANIC, "invalid Umbra mapped auxiliary fork %d",
				 (int) forknum);
	}
}

static bool
ummap_root_aux_frontier_valid(const UmbraMapRootData *root,
								ForkNumber forknum)
{
	BlockNumber	logical_eof;
	BlockNumber	physical_capacity;

	logical_eof = ummap_root_get_aux_frontier(root, forknum);
	if (!ummap_root_aux_slot0_active(root, forknum))
		return !BlockNumberIsValid(logical_eof);
	return BlockNumberIsValid(logical_eof) &&
		UmbraMappedPhysicalCapacity(logical_eof, &physical_capacity);
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
	root->chunk_pages = UMBRA_CHUNK_PAIRED_PAGES;
	root->flags = UMMAP_ROOT_FLAGS_V1;
	root->logical_eof_main = 0;
	root->logical_eof_fsm = InvalidBlockNumber;
	root->logical_eof_vm = InvalidBlockNumber;
	ummap_root_refresh_crc(root);
}

static void
ummap_root_refresh_crc(UmbraMapRootData *root)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, root, offsetof(UmbraMapRootData, crc));
	FIN_CRC32C(crc);
	root->crc = crc;
}

static bool
ummap_root_is_valid(const UmbraMapRootData *root)
{
	pg_crc32c	crc;
	BlockNumber	expected_capacity;

	if (root->magic != UMMAP_ROOT_MAGIC ||
		root->version != UMMAP_ROOT_VERSION ||
		root->blcksz != BLCKSZ ||
		root->chunk_pages != UMBRA_CHUNK_PAIRED_PAGES ||
		(root->flags & UMMAP_ROOT_FLAGS_V1) == 0 ||
		(root->flags & ~UMMAP_ROOT_FLAGS_KNOWN) != 0 ||
		!BlockNumberIsValid(root->logical_eof_main) ||
		!pg_memory_is_all_zeros(root->reserved, sizeof(root->reserved)))
		return false;
	if (((root->flags & (UMMAP_ROOT_FLAG_FSM_SLOT0 |
						UMMAP_ROOT_FLAG_VM_SLOT0)) != 0 &&
		 (root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0) ||
		!ummap_root_aux_frontier_valid(root, FSM_FORKNUM) ||
		!ummap_root_aux_frontier_valid(root, VISIBILITYMAP_FORKNUM))
		return false;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		if (root->logical_eof_main != 0)
			return false;
	}
	else if (!UmbraMappedPhysicalCapacity(root->logical_eof_main,
									 &expected_capacity))
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, root, offsetof(UmbraMapRootData, crc));
	FIN_CRC32C(crc);
	return crc == root->crc;
}

static void
ummap_root_load_image(UmbraFileContext *ctx, char *image)
{
	if (!ummap_root_read_valid_image(ctx, image))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Umbra metadata root is corrupted or incompatible")));
}

static bool
ummap_root_read_valid_image(UmbraFileContext *ctx, char *image)
{
	char		sector[UMMAP_ROOT_SECTOR_SIZE];

	Assert(ctx != NULL);
	Assert(image != NULL);
	if (!ummap_exists(ctx) ||
		umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM) < 1)
		return false;

	umfile_read_bytes(ctx, UMBRA_METADATA_FORKNUM, 0, sector,
					  UMMAP_ROOT_SECTOR_SIZE);
	if (!pg_memory_is_all_zeros(sector + UMMAP_ROOT_IMAGE_SIZE,
							  UMMAP_ROOT_SECTOR_SIZE -
							  UMMAP_ROOT_IMAGE_SIZE) ||
		!ummap_root_is_valid((UmbraMapRootData *) sector))
		return false;
	memcpy(image, sector, UMMAP_ROOT_IMAGE_SIZE);
	return true;
}

static void
ummap_root_write_image(UmbraFileContext *ctx, const char *image,
							 bool skipFsync)
{
	char		sector[UMMAP_ROOT_SECTOR_SIZE];
	PGIOAlignedBlock page = {0};
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(image != NULL);
	MemSet(sector, 0, sizeof(sector));
	memcpy(sector, image, UMMAP_ROOT_IMAGE_SIZE);
	root = (UmbraMapRootData *) sector;
	ummap_root_refresh_crc(root);
	Assert(ummap_root_is_valid(root));
	if (umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM) == 0)
	{
		/* umfile_extend() always writes one aligned BLCKSZ buffer. */
		memcpy(page.data, sector, sizeof(sector));
		umfile_extend(ctx, UMBRA_METADATA_FORKNUM, 0, page.data, skipFsync);
	}
	else
		umfile_write_bytes(ctx, UMBRA_METADATA_FORKNUM, 0, sector,
						   UMMAP_ROOT_SECTOR_SIZE, skipFsync);
}

static Size
ummap_root_cache_shmem_size(void)
{
	Assert(dsa_minimum_size() <= UMMAP_ROOT_DSA_INIT_SIZE);
	return add_size(MAXALIGN(sizeof(UmbraMapRootCacheCtl)),
				UMMAP_ROOT_DSA_INIT_SIZE);
}

static void
ummap_root_cache_request(void *arg)
{
	ShmemRequestStruct(.name = "Umbra metadata root cache",
					   .size = ummap_root_cache_shmem_size(),
					   .ptr = (void **) &UmbraMapRootCacheCtlData,
		);
}

static void
ummap_root_cache_init(void *arg)
{
	dsa_area   *dsa;
	dshash_table *hash;
	char       *raw_area;

	raw_area = (char *) UmbraMapRootCacheCtlData +
		MAXALIGN(sizeof(UmbraMapRootCacheCtl));
	UmbraMapRootCacheCtlData->raw_dsa_area = raw_area;

	dsa = dsa_create_in_place(raw_area, UMMAP_ROOT_DSA_INIT_SIZE,
					  LWTRANCHE_UMBRA_ROOT_MAPPING, NULL);
	dsa_pin(dsa);
	dsa_set_size_limit(dsa, UMMAP_ROOT_DSA_INIT_SIZE);
	hash = dshash_create(dsa, &UmbraMapRootHashParams, NULL);
	UmbraMapRootCacheCtlData->hash_handle =
		dshash_get_hash_table_handle(hash);
	dsa_set_size_limit(dsa, UMMAP_ROOT_DSA_MAX_SIZE);

	dshash_detach(hash);
	dsa_detach(dsa);
}

void
ummap_root_cache_backend_init(void)
{
	if (IsUnderPostmaster && !ummap_root_cache_exit_handler_registered)
	{
		before_shmem_exit(ummap_root_cache_detach, 0);
		ummap_root_cache_exit_handler_registered = true;
	}
}

static void
ummap_root_cache_attach(void)
{
	MemoryContext oldcontext;
	dsa_area   *volatile dsa = NULL;
	dshash_table *volatile hash = NULL;

	if (UmbraMapRootHash != NULL)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	PG_TRY();
	{
		dsa = dsa_attach_in_place(UmbraMapRootCacheCtlData->raw_dsa_area,
								  NULL);
		dsa_pin_mapping(dsa);
		if (!IsUnderPostmaster)
			dsa_set_size_limit(dsa, UMMAP_ROOT_DSA_INIT_SIZE);
		hash = dshash_attach(dsa, &UmbraMapRootHashParams,
							 UmbraMapRootCacheCtlData->hash_handle, NULL);
	}
	PG_CATCH();
	{
		if (hash != NULL)
			dshash_detach(hash);
		if (dsa != NULL)
		{
			dsa_detach(dsa);
			dsa_release_in_place(UmbraMapRootCacheCtlData->raw_dsa_area);
		}
		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	UmbraMapRootDSA = dsa;
	UmbraMapRootHash = hash;
	MemoryContextSwitchTo(oldcontext);
}

static void
ummap_root_cache_detach(int code, Datum arg)
{
	if (UmbraMapRootHash != NULL)
	{
		dshash_detach(UmbraMapRootHash);
		UmbraMapRootHash = NULL;
	}
	if (UmbraMapRootDSA != NULL)
	{
		dsa_detach(UmbraMapRootDSA);
		dsa_release_in_place(UmbraMapRootCacheCtlData->raw_dsa_area);
		UmbraMapRootDSA = NULL;
	}
}

static void
ummap_root_cache_ensure_initialized(void)
{
	if (UmbraMapRootCacheCtlData == NULL)
		elog(ERROR, "Umbra metadata root cache is not initialized");
	if (UmbraMapRootHash == NULL)
		ummap_root_cache_attach();
}

static bool
ummap_root_cache_find_locked(const UmbraMapRootTag *tag, LWLockMode mode,
							 UmbraMapRootEntry **entry)
{
	Assert(tag != NULL);
	Assert(entry != NULL);
	if (UmbraMapRootHash == NULL)
		return false;
	*entry = dshash_find(UmbraMapRootHash, tag, false);
	if (*entry == NULL)
		return false;
	LWLockAcquire(&(*entry)->content_lock, mode);
	dshash_release_lock(UmbraMapRootHash, *entry);
	return true;
}

static UmbraMapRootEntry *
ummap_root_cache_ensure_entry_locked(const UmbraMapRootTag *tag)
{
	UmbraMapRootEntry *entry;
	bool        found;

	entry = dshash_find_or_insert_extended(UmbraMapRootHash, tag, &found,
										  DSHASH_INSERT_NO_OOM);
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not allocate an Umbra metadata root cache entry"),
				 errdetail("The fixed 64 MiB cache has no replacement policy.")));
	if (!found)
	{
		LWLockInitialize(&entry->content_lock, LWTRANCHE_UMBRA_ROOT_CONTENT);
		entry->valid = false;
		entry->dirty = false;
		entry->needs_fsync = false;
		entry->wal_flush_lsn = InvalidXLogRecPtr;
		MemSet(entry->image, 0, sizeof(entry->image));
	}
	LWLockAcquire(&entry->content_lock, LW_EXCLUSIVE);
	dshash_release_lock(UmbraMapRootHash, entry);
	return entry;
}

static UmbraMapRootEntry *
ummap_root_cache_get(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
					 LWLockMode mode)
{
	UmbraMapRootTag tag = {0};
	UmbraMapRootEntry *entry;

	Assert(ctx != NULL);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	ummap_root_cache_ensure_initialized();
	tag.rlocator = rlocator;

	for (;;)
	{
		if (ummap_root_cache_find_locked(&tag, mode, &entry))
		{
			if (entry->valid)
				break;
			if (mode == LW_EXCLUSIVE)
			{
				ummap_root_cache_load_locked(ctx, entry);
				entry->valid = true;
				entry->dirty = false;
				entry->needs_fsync = false;
				entry->wal_flush_lsn = InvalidXLogRecPtr;
				break;
			}
			LWLockRelease(&entry->content_lock);
			if (!ummap_root_cache_find_locked(&tag, LW_EXCLUSIVE, &entry))
				continue;
		}
		else
			entry = ummap_root_cache_ensure_entry_locked(&tag);

		if (!entry->valid)
		{
			ummap_root_cache_load_locked(ctx, entry);
			entry->valid = true;
			entry->dirty = false;
			entry->needs_fsync = false;
			entry->wal_flush_lsn = InvalidXLogRecPtr;
		}
		break;
	}

	return entry;
}

static void
ummap_root_cache_load_locked(UmbraFileContext *ctx, UmbraMapRootEntry *entry)
{
	Assert(ctx != NULL);
	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->content_lock, LW_EXCLUSIVE));
	ummap_root_load_image(ctx, entry->image);
}

static void
ummap_root_cache_flush_locked(UmbraMapRootEntry *entry,
						  UmbraFileContext *ctx)
{
	ummap_root_cache_flush_internal_locked(entry, ctx, false);
}

/*
 * Truncate prepares all active data and metadata descriptors before entering
 * its critical section.  Do not open transient inactive segments while the
 * prepared root publication is in progress.
 */
static void
ummap_root_cache_flush_prepared_locked(UmbraMapRootEntry *entry,
									   UmbraFileContext *ctx)
{
	Assert(ctx != NULL);
	ummap_root_cache_flush_internal_locked(entry, ctx, true);
}

static void
ummap_root_cache_flush_internal_locked(UmbraMapRootEntry *entry,
									  UmbraFileContext *ctx, bool prepared)
{
	UmbraFileContext *volatile temporary_ctx = NULL;
	UmbraFileContext *write_ctx;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->content_lock, LW_EXCLUSIVE));
	if (!entry->valid || !entry->dirty)
		return;

	PG_TRY();
	{
		if (ctx == NULL)
		{
			temporary_ctx = umfile_open_temporary(entry->tag.rlocator);
			write_ctx = temporary_ctx;
		}
		else
			write_ctx = ctx;

		if (XLogRecPtrIsValid(entry->wal_flush_lsn))
			XLogFlush(entry->wal_flush_lsn);
		/*
		 * Each mapped logical EOF determines its physical capacity.  Do not
		 * allow the root to become durable before any activated data fork it
		 * describes: checkpoint fsync requests have no ordering guarantee
		 * between forks.
		 */
		if ((((UmbraMapRootData *) entry->image)->flags &
			 UMMAP_ROOT_FLAG_MAIN_SLOT0) != 0)
		{
			if (prepared)
				umfile_immedsync_prepared(write_ctx, MAIN_FORKNUM);
			else
				umfile_immedsync(write_ctx, MAIN_FORKNUM);
		}
		if ((((UmbraMapRootData *) entry->image)->flags &
			 UMMAP_ROOT_FLAG_FSM_SLOT0) != 0)
		{
			if (prepared)
				umfile_immedsync_prepared(write_ctx, FSM_FORKNUM);
			else
				umfile_immedsync(write_ctx, FSM_FORKNUM);
		}
		if ((((UmbraMapRootData *) entry->image)->flags &
			 UMMAP_ROOT_FLAG_VM_SLOT0) != 0)
		{
			if (prepared)
				umfile_immedsync_prepared(write_ctx, VISIBILITYMAP_FORKNUM);
			else
				umfile_immedsync(write_ctx, VISIBILITYMAP_FORKNUM);
		}
		/*
		 * Selector pages share the metadata file with the root.  Make their
		 * WAL-protected contents durable before a root write can make this
		 * metadata state authoritative.
		 */
		MapFlushRelation(write_ctx, entry->tag.rlocator);
		umfile_immedsync(write_ctx, UMBRA_METADATA_FORKNUM);
		ummap_root_write_image(write_ctx, entry->image, !entry->needs_fsync);
		if (temporary_ctx != NULL)
		{
			umfile_destroy(temporary_ctx);
			temporary_ctx = NULL;
		}

		entry->dirty = false;
		entry->needs_fsync = false;
		entry->wal_flush_lsn = InvalidXLogRecPtr;
	}
	PG_CATCH();
	{
		if (temporary_ctx != NULL)
			umfile_destroy(temporary_ctx);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/* Open every file the prepared root publication may need to sync or write. */
static void
ummap_prepare_root_flush(UmbraFileContext *ctx, uint32 flags)
{
	Assert(ctx != NULL);
	if ((flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) != 0)
		(void) umfile_nblocks(ctx, MAIN_FORKNUM);
	if ((flags & UMMAP_ROOT_FLAG_FSM_SLOT0) != 0)
		(void) umfile_nblocks(ctx, FSM_FORKNUM);
	if ((flags & UMMAP_ROOT_FLAG_VM_SLOT0) != 0)
		(void) umfile_nblocks(ctx, VISIBILITYMAP_FORKNUM);
	(void) umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);
}

static int
ummap_root_cache_collect_tags(Oid dbid, Oid spcOid, UmbraMapRootTag **tags)
{
	dshash_seq_status status;
	UmbraMapRootEntry *entry;
	UmbraMapRootTag *result = NULL;
	int         capacity = 0;
	int         count = 0;

	Assert(tags != NULL);
	ummap_root_cache_ensure_initialized();
	dshash_seq_init(&status, UmbraMapRootHash, false);
	while ((entry = dshash_seq_next(&status)) != NULL)
	{
		if ((OidIsValid(dbid) &&
			 entry->tag.rlocator.locator.dbOid != dbid) ||
			(OidIsValid(spcOid) &&
			 entry->tag.rlocator.locator.spcOid != spcOid))
			continue;
		if (count == capacity)
		{
			capacity = capacity == 0 ? 128 : capacity * 2;
			if (result == NULL)
				result = palloc_array(UmbraMapRootTag, capacity);
			else
				result = repalloc_array(result, UmbraMapRootTag, capacity);
		}
		result[count++] = entry->tag;
	}
	dshash_seq_term(&status);
	*tags = result;
	return count;
}

static void
ummap_root_cache_flush_matching(Oid dbid, Oid spcOid)
{
	UmbraMapRootTag *tags;
	int         count;

	if (UmbraMapRootCacheCtlData == NULL)
		return;
	count = ummap_root_cache_collect_tags(dbid, spcOid, &tags);
	for (int i = 0; i < count; i++)
	{
		UmbraMapRootEntry *entry;

		if (!ummap_root_cache_find_locked(&tags[i], LW_EXCLUSIVE, &entry))
			continue;
		ummap_root_cache_flush_locked(entry, NULL);
		LWLockRelease(&entry->content_lock);
	}
	if (tags != NULL)
		pfree(tags);
}

static void
ummap_root_cache_delete_entry(const UmbraMapRootTag *tag)
{
	UmbraMapRootEntry *entry;

	Assert(tag != NULL);
	ummap_root_cache_ensure_initialized();
	entry = dshash_find(UmbraMapRootHash, tag, true);
	if (entry == NULL)
		return;
	LWLockAcquire(&entry->content_lock, LW_EXCLUSIVE);
	LWLockRelease(&entry->content_lock);
	dshash_delete_entry(UmbraMapRootHash, entry);
}

static void
ummap_root_cache_invalidate_matching(Oid dbid, Oid spcOid)
{
	UmbraMapRootTag *tags;
	int         count;

	if (UmbraMapRootCacheCtlData == NULL)
		return;
	count = ummap_root_cache_collect_tags(dbid, spcOid, &tags);
	for (int i = 0; i < count; i++)
		ummap_root_cache_delete_entry(&tags[i]);
	if (tags != NULL)
		pfree(tags);
}
