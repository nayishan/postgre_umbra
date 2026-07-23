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
#define UMMAP_ROOT_VERSION		1U
#define UMMAP_ROOT_FLAGS_V1	0x00000001U
#define UMMAP_ROOT_FLAG_MAIN_SLOT0 0x00000002U
#define UMMAP_ROOT_FLAGS_KNOWN \
	(UMMAP_ROOT_FLAGS_V1 | UMMAP_ROOT_FLAG_MAIN_SLOT0)
#define UMMAP_ROOT_IMAGE_SIZE	64
#define UMMAP_ROOT_SECTOR_SIZE	512

#define UMMAP_ROOT_DSA_INIT_SIZE (2 * 1024 * 1024)
#define UMMAP_ROOT_DSA_MAX_SIZE  (64 * 1024 * 1024)

/*
 * The format reserves FSM and VM values now so their later activation does
 * not change the root layout.  InvalidBlockNumber means that fork has not
 * entered the chunk mapping policy.
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
	uint8		reserved_before_capacity[8];
	BlockNumber physical_capacity_main;
	BlockNumber physical_capacity_fsm;
	BlockNumber physical_capacity_vm;
	uint8		reserved[8];
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
StaticAssertDecl(offsetof(UmbraMapRootData, reserved_before_capacity) == 32,
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
static int ummap_root_cache_collect_tags(Oid dbid, Oid spcOid,
	UmbraMapRootTag **tags);
static void ummap_root_cache_flush_matching(Oid dbid, Oid spcOid);
static void ummap_root_cache_delete_entry(const UmbraMapRootTag *tag);
static void ummap_root_cache_invalidate_matching(Oid dbid, Oid spcOid);

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
	 * Only an actual CREATE finish path calls this during redo.  Preserve an
	 * existing valid root: it can belong to later lifecycle WAL at the same
	 * locator and must not be reset by an old CREATE record.
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
						  XLogRecPtr create_lsn)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	bool		immediate_sync = InRecovery;

	Assert(ctx != NULL);
	Assert(XLogRecPtrIsValid(create_lsn));
	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		root->flags |= UMMAP_ROOT_FLAG_MAIN_SLOT0;
		root->logical_eof_main = 0;
		root->physical_capacity_main = 0;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		/* The create LSN is cache-local WAL-before-root ordering state. */
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, create_lsn);
	}
	LWLockRelease(&entry->content_lock);

	/* Recovery must retain this mapping birth across another crash. */
	if (immediate_sync)
		ummap_immedsync_if_exists(ctx, rlocator);
}

void
ummap_get_main_frontiers(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 BlockNumber *logical_eof,
						 BlockNumber *physical_capacity)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	Assert(logical_eof != NULL);
	Assert(physical_capacity != NULL);
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontiers require slot-0 activation");
	}
	*logical_eof = root->logical_eof_main;
	*physical_capacity = root->physical_capacity_main;
	LWLockRelease(&entry->content_lock);
}

void
ummap_set_main_frontiers(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator,
						 BlockNumber logical_eof,
						 BlockNumber physical_capacity)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	expected_capacity;

	Assert(ctx != NULL);
	if (!UmbraMainSlot0PhysicalCapacity(logical_eof, &expected_capacity) ||
		physical_capacity != expected_capacity)
		elog(PANIC, "invalid Umbra MAIN slot-0 frontiers");

	entry = ummap_root_cache_get(ctx, rlocator, LW_EXCLUSIVE);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontiers require slot-0 activation");
	}
	if (root->logical_eof_main != logical_eof ||
		root->physical_capacity_main != physical_capacity)
	{
		root->logical_eof_main = logical_eof;
		root->physical_capacity_main = physical_capacity;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	LWLockRelease(&entry->content_lock);
}

void
ummap_prepare_main_frontiers(UmbraFileContext *ctx,
							 RelFileLocatorBackend rlocator)
{
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;

	Assert(ctx != NULL);
	entry = ummap_root_cache_get(ctx, rlocator, LW_SHARED);
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontiers require slot-0 activation");
	}
	LWLockRelease(&entry->content_lock);

	/* Open the metadata descriptor before the truncation critical section. */
	(void) umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_publish_prepared_main_frontiers(UmbraFileContext *ctx,
								  RelFileLocatorBackend rlocator,
								  BlockNumber logical_eof,
								  BlockNumber physical_capacity)
{
	UmbraMapRootTag tag = {0};
	UmbraMapRootEntry *entry;
	UmbraMapRootData *root;
	BlockNumber	expected_capacity;

	Assert(ctx != NULL);
	if (!UmbraMainSlot0PhysicalCapacity(logical_eof, &expected_capacity) ||
		physical_capacity != expected_capacity)
		elog(PANIC, "invalid Umbra MAIN slot-0 frontiers");

	tag.rlocator = rlocator;
	if (!ummap_root_cache_find_locked(&tag, LW_EXCLUSIVE, &entry) ||
		!entry->valid)
		elog(PANIC, "Umbra MAIN slot-0 frontiers were not prepared");
	root = (UmbraMapRootData *) entry->image;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		LWLockRelease(&entry->content_lock);
		elog(PANIC, "Umbra MAIN frontiers require slot-0 activation");
	}
	if (root->logical_eof_main != logical_eof ||
		root->physical_capacity_main != physical_capacity)
	{
		root->logical_eof_main = logical_eof;
		root->physical_capacity_main = physical_capacity;
		ummap_root_refresh_crc(root);
		entry->dirty = true;
		entry->needs_fsync = true;
		entry->wal_flush_lsn = InRecovery ? InvalidXLogRecPtr :
			Max(entry->wal_flush_lsn, GetXLogWriteRecPtr());
	}
	ummap_root_cache_flush_locked(entry, ctx);
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
	root->physical_capacity_main = 0;
	root->physical_capacity_fsm = InvalidBlockNumber;
	root->physical_capacity_vm = InvalidBlockNumber;
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
		!BlockNumberIsValid(root->physical_capacity_main) ||
		BlockNumberIsValid(root->logical_eof_fsm) ||
		BlockNumberIsValid(root->logical_eof_vm) ||
		BlockNumberIsValid(root->physical_capacity_fsm) ||
		BlockNumberIsValid(root->physical_capacity_vm) ||
		!pg_memory_is_all_zeros(root->reserved_before_capacity,
								 sizeof(root->reserved_before_capacity)) ||
		!pg_memory_is_all_zeros(root->reserved, sizeof(root->reserved)))
		return false;
	if ((root->flags & UMMAP_ROOT_FLAG_MAIN_SLOT0) == 0)
	{
		if (root->logical_eof_main != 0 ||
			root->physical_capacity_main != 0)
			return false;
	}
	else if (!UmbraMainSlot0PhysicalCapacity(root->logical_eof_main,
											 &expected_capacity) ||
			 root->physical_capacity_main != expected_capacity)
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
		/* Keep MAIN durable before its authoritative capacity reaches disk. */
		if ((((UmbraMapRootData *) entry->image)->flags &
			 UMMAP_ROOT_FLAG_MAIN_SLOT0) != 0)
			umfile_immedsync(write_ctx, MAIN_FORKNUM);
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
