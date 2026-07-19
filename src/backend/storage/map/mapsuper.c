/*-------------------------------------------------------------------------
 *
 * mapsuper.c
 *	  Resident cache for Umbra MAP roots.
 *
 * Entries are allocated from a bounded dynamic shared area as relations are
 * first accessed.  There is no replacement policy in this patch: entries
 * remain resident until relation or database invalidation removes them, and
 * insertion reports an error if the cache reaches its fixed limit.
 *
 * This module owns residency, locking, dirty state, and writeback ordering.
 * The cached bytes are opaque here; ummap.c owns their format, validation,
 * CRC, and full-block physical I/O.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/map/mapsuper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/map_internal.h"
#include "storage/shmem.h"
#include "storage/umfile.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

#define MAP_SUPER_DSA_INIT_SIZE (2 * 1024 * 1024)
#define MAP_SUPER_DSA_MAX_SIZE (64 * 1024 * 1024)

StaticAssertDecl(MAP_SUPER_DSA_INIT_SIZE <= MAP_SUPER_DSA_MAX_SIZE,
				 "Umbra MAP root cache initial size exceeds its limit");

MapSuperCacheCtl *MapSuperCacheCtlData = NULL;

static dsa_area *MapSuperDSA = NULL;
static dshash_table *MapSuperHash = NULL;

static const dshash_parameters MapSuperHashParams = {
	.key_size = sizeof(MapSuperTag),
	.entry_size = sizeof(MapSuperDesc),
	.compare_function = dshash_memcmp,
	.hash_function = dshash_memhash,
	.copy_function = dshash_memcpy,
	.tranche_id = LWTRANCHE_MAP_SUPER_MAPPING,
};

static Size MapSuperShmemSize(void);
static void MapSuperAttach(void);
static void MapSuperDetach(int code, Datum arg);
static MapSuperDesc *MapSuperEnsureEntryLocked(const MapSuperTag *tag);
static void MapSuperLoadLocked(UmbraFileContext *ctx, MapSuperDesc *desc);

MapSuperBuffer
MapSuperBufferRead(UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
				   LWLockMode mode)
{
	MapSuperBuffer buffer;
	MapSuperDesc *desc;
	MapSuperTag tag = {0};

	Assert(ctx != NULL);
	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	MapSuperEnsureInitialized();
	tag.rlocator = rlocator;

	for (;;)
	{
		if (MapSuperFindEntryLocked(&tag, mode, &desc))
		{
			if (desc->valid)
				break;

			if (mode == LW_EXCLUSIVE)
			{
				MapSuperLoadLocked(ctx, desc);
				desc->valid = true;
				desc->dirty = false;
				desc->needs_fsync = false;
				desc->wal_flush_lsn = InvalidXLogRecPtr;
				break;
			}

			LWLockRelease(&desc->content_lock);
			if (!MapSuperFindEntryLocked(&tag, LW_EXCLUSIVE, &desc))
				continue;
		}
		else
			desc = MapSuperEnsureEntryLocked(&tag);

		if (!desc->valid)
		{
			MapSuperLoadLocked(ctx, desc);
			desc->valid = true;
			desc->dirty = false;
			desc->needs_fsync = false;
			desc->wal_flush_lsn = InvalidXLogRecPtr;
		}
		break;
	}

	buffer.desc = desc;
	return buffer;
}

char *
MapSuperBufferGetData(MapSuperBuffer buffer)
{
	MapSuperDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	Assert(desc->valid);

	return desc->data;
}

void
MapSuperMarkBufferDirty(MapSuperBuffer buffer, bool skipFsync,
						XLogRecPtr wal_flush_lsn)
{
	MapSuperDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	Assert(desc->valid);

	desc->dirty = true;
	if (!skipFsync)
		desc->needs_fsync = true;
	if (wal_flush_lsn > desc->wal_flush_lsn)
		desc->wal_flush_lsn = wal_flush_lsn;
}

void
MapSuperReleaseBuffer(MapSuperBuffer buffer)
{
	MapSuperDesc *desc = buffer.desc;

	Assert(desc != NULL);
	Assert(LWLockHeldByMe(&desc->content_lock));
	LWLockRelease(&desc->content_lock);
}

bool
MapSuperFindEntryLocked(const MapSuperTag *tag, LWLockMode mode,
						MapSuperDesc **desc)
{
	Assert(tag != NULL);
	Assert(desc != NULL);

	/* This lookup never attaches the DSA or creates an entry. */
	if (MapSuperHash == NULL)
		return false;
	*desc = dshash_find(MapSuperHash, tag, false);
	if (*desc == NULL)
		return false;

	LWLockAcquire(&(*desc)->content_lock, mode);
	dshash_release_lock(MapSuperHash, *desc);
	return true;
}

void
MapSuperDeleteEntry(const MapSuperTag *tag)
{
	MapSuperDesc *desc;

	Assert(tag != NULL);
	MapSuperEnsureInitialized();
	desc = dshash_find(MapSuperHash, tag, true);
	if (desc == NULL)
		return;

	/* Wait for every user before removing and freeing the entry. */
	LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
	LWLockRelease(&desc->content_lock);
	dshash_delete_entry(MapSuperHash, desc);
}

void
MapSuperFlushLocked(MapSuperDesc *desc, UmbraFileContext *ctx)
{
	UmbraFileContext *volatile temporary_ctx = NULL;
	UmbraFileContext *write_ctx;

	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));

	if (!desc->valid || !desc->dirty)
		return;

	PG_TRY();
	{
		if (ctx == NULL)
		{
			temporary_ctx = umfile_open_temporary(desc->tag.rlocator);
			write_ctx = temporary_ctx;
		}
		else
			write_ctx = ctx;

		if (XLogRecPtrIsValid(desc->wal_flush_lsn))
			XLogFlush(desc->wal_flush_lsn);
		ummap_root_write_image(write_ctx, desc->data, !desc->needs_fsync);
		if (temporary_ctx != NULL)
		{
			umfile_destroy(temporary_ctx);
			temporary_ctx = NULL;
		}

		desc->dirty = false;
		desc->needs_fsync = false;
		desc->wal_flush_lsn = InvalidXLogRecPtr;
	}
	PG_CATCH();
	{
		if (temporary_ctx != NULL)
			umfile_destroy(temporary_ctx);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

int
MapSuperCollectTags(Oid dbid, Oid spcOid, MapSuperTag **tags)
{
	dshash_seq_status status;
	MapSuperDesc *desc;
	MapSuperTag *result = NULL;
	int			capacity = 0;
	int			count = 0;

	Assert(tags != NULL);
	MapSuperEnsureInitialized();

	dshash_seq_init(&status, MapSuperHash, false);
	while ((desc = dshash_seq_next(&status)) != NULL)
	{
		if ((OidIsValid(dbid) &&
			 desc->tag.rlocator.locator.dbOid != dbid) ||
			(OidIsValid(spcOid) &&
			 desc->tag.rlocator.locator.spcOid != spcOid))
			continue;

		if (count == capacity)
		{
			capacity = capacity == 0 ? 128 : capacity * 2;
			if (result == NULL)
				result = palloc_array(MapSuperTag, capacity);
			else
				result = repalloc_array(result, MapSuperTag, capacity);
		}
		result[count++] = desc->tag;
	}
	dshash_seq_term(&status);

	*tags = result;
	return count;
}

void
MapSuperEnsureInitialized(void)
{
	if (MapSuperCacheCtlData == NULL)
		elog(ERROR, "Umbra MAP root cache is not initialized");
	if (MapSuperHash == NULL)
		MapSuperAttach();
}

static MapSuperDesc *
MapSuperEnsureEntryLocked(const MapSuperTag *tag)
{
	MapSuperDesc *desc;
	bool		found;

	desc = dshash_find_or_insert_extended(MapSuperHash, tag, &found,
										 DSHASH_INSERT_NO_OOM);
	if (desc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not allocate an Umbra MAP root cache entry"),
				 errdetail("The fixed 64 MiB cache has no replacement policy.")));
	if (!found)
	{
		LWLockInitialize(&desc->content_lock, LWTRANCHE_MAP_SUPER_CONTENT);
		desc->valid = false;
		desc->dirty = false;
		desc->needs_fsync = false;
		desc->wal_flush_lsn = InvalidXLogRecPtr;
		MemSet(desc->data, 0, sizeof(desc->data));
	}

	/* Initialize and content-lock the entry before releasing its hash lock. */
	LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
	dshash_release_lock(MapSuperHash, desc);
	return desc;
}

static void
MapSuperLoadLocked(UmbraFileContext *ctx, MapSuperDesc *desc)
{
	Assert(ctx != NULL);
	Assert(desc != NULL);
	Assert(LWLockHeldByMeInMode(&desc->content_lock, LW_EXCLUSIVE));
	ummap_root_load_image(ctx, desc->data);
}

static Size
MapSuperShmemSize(void)
{
	Assert(dsa_minimum_size() <= MAP_SUPER_DSA_INIT_SIZE);
	return add_size(MAXALIGN(sizeof(MapSuperCacheCtl)),
					MAP_SUPER_DSA_INIT_SIZE);
}

void
MapSuperTableShmemRequest(void)
{
	ShmemRequestStruct(.name = "Umbra MAP Root Cache",
					   .size = MapSuperShmemSize(),
					   .ptr = (void **) &MapSuperCacheCtlData,
		);
}

void
MapSuperTableShmemInit(void)
{
	dsa_area   *dsa;
	dshash_table *hash;
	char	   *raw_area;

	raw_area = (char *) MapSuperCacheCtlData +
		MAXALIGN(sizeof(MapSuperCacheCtl));
	MapSuperCacheCtlData->raw_dsa_area = raw_area;

	dsa = dsa_create_in_place(raw_area, MAP_SUPER_DSA_INIT_SIZE,
							  LWTRANCHE_MAP_SUPER_MAPPING, NULL);
	dsa_pin(dsa);
	dsa_set_size_limit(dsa, MAP_SUPER_DSA_INIT_SIZE);
	hash = dshash_create(dsa, &MapSuperHashParams, NULL);
	MapSuperCacheCtlData->hash_handle = dshash_get_hash_table_handle(hash);
	dsa_set_size_limit(dsa, MAP_SUPER_DSA_MAX_SIZE);

	dshash_detach(hash);
	dsa_detach(dsa);
}

void
MapSuperTableShmemAttach(void)
{
	MapSuperAttach();
}

static void
MapSuperAttach(void)
{
	MemoryContext oldcontext;

	if (MapSuperHash != NULL)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	MapSuperDSA = dsa_attach_in_place(MapSuperCacheCtlData->raw_dsa_area, NULL);
	dsa_pin_mapping(MapSuperDSA);
	if (!IsUnderPostmaster)
		dsa_set_size_limit(MapSuperDSA, MAP_SUPER_DSA_INIT_SIZE);
	MapSuperHash = dshash_attach(MapSuperDSA, &MapSuperHashParams,
								MapSuperCacheCtlData->hash_handle, NULL);
	if (IsUnderPostmaster)
		/* Detach before dsm_backend_shutdown() tears down pinned mappings. */
		before_shmem_exit(MapSuperDetach, 0);
	MemoryContextSwitchTo(oldcontext);
}

static void
MapSuperDetach(int code, Datum arg)
{
	if (MapSuperHash != NULL)
	{
		dshash_detach(MapSuperHash);
		MapSuperHash = NULL;
	}

	if (MapSuperDSA != NULL)
	{
		dsa_detach(MapSuperDSA);
		dsa_release_in_place(MapSuperCacheCtlData->raw_dsa_area);
		MapSuperDSA = NULL;
	}
}
