/*-------------------------------------------------------------------------
 *
 * ummap.c
 *	  Umbra-private relation metadata root.
 *
 * The _map file is an Umbra-owned physical file.  At this point in the
 * series it contains only a fixed disk root; no regular relation I/O consults
 * it yet.  Later patches add a resident root and MAP page interpretation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/pg_crc32c.h"
#include "storage/umfile.h"
#include "storage/ummap.h"
#include "storage/umbra.h"
#include "utils/memutils.h"

#define UMMAP_ROOT_MAGIC		0x554D4252U	/* "UMBR" */
#define UMMAP_ROOT_VERSION		1U
#define UMMAP_ROOT_FLAGS_V1	0x00000001U
#define UMMAP_ROOT_IMAGE_SIZE	64
#define UMMAP_ROOT_SECTOR_SIZE	512

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

StaticAssertDecl(sizeof(UmbraMapRootData) == UMMAP_ROOT_IMAGE_SIZE,
				 "Umbra MAP root payload size is wrong");
StaticAssertDecl(offsetof(UmbraMapRootData, crc) == 60,
				 "Umbra MAP root CRC offset is wrong");
StaticAssertDecl(BLCKSZ >= UMMAP_ROOT_SECTOR_SIZE,
				 "Umbra MAP root sector must fit in a block");

static void ummap_root_init(char *sector);
static void ummap_root_refresh_crc(UmbraMapRootData *root);
static bool ummap_root_is_valid(const UmbraMapRootData *root);
static void ummap_root_validate(UmbraFileContext *ctx);

bool
ummap_exists(UmbraFileContext *ctx)
{
	return umfile_exists(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_create(UmbraFileContext *ctx, bool isRedo)
{
	PGIOAlignedBlock page = {0};
	BlockNumber nblocks;

	Assert(ctx != NULL);
	umfile_create(ctx, UMBRA_METADATA_FORKNUM, isRedo);
	nblocks = umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM);

	/*
	 * MAIN CREATE redo deterministically reconstructs this bootstrap root.
	 * It must repair a zero-length or torn prior write before normal opens
	 * validate the format.
	 */
	if (isRedo || nblocks == 0)
	{
		ummap_root_init(page.data);
		if (nblocks == 0)
			umfile_extend(ctx, UMBRA_METADATA_FORKNUM, 0, page.data, false);
		else
			umfile_write_bytes(ctx, UMBRA_METADATA_FORKNUM, 0, page.data,
							   BLCKSZ, false);
	}

	if (!isRedo)
		ummap_root_validate(ctx);
}

void
ummap_validate_if_exists(UmbraFileContext *ctx)
{
	Assert(ctx != NULL);

	if (ummap_exists(ctx))
		ummap_root_validate(ctx);
}

void
ummap_immedsync_if_exists(UmbraFileContext *ctx)
{
	Assert(ctx != NULL);

	if (ummap_exists(ctx))
		umfile_immedsync(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_registersync_if_exists(UmbraFileContext *ctx)
{
	Assert(ctx != NULL);

	if (ummap_exists(ctx))
		umfile_registersync(ctx, UMBRA_METADATA_FORKNUM);
}

void
ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo)
{
	umfile_unlink(rlocator, UMBRA_METADATA_FORKNUM, isRedo);
}

static void
ummap_root_init(char *sector)
{
	UmbraMapRootData *root;

	Assert(sector != NULL);
	MemSet(sector, 0, UMMAP_ROOT_SECTOR_SIZE);
	root = (UmbraMapRootData *) sector;
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

	if (root->magic != UMMAP_ROOT_MAGIC ||
		root->version != UMMAP_ROOT_VERSION ||
		root->blcksz != BLCKSZ ||
		root->chunk_pages != UMBRA_CHUNK_PAIRED_PAGES ||
		root->flags != UMMAP_ROOT_FLAGS_V1 ||
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

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, root, offsetof(UmbraMapRootData, crc));
	FIN_CRC32C(crc);
	return crc == root->crc;
}

static void
ummap_root_validate(UmbraFileContext *ctx)
{
	char		sector[UMMAP_ROOT_SECTOR_SIZE];

	if (umfile_nblocks(ctx, UMBRA_METADATA_FORKNUM) < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Umbra metadata root has an invalid length")));

	umfile_read_bytes(ctx, UMBRA_METADATA_FORKNUM, 0, sector,
					  UMMAP_ROOT_SECTOR_SIZE);
	if (!pg_memory_is_all_zeros(sector + UMMAP_ROOT_IMAGE_SIZE,
							  UMMAP_ROOT_SECTOR_SIZE -
							  UMMAP_ROOT_IMAGE_SIZE) ||
		!ummap_root_is_valid((UmbraMapRootData *) sector))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Umbra metadata root is corrupted or incompatible")));
}
