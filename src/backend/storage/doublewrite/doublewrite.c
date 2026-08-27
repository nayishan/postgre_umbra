/*-------------------------------------------------------------------------
 *
 * doublewrite.c
 *    Crash-safe shadow copies for ordinary relation page writes.
 *
 * A page is made durable in pg_doublewrite before its destination write is
 * issued.  Generations divide the shadow files at checkpoint boundaries:
 * files from before a completed checkpoint can be discarded, while writes
 * after the boundary remain available for the next crash recovery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "common/int.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
#include "storage/doublewrite.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#define DOUBLEWRITE_FILE_PREFIX "dw."
#define DOUBLEWRITE_MAGIC UINT32_C(0x44575231)
#define DOUBLEWRITE_VERSION 1

typedef struct DoubleWriteRecord
{
	uint32		magic;
	uint16		version;
	uint16		reserved;
	RelFileLocator rlocator;
	int16		forknum;
	uint16		reserved2;
	BlockNumber blocknum;
	XLogRecPtr	page_lsn;
	pg_crc32c	page_crc;
	pg_crc32c	metadata_crc;
	PGAlignedBlock page;
} DoubleWriteRecord;

typedef struct DoubleWriteSharedState
{
	LWLockPadded generation_lock;
	LWLockPadded append_lock;
	uint64		active_generation;
} DoubleWriteSharedState;

bool		enableDoubleWrite = false;

static DoubleWriteSharedState *DoubleWriteCtl;
static bool doublewrite_recovery_active = false;

static void DoubleWriteShmemRequest(void *arg);
static void DoubleWriteShmemInit(void *arg);
static void DoubleWriteEnsureDirectory(void);
static bool DoubleWriteDirectoryHasRecords(void);
static void DoubleWritePath(uint64 generation, char *path);
static bool DoubleWriteParseGeneration(const char *name, uint64 *generation);
static int DoubleWriteGenerationCompare(const void *left, const void *right);
static pg_crc32c DoubleWritePageCrc(const void *page);
static pg_crc32c DoubleWriteMetadataCrc(const DoubleWriteRecord *record);
static void DoubleWriteMakeRecord(DoubleWriteRecord *record,
								  SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum, const void *buffer);
static bool DoubleWriteRecordIsValid(const DoubleWriteRecord *record);
static void DoubleWriteAppend(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum, const void **buffers,
								  BlockNumber nblocks);
static uint64 DoubleWriteRecoverFiles(void);
static void DoubleWriteRecoverFile(const char *path);
static void DoubleWriteRestoreRecord(const DoubleWriteRecord *record);
static void DoubleWriteRemoveOldGenerations(bool remove_active_generation);
static void DoubleWriteRequirePrerequisites(int elevel);
static bool DoubleWriteOffsetAdd(pgoff_t base, Size amount,
							 pgoff_t *result);
static ssize_t DoubleWritePwrite(int fd, const void *buffer, Size amount,
								 pgoff_t offset);
static ssize_t DoubleWritePread(int fd, void *buffer, Size amount,
							pgoff_t offset);

const ShmemCallbacks DoubleWriteShmemCallbacks = {
	.request_fn = DoubleWriteShmemRequest,
	.init_fn = DoubleWriteShmemInit,
};

bool
check_double_write(bool *newval, void **extra, GucSource source)
{
	(void) extra;
	(void) source;

	if (*newval && !enableFsync)
	{
		GUC_check_errdetail("double_write requires fsync to be on.");
		return false;
	}

	return true;
}

bool
check_fsync_for_double_write(bool *newval, void **extra, GucSource source)
{
	(void) extra;
	(void) source;

	if (!*newval && enableDoubleWrite)
	{
		GUC_check_errdetail("fsync cannot be turned off while double_write is on.");
		return false;
	}

	return true;
}

static void
DoubleWriteShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "DoubleWrite State",
					   .size = sizeof(DoubleWriteSharedState),
					   .ptr = (void **) &DoubleWriteCtl,
		);
}

static void
DoubleWriteShmemInit(void *arg)
{
	int			tranche;

	tranche = LWLockNewTrancheId("DoubleWrite");
	LWLockInitialize(&DoubleWriteCtl->generation_lock.lock, tranche);
	LWLockInitialize(&DoubleWriteCtl->append_lock.lock, tranche);
	DoubleWriteCtl->active_generation = 0;
}

static void
DoubleWriteRequirePrerequisites(int elevel)
{
	if (!enableFsync)
		ereport(elevel,
				(errmsg("double_write requires fsync to be on")));
}

bool
DoubleWriteEnabled(void)
{
	return enableDoubleWrite && DoubleWriteCtl != NULL &&
		!doublewrite_recovery_active;
}

static void
DoubleWriteEnsureDirectory(void)
{
	struct stat st;

	if (stat(PG_DOUBLEWRITE_DIR, &st) == 0)
	{
		if (!S_ISDIR(st.st_mode))
			ereport(FATAL,
					(errmsg("double-write path \"%s\" is not a directory",
							PG_DOUBLEWRITE_DIR)));
		return;
	}
	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat double-write directory \"%s\": %m",
						PG_DOUBLEWRITE_DIR)));

	if (MakePGDirectory(PG_DOUBLEWRITE_DIR) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create double-write directory \"%s\": %m",
						PG_DOUBLEWRITE_DIR)));

	/* Persist both the new directory and its entry in PGDATA. */
	fsync_fname(PG_DOUBLEWRITE_DIR, true);
	fsync_fname(".", true);
}

static bool
DoubleWriteDirectoryHasRecords(void)
{
	DIR		   *dir;
	struct dirent *de;
	bool		has_records = false;

	dir = AllocateDir(PG_DOUBLEWRITE_DIR);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return false;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open double-write directory \"%s\": %m",
						PG_DOUBLEWRITE_DIR)));
	}

	while ((de = ReadDir(dir, PG_DOUBLEWRITE_DIR)) != NULL)
	{
		uint64		generation;

		if (DoubleWriteParseGeneration(de->d_name, &generation))
		{
			char		path[MAXPGPATH];
			struct stat st;

			DoubleWritePath(generation, path);
			if (stat(path, &st) == 0)
			{
				if (!S_ISREG(st.st_mode))
					ereport(FATAL,
							(errmsg("double-write path \"%s\" is not a regular file",
									path)));
				if (st.st_size > 0)
				{
					has_records = true;
					break;
				}
			}
			else if (errno != ENOENT)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not stat double-write file \"%s\": %m",
								path)));
		}
	}

	FreeDir(dir);
	return has_records;
}

static void
DoubleWritePath(uint64 generation, char *path)
{
	if (snprintf(path, MAXPGPATH, PG_DOUBLEWRITE_DIR "/" DOUBLEWRITE_FILE_PREFIX
				 UINT64_FORMAT, generation) >= MAXPGPATH)
		elog(ERROR, "double-write path is too long");
}

static bool
DoubleWriteParseGeneration(const char *name, uint64 *generation)
{
	const char *number;
	char	   *endptr;
	unsigned long long parsed;

	if (strncmp(name, DOUBLEWRITE_FILE_PREFIX,
				strlen(DOUBLEWRITE_FILE_PREFIX)) != 0)
		return false;

	number = name + strlen(DOUBLEWRITE_FILE_PREFIX);
	if (*number == '\0' || *number == '-')
		return false;
	if (number[0] == '0' && number[1] != '\0')
		return false;
	for (const char *p = number; *p != '\0'; p++)
	{
		if (!isdigit((unsigned char) *p))
			return false;
	}

	errno = 0;
	parsed = strtoull(number, &endptr, 10);
	if (errno != 0 || *endptr != '\0' || (uint64) parsed != parsed)
		return false;

	*generation = (uint64) parsed;
	return true;
}

static int
DoubleWriteGenerationCompare(const void *left, const void *right)
{
	uint64		generation_left = *((const uint64 *) left);
	uint64		generation_right = *((const uint64 *) right);

	if (generation_left < generation_right)
		return -1;
	if (generation_left > generation_right)
		return 1;
	return 0;
}

static pg_crc32c
DoubleWritePageCrc(const void *page)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page, BLCKSZ);
	FIN_CRC32C(crc);
	return crc;
}

/* The low-level pread/pwrite interfaces may legally transfer short buffers. */
static ssize_t
DoubleWritePwrite(int fd, const void *buffer, Size amount, pgoff_t offset)
{
	Size		transferred = 0;

	while (transferred < amount)
	{
		ssize_t		written;
		pgoff_t		io_offset;

		if (!DoubleWriteOffsetAdd(offset, transferred, &io_offset))
		{
			errno = EFBIG;
			return -1;
		}

		written = pg_pwrite(fd, (const char *) buffer + transferred,
							amount - transferred, io_offset);
		if (written < 0)
		{
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (written == 0)
		{
			errno = ENOSPC;
			return -1;
		}
		transferred += written;
	}

	return transferred;
}

static ssize_t
DoubleWritePread(int fd, void *buffer, Size amount, pgoff_t offset)
{
	Size		transferred = 0;

	while (transferred < amount)
	{
		ssize_t		read;
		pgoff_t		io_offset;

		if (!DoubleWriteOffsetAdd(offset, transferred, &io_offset))
		{
			errno = EFBIG;
			return -1;
		}

		read = pg_pread(fd, (char *) buffer + transferred,
						amount - transferred, io_offset);
		if (read < 0)
		{
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (read == 0)
			return transferred;
		transferred += read;
	}

	return transferred;
}

static pg_crc32c
DoubleWriteMetadataCrc(const DoubleWriteRecord *record)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, record, offsetof(DoubleWriteRecord, metadata_crc));
	FIN_CRC32C(crc);
	return crc;
}

/* Keep all file offsets representable on platforms with a narrower off_t. */
static bool
DoubleWriteOffsetAdd(pgoff_t base, Size amount, pgoff_t *result)
{
	int64		base64;
	int64		result64;

	base64 = (int64) base;
	if (base64 < 0 ||
		pg_add_s64_overflow(base64, (int64) amount, &result64) ||
		(pgoff_t) result64 != result64)
		return false;

	*result = (pgoff_t) result64;
	return true;
}

static void
DoubleWriteMakeRecord(DoubleWriteRecord *record, SMgrRelation reln,
						  ForkNumber forknum, BlockNumber blocknum,
						  const void *buffer)
{
	if (buffer == NULL)
		elog(ERROR, "cannot double-write a null page buffer");

	MemSet(record, 0, sizeof(*record));
	record->magic = DOUBLEWRITE_MAGIC;
	record->version = DOUBLEWRITE_VERSION;
	record->rlocator = reln->smgr_rlocator.locator;
	record->forknum = (int16) forknum;
	record->blocknum = blocknum;
	record->page_lsn = PageGetLSN((const PageData *) buffer);
	memcpy(record->page.data, buffer, BLCKSZ);
	record->page_crc = DoubleWritePageCrc(&record->page);
	record->metadata_crc = DoubleWriteMetadataCrc(record);
}

static bool
DoubleWriteRecordIsValid(const DoubleWriteRecord *record)
{
	if (record->magic != DOUBLEWRITE_MAGIC ||
		record->version != DOUBLEWRITE_VERSION ||
		record->reserved != 0 ||
		record->reserved2 != 0 ||
		!RelFileNumberIsValid(record->rlocator.relNumber) ||
		record->forknum < MAIN_FORKNUM || record->forknum > MAX_FORKNUM ||
		record->blocknum == InvalidBlockNumber)
		return false;
	if (!EQ_CRC32C(record->page_crc, DoubleWritePageCrc(&record->page)) ||
		!EQ_CRC32C(record->metadata_crc, DoubleWriteMetadataCrc(record)))
		return false;
	if (record->page_lsn != PageGetLSN(record->page.data))
		return false;
	/* The page CRC protects the shadow copy even if its data checksum is stale. */
	if (!PageIsVerified((PageData *) record->page.data, record->blocknum,
						PIV_IGNORE_CHECKSUM_FAILURE, NULL))
		return false;

	return true;
}

static void
DoubleWriteAppend(SMgrRelation reln, ForkNumber forknum,
				  BlockNumber blocknum, const void **buffers,
				  BlockNumber nblocks)
{
	char		path[MAXPGPATH];
	struct stat st;
	pgoff_t		offset;
	int			fd = -1;
	bool		created = false;
	uint64		generation;

	Assert(LWLockHeldByMeInMode(&DoubleWriteCtl->generation_lock.lock,
								LW_SHARED));

	LWLockAcquire(&DoubleWriteCtl->append_lock.lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		generation = DoubleWriteCtl->active_generation;
		if (generation == 0)
			elog(ERROR, "double-write state has not been initialized");
		DoubleWritePath(generation, path);

		fd = OpenTransientFile(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
		if (fd >= 0)
			created = true;
		else if (errno == EEXIST)
		{
			fd = OpenTransientFile(path, O_WRONLY | PG_BINARY);
			if (fd < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open double-write file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create double-write file \"%s\": %m", path)));

		if (fstat(fd, &st) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat double-write file \"%s\": %m", path)));
		if (!S_ISREG(st.st_mode) || st.st_size < 0)
			ereport(ERROR,
					(errmsg("double-write path \"%s\" is not a regular file", path)));
		if ((uint64) st.st_size > (uint64) PG_INT64_MAX)
			ereport(ERROR,
					(errmsg("double-write file \"%s\" is too large", path)));

		offset = (pgoff_t) st.st_size;
		if (offset % sizeof(DoubleWriteRecord) != 0)
		{
			offset -= offset % sizeof(DoubleWriteRecord);
			if (ftruncate(fd, offset) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate incomplete double-write record in \"%s\": %m",
								path)));
		}

		if (blocknum == InvalidBlockNumber ||
			(uint64) blocknum + (uint64) nblocks > (uint64) InvalidBlockNumber)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("double-write block range overflows")));

		for (BlockNumber i = 0; i < nblocks; i++)
		{
			DoubleWriteRecord record;
			pgoff_t		next_offset;

			DoubleWriteMakeRecord(&record, reln, forknum, blocknum + i,
								  buffers[i]);
			if (!DoubleWriteOffsetAdd(offset, sizeof(record), &next_offset))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("double-write file \"%s\" is too large", path)));
			if (DoubleWritePwrite(fd, &record, sizeof(record), offset) != sizeof(record))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write double-write file \"%s\": %m", path)));
			offset = next_offset;
		}

		if (pg_fsync(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync double-write file \"%s\": %m", path)));
		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close double-write file \"%s\": %m", path)));
		fd = -1;

		if (created)
			fsync_fname(PG_DOUBLEWRITE_DIR, true);
	}
	PG_CATCH();
	{
		if (fd >= 0)
			(void) CloseTransientFile(fd);
		LWLockRelease(&DoubleWriteCtl->append_lock.lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(&DoubleWriteCtl->append_lock.lock);
}

bool
DoubleWriteBeginWrite(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void **buffers,
					 BlockNumber nblocks)
{
	if (!DoubleWriteEnabled() || SmgrIsTemp(reln) || nblocks == 0)
		return false;

	DoubleWriteRequirePrerequisites(ERROR);
	LWLockAcquire(&DoubleWriteCtl->generation_lock.lock, LW_SHARED);
	PG_TRY();
	{
		DoubleWriteAppend(reln, forknum, blocknum, buffers, nblocks);
		INJECTION_POINT("doublewrite-after-fsync", NULL);
	}
	PG_CATCH();
	{
		LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return true;
}

void
DoubleWriteEndWrite(void)
{
	Assert(DoubleWriteCtl != NULL);
	Assert(LWLockHeldByMeInMode(&DoubleWriteCtl->generation_lock.lock,
								LW_SHARED));
	LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
}

static void
DoubleWriteRestoreRecord(const DoubleWriteRecord *record)
{
	SMgrRelation reln;
	ForkNumber	forknum = (ForkNumber) record->forknum;
	BlockNumber	nblocks;
	PGIOAlignedBlock target;
	PGIOAlignedBlock restore_page;
	bool		extend;
	bool		target_valid;
	bool		target_matches;
	bool		restore;

	reln = smgropen(record->rlocator, INVALID_PROC_NUMBER);
	if (!smgrexists(reln, forknum))
		return;

	nblocks = smgrnblocks(reln, forknum);
	if (nblocks == InvalidBlockNumber)
		return;
	/* smgrextend() fills any intervening blocks with zeroes. */
	extend = record->blocknum >= nblocks;

	if (extend)
		restore = true;
	else
	{
		smgrread(reln, forknum, record->blocknum, target.data);
		target_valid = PageIsVerified(target.data, record->blocknum, 0, NULL);
		target_matches = target_valid &&
			EQ_CRC32C(record->page_crc, DoubleWritePageCrc(target.data));
		if (!target_valid)
			restore = true;
		else if (target_matches)
			restore = false;
		else if (PageIsNew(target.data))
			restore = !PageIsNew(record->page.data);
		else if (PageIsNew(record->page.data))
			restore = false;
		else
			restore = PageGetLSN(target.data) <= record->page_lsn;
	}
	if (!restore)
		return;

	memcpy(restore_page.data, record->page.data, BLCKSZ);
	/* The source page may have been captured before its checksum was set. */
	PageSetChecksum((Page) restore_page.data, record->blocknum);
	if (extend)
		smgrextend(reln, forknum, record->blocknum, restore_page.data, false);
	else
		smgrwrite(reln, forknum, record->blocknum, restore_page.data, false);
	smgrimmedsync(reln, forknum);
	ereport(LOG,
			(errmsg("restored relation page from double-write file"),
			 errdetail("relation %u/%u/%u, fork %d, block %u",
					   record->rlocator.spcOid, record->rlocator.dbOid,
					   record->rlocator.relNumber, (int) forknum,
					   record->blocknum)));
}

static void
DoubleWriteRecoverFile(const char *path)
{
	struct stat st;
	pgoff_t		offset;
	int			fd = -1;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open double-write file \"%s\": %m", path)));

	PG_TRY();
	{
		pgoff_t		file_size;

		if (fstat(fd, &st) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat double-write file \"%s\": %m", path)));
		if (!S_ISREG(st.st_mode) || st.st_size < 0)
			ereport(ERROR,
					(errmsg("double-write path \"%s\" is not a regular file", path)));
		if ((uint64) st.st_size > (uint64) PG_INT64_MAX)
			ereport(ERROR,
					(errmsg("double-write file \"%s\" is too large", path)));
		file_size = (pgoff_t) st.st_size;

		for (offset = 0; offset < file_size; )
		{
			DoubleWriteRecord record;
			pgoff_t		next_offset;

			if (!DoubleWriteOffsetAdd(offset, sizeof(record), &next_offset) ||
				next_offset > file_size)
				break;

			if (DoubleWritePread(fd, &record, sizeof(record), offset) != sizeof(record))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read double-write file \"%s\": %m", path)));
			if (!DoubleWriteRecordIsValid(&record))
			{
				ereport(WARNING,
						(errmsg("ignoring invalid double-write record in \"%s\"", path)));
			}
			else
				DoubleWriteRestoreRecord(&record);
			offset = next_offset;
		}

		if (offset != file_size)
			ereport(LOG,
					(errmsg("ignoring incomplete double-write record at the end of \"%s\"",
							path)));
	}
	PG_CATCH();
	{
		if (fd >= 0)
			(void) CloseTransientFile(fd);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close double-write file \"%s\": %m", path)));
}

static uint64
DoubleWriteRecoverFiles(void)
{
	DIR		   *dir;
	struct dirent *de;
	uint64	   *generations = NULL;
	int			ngenerations = 0;
	int			max_generations = 0;
	uint64		max_generation;

	dir = AllocateDir(PG_DOUBLEWRITE_DIR);
	if (dir == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open double-write directory \"%s\": %m",
						PG_DOUBLEWRITE_DIR)));

	while ((de = ReadDir(dir, PG_DOUBLEWRITE_DIR)) != NULL)
	{
		uint64		generation;

		if (!DoubleWriteParseGeneration(de->d_name, &generation))
			continue;
		if (ngenerations == max_generations)
		{
			if (max_generations == 0)
				max_generations = 16;
			else if (max_generations > INT_MAX / 2)
				ereport(FATAL,
						(errmsg("too many double-write generation files")));
			else
				max_generations *= 2;
			if ((Size) max_generations > MaxAllocSize / sizeof(uint64))
				ereport(FATAL,
						(errmsg("too many double-write generation files")));
			if (generations == NULL)
				generations = palloc_array(uint64, max_generations);
			else
				generations = repalloc_array(generations, uint64,
										 max_generations);
		}
		generations[ngenerations++] = generation;
	}

	FreeDir(dir);
	if (ngenerations == 0)
	{
		if (generations != NULL)
			pfree(generations);
		return 0;
	}

	qsort(generations, ngenerations, sizeof(uint64),
		  DoubleWriteGenerationCompare);
	max_generation = generations[ngenerations - 1];
	for (int i = 0; i < ngenerations; i++)
	{
		char		path[MAXPGPATH];

		DoubleWritePath(generations[i], path);
		DoubleWriteRecoverFile(path);
		(void) durable_unlink(path, FATAL);
	}
	pfree(generations);

	if (max_generation == UINT64_MAX)
		ereport(FATAL,
				(errmsg("double-write generation counter has overflowed")));
	return max_generation;
}

void
DoubleWriteStartup(void)
{
	uint64		last_generation;

	if (!enableDoubleWrite)
	{
		if (DoubleWriteDirectoryHasRecords())
			ereport(FATAL,
					(errmsg("double-write recovery is required"),
					 errhint("restart with double_write enabled to recover or discard the pending double-write records.")));
		return;
	}

	DoubleWriteRequirePrerequisites(FATAL);
	DoubleWriteEnsureDirectory();
	LWLockAcquire(&DoubleWriteCtl->generation_lock.lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		doublewrite_recovery_active = true;
		last_generation = DoubleWriteRecoverFiles();
		doublewrite_recovery_active = false;
		if (last_generation == UINT64_MAX)
			elog(ERROR, "double-write generation counter has overflowed");
		DoubleWriteCtl->active_generation = last_generation + 1;
	}
	PG_CATCH();
	{
		doublewrite_recovery_active = false;
		LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
}

void
DoubleWriteCheckpointBegin(void)
{
	uint64		active_generation;

	if (!enableDoubleWrite)
		return;

	DoubleWriteRequirePrerequisites(ERROR);
	LWLockAcquire(&DoubleWriteCtl->generation_lock.lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		active_generation = DoubleWriteCtl->active_generation;
		if (active_generation == 0 || active_generation == UINT64_MAX)
			elog(ERROR, "double-write generation counter is invalid");
		DoubleWriteCtl->active_generation = active_generation + 1;
	}
	PG_CATCH();
	{
		LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
}

static void
DoubleWriteRemoveOldGenerations(bool remove_active_generation)
{
	DIR		   *dir;
	struct dirent *de;
	uint64		active_generation = DoubleWriteCtl->active_generation;

	dir = AllocateDir(PG_DOUBLEWRITE_DIR);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open double-write directory \"%s\": %m",
						PG_DOUBLEWRITE_DIR)));

	while ((de = ReadDir(dir, PG_DOUBLEWRITE_DIR)) != NULL)
	{
		char		path[MAXPGPATH];
		uint64		generation;

		if (!DoubleWriteParseGeneration(de->d_name, &generation) ||
			(generation > active_generation) ||
			(generation == active_generation && !remove_active_generation))
			continue;

		DoubleWritePath(generation, path);
		(void) durable_unlink(path, LOG);
	}

	FreeDir(dir);
}

void
DoubleWriteCheckpointComplete(bool remove_active_generation)
{
	if (!enableDoubleWrite)
		return;

	LWLockAcquire(&DoubleWriteCtl->generation_lock.lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		DoubleWriteRemoveOldGenerations(remove_active_generation);
	}
	PG_CATCH();
	{
		LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(&DoubleWriteCtl->generation_lock.lock);
}
