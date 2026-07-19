/*-------------------------------------------------------------------------
 *
 * ummap.h
 *	  Umbra private map fork declarations.
 *
 * This header describes Umbra's relation-local private map fork container.
 * The MAP page and disk-root formats remain private to ummap.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/storage/ummap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMMAP_H
#define UMMAP_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

typedef struct UmbraFileContext UmbraFileContext;

typedef struct UmbraMapRange
{
	BlockNumber first_lblkno;
	BlockNumber first_pblkno;
	BlockNumber nblocks;
} UmbraMapRange;

/*
 * Umbra-private relation-local map fork.
 *
 * This is not a PostgreSQL-visible ForkNumber.  It is only a private slot and
 * path convention used below the Umbra smgr implementation.
 */
#define UMBRA_MAP_FORKNUM	((ForkNumber) (MAX_FORKNUM + 1))
#define UMBRA_NUM_FORKS		((int) UMBRA_MAP_FORKNUM + 1)

extern RelPathStr ummap_relpath(RelFileLocatorBackend rlocator);

extern bool ummap_tracks_fork(ForkNumber forknum);
extern bool ummap_exists(UmbraFileContext *ctx);
extern void ummap_create(UmbraFileContext *ctx,
						 RelFileLocatorBackend rlocator, bool isRedo);
extern void ummap_close(UmbraFileContext *ctx);
extern void ummap_immedsync_if_exists(UmbraFileContext *ctx);
extern void ummap_registersync_if_exists(UmbraFileContext *ctx);
extern void ummap_unlink(RelFileLocatorBackend rlocator, bool isRedo);

extern void ummap_root_read_frontiers(UmbraFileContext *ctx,
									  RelFileLocatorBackend rlocator,
									  ForkNumber forknum,
									  BlockNumber *logical_eof,
									  BlockNumber *physical_frontier);
extern void ummap_root_advance(UmbraFileContext *ctx,
								RelFileLocatorBackend rlocator,
								ForkNumber forknum, BlockNumber logical_end,
								BlockNumber physical_end,
								XLogRecPtr wal_flush_lsn, bool skipFsync);
extern void ummap_root_set_logical(UmbraFileContext *ctx,
									RelFileLocatorBackend rlocator,
									ForkNumber forknum,
									BlockNumber logical_eof,
									BlockNumber physical_floor,
									XLogRecPtr wal_flush_lsn,
									bool skipFsync);

extern BlockNumber ummap_nblocks(UmbraFileContext *ctx,
								  RelFileLocatorBackend rlocator,
								  ForkNumber forknum);
extern BlockNumber ummap_lookup_block(UmbraFileContext *ctx,
									  RelFileLocatorBackend rlocator,
									  ForkNumber forknum,
									  BlockNumber lblkno);
extern BlockNumber ummap_lookup_run(UmbraFileContext *ctx,
									RelFileLocatorBackend rlocator,
									ForkNumber forknum,
									BlockNumber lblkno,
									BlockNumber maxblocks,
									BlockNumber *pblkno);
extern BlockNumber ummap_lookup_write_run(UmbraFileContext *ctx,
									  RelFileLocatorBackend rlocator,
									  ForkNumber forknum,
									  BlockNumber lblkno,
									  BlockNumber maxblocks,
									  BlockNumber *pblkno);
extern BlockNumber ummap_identity_run_limit(ForkNumber forknum,
											BlockNumber lblkno,
											BlockNumber maxblocks,
											BlockNumber *pblkno);
extern void ummap_publish_mapping_run(UmbraFileContext *ctx,
									  RelFileLocatorBackend rlocator,
									  ForkNumber forknum,
									  BlockNumber first_lblkno,
									  BlockNumber first_pblkno,
									  BlockNumber nblocks,
									  XLogRecPtr wal_flush_lsn,
									  bool skipFsync);
extern void ummap_prepare_firstborn_range(UmbraFileContext *ctx,
										  RelFileLocatorBackend rlocator,
										  ForkNumber forknum,
										  BlockNumber first_lblkno,
										  BlockNumber first_pblkno,
										  BlockNumber nblocks,
										  BlockNumber anchor_lblkno,
										  bool physical_ready);
extern void ummap_prepare_wal_range(UmbraFileContext *ctx,
									RelFileLocatorBackend rlocator,
									ForkNumber forknum,
									BlockNumber first_lblkno,
										BlockNumber first_pblkno,
										BlockNumber nblocks,
										BlockNumber anchor_lblkno);
extern bool ummap_pending_range_for_block(RelFileLocator rlocator,
										ForkNumber forknum,
										BlockNumber lblkno,
										UmbraMapRange *range);
extern bool ummap_pending_range_for_target(RelFileLocatorBackend rlocator,
										 ForkNumber forknum,
										 BlockNumber lblkno,
										 UmbraMapRange *range,
										 bool *wal_ready);
extern void ummap_reset_wal_attachments(void);
extern void ummap_publish_attached_ranges(XLogRecPtr lsn);
extern void ummap_mark_range_physical(RelFileLocatorBackend rlocator,
										ForkNumber forknum,
										BlockNumber lblkno);
extern BlockNumber ummap_next_physical_block(UmbraFileContext *ctx,
											 RelFileLocatorBackend rlocator,
											 ForkNumber forknum);
extern void ummap_abort_pending_ranges(SubTransactionId subxid);
extern void ummap_reparent_pending_ranges(SubTransactionId mySubid,
										  SubTransactionId parentSubid);
extern bool ummap_has_pending_ranges(void);
extern bool ummap_has_root_updates(void);
extern void ummap_publish_ready_ranges(void);
extern void ummap_prepare_replay_range(UmbraFileContext *ctx,
									   RelFileLocatorBackend rlocator,
									   ForkNumber forknum,
									   const UmbraMapRange *range,
									   XLogRecPtr lsn,
									   bool physical_ready);
extern bool ummap_replay_range_for_extension(
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	BlockNumber first_lblkno, BlockNumber nblocks,
	UmbraMapRange *range, bool *physical_ready);
extern void ummap_mark_replay_range_physical(
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	const UmbraMapRange *range);
extern void ummap_publish_replay_ranges(void);
extern bool ummap_has_replay_ranges(void);
extern void ummap_prepare_recovery_scratch_range(
	UmbraFileContext *ctx, RelFileLocatorBackend rlocator,
	ForkNumber forknum, const UmbraMapRange *range);
extern void ummap_forget_recovery_scratch(
	RelFileLocatorBackend rlocator, ForkNumber forknum,
	BlockNumber min_lblkno);
extern void ummap_forget_recovery_scratch_database(Oid dbid, Oid spcOid);
extern bool ummap_has_recovery_scratch(void);
extern bool ummap_has_recovery_scratch_relation(
	RelFileLocatorBackend rlocator);
extern void ummap_recovery_end(void);
extern BlockNumber ummap_set_identity_block(UmbraFileContext *ctx,
											RelFileLocatorBackend rlocator,
											ForkNumber forknum,
											BlockNumber lblkno,
											bool skipFsync);
extern BlockNumber ummap_set_identity_run(UmbraFileContext *ctx,
										  RelFileLocatorBackend rlocator,
										  ForkNumber forknum,
										  BlockNumber lblkno,
										  BlockNumber maxblocks,
										  BlockNumber *pblkno,
										  bool skipFsync);
extern void ummap_truncate(UmbraFileContext *ctx,
						   RelFileLocatorBackend rlocator,
						   ForkNumber forknum, BlockNumber old_nblocks,
						   BlockNumber new_nblocks,
						   XLogRecPtr wal_flush_lsn, bool skipFsync);

#endif							/* UMMAP_H */
