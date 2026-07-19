/*-------------------------------------------------------------------------
 *
 * umbra.h
 *	  Umbra storage manager public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/storage/umbra.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMBRA_H
#define UMBRA_H

#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "storage/ummap.h"

/* Umbra storage manager functionality */
extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void uminitnewrelation(SMgrRelation reln, bool needs_wal);
extern void umsetgeneration(SMgrRelation reln, ForkNumber forknum,
								XLogRecPtr generation_lsn);
extern void umredocreate(SMgrRelation reln, ForkNumber forknum,
							 XLogRecPtr generation_lsn);
extern void umprepareredo(SMgrRelation reln, XLogRecPtr replay_lsn);
extern bool umredogenerationahead(SMgrRelation reln, XLogRecPtr replay_lsn);
extern void umcheckpoint(void);
extern void umflushdatabasetablespace(Oid dbid, Oid spcOid);
extern void uminvalidatedatabase(Oid dbid);
extern void uminvalidatedatabasetablespace(Oid dbid, Oid spcOid);
extern bool umexists(SMgrRelation reln, ForkNumber forknum);
extern void umunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
extern void umextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void umzeroextend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool umprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, int nblocks);
extern uint32 ummaxcombine(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum);
extern void umreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					void **buffers, BlockNumber nblocks);
extern void umstartreadv(PgAioHandle *ioh,
						 SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks);
extern void umwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					 const void **buffers, BlockNumber nblocks, bool skipFsync);
extern void umwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber umnblocks(SMgrRelation reln, ForkNumber forknum);
extern void umpretruncate(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber old_blocks, BlockNumber nblocks,
						  XLogRecPtr truncate_lsn);
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern int	umfd(SMgrRelation reln, ForkNumber forknum,
				 BlockNumber blocknum, uint32 *off);
extern void UmPrepareFirstbornLocator(RelFileLocator rlocator,
									  ForkNumber forknum, BlockNumber lblkno);
extern void UmPrepareFirstbornRangeLocator(RelFileLocator rlocator,
										 ForkNumber forknum, int nblocks,
										 const BlockNumber *lblknos);
extern bool UmLogMappingRange(RelFileLocator rlocator, ForkNumber forknum,
							  BlockNumber startblk, BlockNumber endblk,
							  BlockNumber anchor_lblkno);
extern void UmLogMappingRangeFinish(bool delay_started);
extern void UmMappingPublicationDone(void);
extern void UmPrepareReplayMappingRange(SMgrRelation reln, ForkNumber forknum,
									const UmbraMapRange *range, XLogRecPtr lsn);
extern void UmPublishReplayMappingRanges(void);
extern bool UmRedoDiscardingPrecreateRecords(SMgrRelation reln);
extern void UmCheckRecoveryDependencies(void);
extern void UmRecoveryEnd(void);

#endif							/* UMBRA_H */
