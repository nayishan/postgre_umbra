/*-------------------------------------------------------------------------
 *
 * umbra.h
 *	  Umbra storage manager public interface declarations.
 *
 * This header declares the Umbra smgr callback surface used by smgr.c when
 * the build is configured with --with-umbra.
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
#include "storage/um_defs.h"

extern bool UmMetadataExists(SMgrRelation reln);
extern bool UmMetadataOpenOrCreate(SMgrRelation reln, bool isRedo, bool *created);
extern BlockNumber UmMetadataNblocks(SMgrRelation reln);
extern void UmMetadataRead(SMgrRelation reln, BlockNumber blkno, void *buffer);
extern void UmMetadataWrite(SMgrRelation reln, BlockNumber blkno,
							const void *buffer, bool skipFsync);
extern void UmMetadataExtend(SMgrRelation reln, BlockNumber blkno,
							 const void *buffer, bool skipFsync);
extern void UmMetadataImmediateSync(SMgrRelation reln);
extern void UmMetadataUnlink(RelFileLocatorBackend rlocator, bool isRedo);

extern void uminit(void);
extern void umopen(SMgrRelation reln);
extern void umclose(SMgrRelation reln, ForkNumber forknum);
extern void umdestroy(SMgrRelation reln);
extern void umcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
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
extern void umtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber old_blocks, BlockNumber nblocks);
extern void umimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void umregistersync(SMgrRelation reln, ForkNumber forknum);
extern int	umfd(SMgrRelation reln, ForkNumber forknum,
				 BlockNumber blocknum, uint32 *off);

#endif							/* UMBRA_H */
