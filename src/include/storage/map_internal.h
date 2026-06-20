/*-------------------------------------------------------------------------
 *
 * map_internal.h
 *	  Internal interfaces shared by split MAP implementation files.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAP_INTERNAL_H
#define MAP_INTERNAL_H

#include "storage/map.h"

extern void MapEnsurePrivateRefCount(void);
extern void MapCacheTableShmemRequest(void);
extern void MapCacheTableShmemInit(void);
extern void MapBufferUpdateStateBits(MapBufferDesc *buf, uint32 set_bits,
									 uint32 clear_bits);
extern void MapMarkBufferDirty(UmbraFileContext *map_ctx, MapBufferDesc *buf,
							   XLogRecPtr page_lsn);
extern bool MapStartBufferIO(MapBufferDesc *buf, uint32 required_bits);
extern void MapTerminateBufferIO(MapBufferDesc *buf, bool clear_dirty,
								 uint32 set_flag_bits);
extern void MapFlushBuffer(int slot_id);
extern void MapInflightCleanupOwned(void);
extern void MapInflightBackendInit(void);
extern void MapResetAllTruncatePreloads(void);
extern BlockNumber MapForkPageIndexToMapBlkno(ForkNumber forknum,
											  BlockNumber fork_page_idx);
extern BlockNumber MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno);
extern bool MapForkPreallocSettings(ForkNumber forknum, BlockNumber *soft_low,
									BlockNumber *hard_low,
									BlockNumber *batch_blocks);
extern bool MapReserveNextPblkno(UmbraFileContext *map_ctx, RelFileLocator rnode,
								 ForkNumber forknum, BlockNumber lblkno,
								 BlockNumber *new_pblkno, bool nowait);
extern bool MapTryReserveFreshPblkno(UmbraFileContext *map_ctx,
									 RelFileLocator rnode,
									 ForkNumber forknum,
									 BlockNumber lblkno,
									 BlockNumber *new_pblkno,
									 bool nowait);
extern bool MapMaybePreallocateFork(UmbraFileContext *map_ctx,
									RelFileLocator rnode,
									ForkNumber forknum,
									bool background_mode);
extern bool MapInflightTryClaim(UmbraFileContext *map_ctx,
								RelFileLocator rnode,
								ForkNumber forknum,
								BlockNumber lblkno);
extern void MapInflightFinishClaim(RelFileLocator rnode,
								   ForkNumber forknum,
								   BlockNumber lblkno,
								   BlockNumber pblkno);
extern bool MapInflightBitIsSet(RelFileLocator rnode,
								ForkNumber forknum,
								BlockNumber lblkno);

#endif							/* MAP_INTERNAL_H */
