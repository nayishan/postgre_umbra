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
extern void MapResetAllTruncatePreloads(void);
extern BlockNumber MapForkPageIndexToMapBlkno(ForkNumber forknum,
											  BlockNumber fork_page_idx);
extern BlockNumber MapLblknoToMapBlkno(ForkNumber forknum, BlockNumber lblkno);
#endif							/* MAP_INTERNAL_H */
