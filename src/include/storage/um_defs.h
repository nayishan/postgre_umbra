/*-------------------------------------------------------------------------
 *
 * um_defs.h
 *    Umbra-private selector-MAP file definitions.
 *
 *-------------------------------------------------------------------------
 */
#ifndef UM_DEFS_H
#define UM_DEFS_H

#include <stdio.h>

#include "common/relpath.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/* This is a private selector file, not a PostgreSQL-visible relation fork. */
#define UMBRA_METADATA_FORKNUM ((ForkNumber) (MAX_FORKNUM + 1))
#define UMBRA_NUM_FORKS ((int) UMBRA_METADATA_FORKNUM + 1)

static inline bool
UmbraForkNumberIsValid(ForkNumber forknum)
{
	return forknum >= MAIN_FORKNUM && forknum <= UMBRA_METADATA_FORKNUM;
}

static inline RelPathStr
UmMetadataRelPathBackend(RelFileLocatorBackend rlocator)
{
	RelPathStr	base;
	RelPathStr	path;

	base = relpath(rlocator, MAIN_FORKNUM);
	snprintf(path.str, sizeof(path.str), "%s_map", base.str);
	return path;
}

static inline RelPathStr
UmMetadataRelPathPerm(RelFileLocator rlocator)
{
	RelFileLocatorBackend backend_rlocator;

	backend_rlocator.locator = rlocator;
	backend_rlocator.backend = INVALID_PROC_NUMBER;
	return UmMetadataRelPathBackend(backend_rlocator);
}

#endif							/* UM_DEFS_H */
