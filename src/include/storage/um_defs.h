/*-------------------------------------------------------------------------
 *
 * um_defs.h
 *	  Umbra-private physical layout definitions.
 *
 * These definitions describe only the extra file slot used by Umbra's
 * relation-local metadata.  They do not make it a PostgreSQL-visible fork.
 *
 *-------------------------------------------------------------------------
 */
#ifndef UM_DEFS_H
#define UM_DEFS_H

#include <stdio.h>

#include "common/relpath.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/* Kept below the selected smgr implementation, outside core ForkNumber use. */
#define UMBRA_METADATA_FORKNUM	((ForkNumber) (MAX_FORKNUM + 1))
#define UMBRA_NUM_FORKS			((int) UMBRA_METADATA_FORKNUM + 1)

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
