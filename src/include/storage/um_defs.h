/*-------------------------------------------------------------------------
 *
 * um_defs.h
 *    Umbra low-level fork and metadata path definitions.
 *
 * This header intentionally contains only storage-layout facts shared by
 * Umbra submodules. Higher-level MAP policy stays in umbra.h/umbra.c.
 *
 *-------------------------------------------------------------------------
 */

#ifndef UM_DEFS_H
#define UM_DEFS_H

#include <stdio.h>

#include "common/relpath.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/*
 * Umbra internal metadata fork numbering.
 *
 * The numeric value still matches the historical MAP slot, but the definition
 * lives here so low-level file/map code does not depend on umbra.h.
 */
#define UMBRA_METADATA_FORKNUM	((int) INIT_FORKNUM + 1)
#define UMBRA_FORK_SLOTS		(UMBRA_METADATA_FORKNUM + 1)

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
