/*-------------------------------------------------------------------------
 *
 * um_defs.h
 *	  Umbra low-level fork and metadata path definitions.
 *
 * This header contains storage-layout facts shared by Umbra submodules.
 *
 * src/include/storage/um_defs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UM_DEFS_H
#define UM_DEFS_H

#include <stdio.h>

#include "common/relpath.h"
#include "storage/relfilelocator.h"

/*
 * Umbra reserves an extra fork slot for relation-local metadata.  This lives
 * outside PostgreSQL's built-in fork numbering so ordinary smgr loops do not
 * try to process it implicitly.
 */
#define UMBRA_METADATA_FORKNUM	((ForkNumber) (INIT_FORKNUM + 1))
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
