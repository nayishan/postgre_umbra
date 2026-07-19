/*-------------------------------------------------------------------------
 *
 * mapcompactor.h
 *	  Exports for the Umbra MAP compactor.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/postmaster/mapcompactor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPCOMPACTOR_H
#define MAPCOMPACTOR_H

extern PGDLLIMPORT int MapCompactorDelay;
extern PGDLLIMPORT int MapCompactorMaxRelations;

extern void MapCompactorRegister(void);
extern void MapCompactorMain(Datum arg);

#endif							/* MAPCOMPACTOR_H */
