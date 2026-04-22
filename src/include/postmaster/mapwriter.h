/*-------------------------------------------------------------------------
 *
 * mapwriter.h
 *	  Exports for Umbra map background workers.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/postmaster/mapwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPWRITER_H
#define MAPWRITER_H

extern PGDLLIMPORT int MapWriterDelay;
extern PGDLLIMPORT int MapWriterMaxPages;
extern PGDLLIMPORT int MapWriterPreallocMaxRelations;
extern PGDLLIMPORT double MapWriterLRUMultiplier;
extern PGDLLIMPORT int MapCompactorDelay;
extern PGDLLIMPORT int MapCompactorMaxRelations;
extern PGDLLIMPORT int MapCompactorBusyAllocThreshold;

extern void MapBackgroundWorkersRegister(void);
extern void MapWriterMain(Datum arg);
extern void MapCompactorMain(Datum arg);

#endif							/* MAPWRITER_H */
