/*-------------------------------------------------------------------------
 *
 * mapwriter.h
 *	  Exports for the Umbra MAP background writer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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

extern void MapBackgroundWorkersRegister(void);
extern void MapWriterMain(Datum arg);

#endif							/* MAPWRITER_H */
