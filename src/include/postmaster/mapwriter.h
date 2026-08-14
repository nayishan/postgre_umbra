/*-------------------------------------------------------------------------
 *
 * mapwriter.h
 *    Exports for the Umbra MAP background writer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAPWRITER_H
#define MAPWRITER_H

extern PGDLLIMPORT int MapWriterDelay;
extern PGDLLIMPORT int MapWriterMaxPages;

extern void MapBackgroundWorkersRegister(void);
extern void MapWriterMain(Datum arg);

#endif							/* MAPWRITER_H */
