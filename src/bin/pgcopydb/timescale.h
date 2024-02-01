/*
 * src/bin/pgcopydb/timescale.h
 *     Functions for interacting with a TimescaleDB catalog
 */

#ifndef TIMESCALE_H
#define TIMESCALE_H

#include "pgsql.h"

bool timescale_init(PGSQL *pgsql, char *pguri);

/*
 * Transforms if the given nspname and relname to hypertable it targets
 * the timescale chunk.
 */
bool timescale_chunk_to_hypertable(const char *nspname_in, const char *relname_in, char *nspname_out,
								   char *relname_out);

/*
 * Checks whether the given relation is a timescale chunk table.
 */
bool timescale_is_chunk(const char *nspname_in, const char *relname_in);

/*
 * Checks whether to ignore the given nspname and relname.
 */
bool timescale_allow_relation(const char *nspname_in, const char *relname_in);

#endif /* TIMESCALE_H */
