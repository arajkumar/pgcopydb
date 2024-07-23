/*
 * src/bin/pgcopydb/timescale.h
 *     Functions for interacting with a TimescaleDB catalog
 */

#ifndef TIMESCALE_H
#define TIMESCALE_H

#include "ld_stream.h"

#include "pgsql.h"

bool timescale_init(PGSQL *pgsql, char *pguri);

/*
 * Transforms if the given nspname and relname to hypertable it targets
 * the timescale chunk.
 */
bool timescale_chunk_to_hypertable(const LogicalMessageRelation *chunk,
								   LogicalMessageRelation *result);

/*
 * Checks whether to ignore the given nspname and relname.
 */
bool timescale_allow_relation(const char *nspname_in, const char *relname_in);


bool timescale_is_hypertable_root(PGSQL *pgsql,
								  const char *nspname,
								  const char *relname,
								  bool *isRoot);

#endif /* TIMESCALE_H */
