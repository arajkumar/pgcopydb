#include "copydb.h"
#include "timescale.h"
#include "log.h"
#include "string_utils.h"
#include "file_utils.h"
#include "uthash.h"

typedef struct SourceHypertable_Lookup
{
	char chunkSchema[PG_NAMEDATALEN];
	char chunkTablePrefix[PG_NAMEDATALEN];
} SourceHypertable_Lookup;

typedef struct SourceHypertable
{
	char chunkSchema[PG_NAMEDATALEN];
	char chunkTablePrefix[PG_NAMEDATALEN];

	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	UT_hash_handle hh;          /* makes this structure hashable */
} SourceHypertable;


static bool tableExists(PGSQL *pgsql, const char *nspname,
						const char *relname,
						bool *exists);

static SourceHypertable *hypertableCache = NULL;

/*
 * parseHypertable parses the result from a PostgreSQL query that
 * fetches the hypertable & chunk details.
 */
static bool
parseHypertable(PGresult *result, int rowNumber, SourceHypertable *hypertable)
{
	int nspname = PQfnumber(result, "schema_name");
	int relname = PQfnumber(result, "table_name");
	int chunkSchmea = PQfnumber(result, "associated_schema_name");
	int chunkTablePrefix = PQfnumber(result, "associated_table_prefix");

	int errors = 0;

	int numCols = PQnfields(result);

	if (numCols != 4)
	{
		log_error("Expected 4 columns in hypertable query result, got %d", numCols);
		return false;
	}

	/* nspname */
	const char *value = PQgetvalue(result, rowNumber, nspname);
	int length = strlcpy(hypertable->nspname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* relname */
	value = PQgetvalue(result, rowNumber, relname);
	length = strlcpy(hypertable->relname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* associated_schema_name */
	value = PQgetvalue(result, rowNumber, chunkSchmea);
	length = strlcpy(hypertable->chunkSchema, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Chunk schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* associated_table_prefix */
	value = PQgetvalue(result, rowNumber, chunkTablePrefix);
	length = strlcpy(hypertable->chunkTablePrefix, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Chunk table prefix name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


static void
getHypertables(void *ctx, PGresult *res)
{
	int numRows = PQntuples(res);
	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < numRows && parsedOk; rowNumber++)
	{
		SourceHypertable *hypertable = (SourceHypertable *) calloc(1,
																   sizeof(SourceHypertable));

		if (hypertable == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);

			parsedOk = false;

			break; /* Exit the loop on memory allocation failure */
		}

		if (!parseHypertable(res, rowNumber, hypertable))
		{
			log_error("Failed to parse hypertable details");
			parsedOk = false;

			break; /* Exit the loop on parse failure */
		}

		log_info("Found hypertable %s.%s with chunk schema %s and prefix %s",
				 hypertable->nspname, hypertable->relname,
				 hypertable->chunkSchema, hypertable->chunkTablePrefix);

		/*
		 * Prepare keylen as per https://troydhanson.github.io/uthash/userguide.html#_compound_keys
		 */
		unsigned keylen = offsetof(SourceHypertable, chunkTablePrefix) + /* offset of last key field */
						  sizeof(hypertable->chunkTablePrefix) - /* size of last key field */
						  offsetof(SourceHypertable, chunkSchema); /* offset of first key field */
		/* Add the table to the GeneratedColumnsCache. */
		HASH_ADD(hh,
				 hypertableCache,
				 chunkSchema,
				 keylen,
				 hypertable);
	}
}


/*
 * tableExists checks that a role with the given table exists on the
 * Postgres server.
 */
static bool
tableExists(PGSQL *pgsql,
			const char *nspname,
			const char *relname,
			bool *exists)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };

	char *existsQuery =
		"select exists( "
		"         select 1 "
		"           from pg_class c "
		"                join pg_namespace n on n.oid = c.relnamespace "
		"          where format('%I', n.nspname) = $1 "
		"            and format('%I', c.relname) = $2"
		"       )";

	int paramCount = 2;
	const Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { 0 };

	paramValues[0] = nspname;
	paramValues[1] = relname;

	if (!pgsql_execute_with_params(pgsql, existsQuery,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to check if \"%s\".\"%s\" exists", nspname, relname);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to check if \"%s\".\"%s\" exists", nspname, relname);
		return false;
	}

	*exists = context.boolVal;

	return true;
}


static bool isTimescale = false;

bool
timescale_init(PGSQL *pgsql, char *pguri)
{
	if (!pgsql_init(pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!tableExists(pgsql, "_timescaledb_catalog", "hypertable", &isTimescale))
	{
		/* errors have already been logged */
		return false;
	}

	if (!isTimescale)
	{
		log_notice("Source database does not have the Timescale extension installed.");
		return true;
	}

	const char *sql =
		"SELECT "
		"	schema_name, table_name, "
		"	associated_schema_name, associated_table_prefix "
		"FROM "
		"	_timescaledb_catalog.hypertable";

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   NULL, &getHypertables))
	{
		log_error("Failed to fetch pgcopydb.sentinel current values");
		return false;
	}

	return true;
}


static bool
findChunkPrefixEndPos(const char *relname, int *prefixPosition)
{
	/*
	 * Format of the chunk table name: <prefix>_<chunk_id>_chunk.
	 * i.e. It always ends with "_chunk".
	 * Few examples of relname:
	 * 1. long_1234567890123456789012345678901234567890123_100_chunk
	 * 2. long_12345678901234567890123456_metrics_prefix_long_long_time_i
	 * custom_schema.metrics_97_chunk
	 * 3. _timescaledb_internal._hyper_2_67_chunk
	 *
	 * The logic should find the 2nd _ from the end of the string.
	 */

	/*
	 * Check whether it ends with "_chunk".
	 * If not, then it is not a chunk table.
	 */
	int position = strlen(relname) - 1;

	char suffix[] = "_chunk";
	int suffixLength = sizeof(suffix) - 1;

	if (position < suffixLength)
	{
		return false;
	}

	if (relname[position - 5] != '_' || relname[position - 4] != 'c' ||
		relname[position - 3] != 'h' || relname[position - 2] != 'u' ||
		relname[position - 1] != 'n' || relname[position] != 'k')
	{
		return false;
	}

	position -= suffixLength;

	/*
	 * Find the previous _ after "_chunk".
	 */
	while (position >= 0)
	{
		if (relname[position] == '_')
		{
			*prefixPosition = position;
			return true;
		}

		position--;
	}

	return false;
}


bool
timescale_chunk_to_hypertable(const LogicalMessageRelation *chunk,
							  LogicalMessageRelation *result)
{
	if (!isTimescale || hypertableCache == NULL)
	{
		return false;
	}

	int chunkPrefixPosition = 0;

	if (!findChunkPrefixEndPos(chunk->relname, &chunkPrefixPosition))
	{
		return false;
	}

	SourceHypertable_Lookup key = { 0 };

	strlcpy(key.chunkSchema, chunk->nspname, PG_NAMEDATALEN);
	strlcpy(key.chunkTablePrefix, chunk->relname, chunkPrefixPosition + 1);

	SourceHypertable *hypertable = NULL;

	HASH_FIND(hh,
			  hypertableCache,
			  &key,
			  sizeof(SourceHypertable_Lookup),
			  hypertable);

	if (hypertable == NULL)
	{
		log_trace("Failed to find hypertable for chunk \"%s\".\"%s\"",
				  key.chunkSchema, key.chunkTablePrefix);
		return false;
	}

	result->nspname = strdup(hypertable->nspname);

	if (result->nspname == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);

		return false;
	}

	result->relname = strdup(hypertable->relname);

	if (result->relname == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);

		return false;
	}

	log_trace("Found hypertable \"%s\".\"%s\" for chunk \"%s\".\"%s\"",
			  result->nspname, result->relname, chunk->nspname, chunk->relname);

	return true;
}


bool
timescale_allow_relation(const char *nspname_in, const char *relname_in)
{
	if (!isTimescale)
	{
		return true;
	}

	const char *denylist[][2] = {
		{ "_timescaledb_catalog", NULL },
		{ "_timescaledb_config", NULL },
		{ "_timescaledb_cache", NULL },
		{ "_timescaledb_internal", "bgw_job_stat" },
		{ "_timescaledb_internal", "compress_hyper_" },
		{ "_timescaledb_internal", "bgw_policy_chunk_stats" },
		{ "_timescaledb_internal", "job_errors" },
		{ "_timescaledb_internal", "bgw_job_stat_history" },
	};

	int denylist_size = sizeof(denylist) / sizeof(denylist[0]);

	for (int i = 0; i < denylist_size; i++)
	{
		const char *deny_nspname = denylist[i][0];
		const char *deny_relname = denylist[i][1];

		if (streq(nspname_in, deny_nspname))
		{
			if (deny_relname == NULL || strstr(relname_in, deny_relname) == relname_in)
			{
				return false; /* Found in denylist, so disallowed */
			}
		}
	}

	return true; /* Not found in denylist, allowed */
}


bool
copydb_is_hypertable(PGSQL *pgsql,
					 const char *nspname,
					 const char *relname,
					 bool restoring,
					 bool *isHypertable)
{
	bool exists = false;

	if (!tableExists(pgsql, "_timescaledb_catalog", "hypertable", &exists))
	{
		/* errors have already been logged */
		return false;
	}

	if (!exists)
	{
		/* The table does not exist, so it cannot be a hypertable root */
		*isHypertable = false;
		return true;
	}

	const char *sql =
		" select exists( "
		"   select 1 from timescaledb_information.hypertables"

		/*
		 * We need to check if the restoring mode is off, otherwise the
		 * hypertable is not considered a root hypertable.
		 *
		 * When restoring mode is on, the migration is for Postgres to
		 * TimescaleDB, and the hypertable is not yet fully migrated.
		 */
		"   where "
		"     hypertable_schema=$1 "
		"     and hypertable_name=$2 "
		"     and current_setting('timescaledb.restoring')::bool = $3"
		"   ) ";

	int paramCount = 3;
	const Oid paramTypes[3] = { TEXTOID, TEXTOID, BOOLOID };
	const char *paramValues[3] = { 0 };

	paramValues[0] = nspname;
	paramValues[1] = relname;
	paramValues[2] = restoring ? "on" : "off";

	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };

	if (!pgsql_execute_with_params(pgsql, sql, paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to check if \"%s\".\"%s\" is a hypertable root", nspname,
				  relname);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to check if \"%s\".\"%s\" is a hypertable root", nspname,
				  relname);
		return false;
	}

	*isHypertable = context.boolVal;

	return true;
}
