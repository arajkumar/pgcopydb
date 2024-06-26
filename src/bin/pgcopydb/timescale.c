#include "timescale.h"
#include "log.h"
#include "string_utils.h"
#include "file_utils.h"
#include "uthash.h"

typedef struct ChunkHypertableMap
{
	uint32_t hypertableID;

	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	UT_hash_handle hh;          /* makes this structure hashable */
} ChunkHypertableMap;


static bool tableExists(PGSQL *pgsql, const char *nspname,
						const char *relname,
						bool *exists);

static ChunkHypertableMap *chunkHypertableMap = NULL;

/*
 * parseHypertableDetails parses the result from a PostgreSQL query that
 * fetches the hypertable & chunk details.
 */
static void
parseHypertableDetails(void *ctx, PGresult *res)
{
	int numRows = PQntuples(res);
	int numCols = PQnfields(res);

	for (int i = 0; i < numRows; i++)
	{
		ChunkHypertableMap *chunkMapEntry = (ChunkHypertableMap *) malloc(
			sizeof(ChunkHypertableMap));
		if (chunkMapEntry == NULL)
		{
			log_error("Failed to allocate memory for ChunkHypertableMap");
			break; /* Exit the loop on memory allocation failure */
		}
		for (int j = 0; j < numCols; j++)
		{
			char *columnName = PQfname(res, j);
			char *columnValue = PQgetvalue(res, i, j);
			if (streq(columnName, "id"))
			{
				if (!stringToUInt32(columnValue, &chunkMapEntry->hypertableID))
				{
					log_error("Failed to parse hypertable id: %s", columnValue);
					continue; /* Skip this row */
				}
			}
			else if (streq(columnName, "schema_name"))
			{
				strlcpy(chunkMapEntry->nspname, columnValue, PG_NAMEDATALEN);
			}
			else if (streq(columnName, "table_name"))
			{
				strlcpy(chunkMapEntry->relname, columnValue, PG_NAMEDATALEN);
			}
		}

		/* Insert the ChunkHypertableMap entry into the hashmap */
		HASH_ADD_INT(chunkHypertableMap, hypertableID, chunkMapEntry);
		log_notice("Adding hypertable relation: %s.%s id: %d", chunkMapEntry->nspname,
				   chunkMapEntry->relname, chunkMapEntry->hypertableID);
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
		"SELECT id, schema_name, table_name FROM _timescaledb_catalog.hypertable";
	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   NULL, &parseHypertableDetails))
	{
		log_error("Failed to fetch pgcopydb.sentinel current values");
		return false;
	}
	return true;
}


static bool
extract_hypertable_id(const char *input, uint32_t *hypertableID)
{
	const char *prefix = "_hyper_";

	/* Find the position of the prefix in the input string */
	const char *prefixPosition = strstr(input, prefix);

	if (prefixPosition != NULL)
	{
		/* Move the pointer to the character after the prefix */
		prefixPosition += strlen(prefix);

		char hypertableIDStr[PG_NAMEDATALEN] = { 0 };

		/* Copy the hypertable id string into a temporary buffer */
		sformat(hypertableIDStr, PG_NAMEDATALEN, "%s", prefixPosition);

		/* Find the position of the first underscore after the hypertable id */
		char *endptr = strchr(hypertableIDStr, '_');

		/* Replace the underscore with a null character */
		*endptr = '\0';

		if (!stringToUInt32(hypertableIDStr, hypertableID))
		{
			log_error("Failed to parse hypertable id from %s", input);
			return false;
		}
		return true;
	}

	return false;
}


bool
timescale_chunk_to_hypertable(const char *nspname_in, const char *relname_in,
							  char *nspname_out,
							  char *relname_out)
{
	if (!isTimescale)
	{
		return true;
	}

	uint32_t targetHypertableID;

	if (!extract_hypertable_id(relname_in, &targetHypertableID))
	{
		log_error("BUG: Failed to find hypertable id from %s.%s", nspname_in, relname_in);
		return false;
	}

	ChunkHypertableMap *foundMapEntry;

	HASH_FIND_INT(chunkHypertableMap, &targetHypertableID, foundMapEntry);

	if (foundMapEntry == NULL)
	{
		log_error("Failed to find hypertable from map for %s.%s", nspname_in, relname_in);
		return false;
	}

	log_trace("Found mapping for chunk %s.%s => %s.%s", nspname_in, relname_in,
			  foundMapEntry->nspname, foundMapEntry->relname);

	strlcpy(nspname_out, foundMapEntry->nspname, PG_NAMEDATALEN);
	strlcpy(relname_out, foundMapEntry->relname, PG_NAMEDATALEN);
	return true;
}


bool
timescale_is_chunk(const char *nspname_in, const char *relname_in)
{
	if (!isTimescale)
	{
		return false;
	}

	/* Chunk will be always present in _timescaledb_internal schema */
	if (streq(nspname_in, "_timescaledb_internal") &&
		strstr(relname_in, "_hyper_"))
	{
		return true;
	}

	return false;
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
timescale_is_hypertable_root(PGSQL *pgsql,
							 const char *nspname,
							 const char *relname,
							 bool *isRoot)
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
		*isRoot = false;
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
		"     current_setting('timescaledb.restoring') = 'off' "
		"     and hypertable_schema=$1 "
		"     and hypertable_name=$2 "
		"   ) ";

	int paramCount = 2;
	const Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { 0 };

	paramValues[0] = nspname;
	paramValues[1] = relname;

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

	*isRoot = context.boolVal;

	return true;
}
