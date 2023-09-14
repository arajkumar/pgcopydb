#include "timescale.h"
#include "log.h"
#include "string_utils.h"
#include "uthash.h"

typedef struct ChunkHypertableMap
{
	uint32_t hypertableID;

	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];

	UT_hash_handle hh;          /* makes this structure hashable */
} ChunkHypertableMap;


static ChunkHypertableMap *chunkHypertableMap = NULL;

/*
 * parseSentinel parses the result from a PostgreSQL query that fetches the
 * sentinel values for startpos, endpos, and apply.
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
				chunkMapEntry->hypertableID = atoi(columnValue);
			}
			else if (streq(columnName, "schema_name"))
			{
				strncpy(chunkMapEntry->nspname, columnValue, NAMEDATALEN);
			}
			else if (streq(columnName, "table_name"))
			{
				strncpy(chunkMapEntry->relname, columnValue, NAMEDATALEN);
			}
		}

		/* Insert the ChunkHypertableMap entry into the hashmap */
		HASH_ADD_INT(chunkHypertableMap, hypertableID, chunkMapEntry);
		log_info("Adding hypertable relation: %s.%s id: %d", chunkMapEntry->nspname,
				 chunkMapEntry->relname, chunkMapEntry->hypertableID);
	}
}


bool
timescale_init(PGSQL *pgsql, char *pguri)
{
	if (!pgsql_init(pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
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

		/* Extract the number using atoi */
		*hypertableID = atoi(prefixPosition);
		return true;
	}

	return false;
}


bool
timescale_chunk_to_hypertable(char *nspname_in, char *relname_in, char *nspname_out,
							  char *relname_out)
{
	uint32_t targetHypertableID;
	if (!extract_hypertable_id(relname_in, &targetHypertableID))
	{
		log_error("BUG: Failed to find hypertable id from %s.%s", nspname_in, relname_in);
		return false;
	}

	ChunkHypertableMap *foundMapEntry;
	HASH_FIND_INT(chunkHypertableMap, &targetHypertableID, foundMapEntry);
	if (!foundMapEntry)
	{
		log_error("Failed to find hypertable from map for %s.%s", nspname_in, relname_in);
		return false;
	}

	log_trace("Found mapping for chunk %s.%s => %s.%s", nspname_in, relname_in,
			  foundMapEntry->nspname, foundMapEntry->relname);

	strcpy(nspname_out, foundMapEntry->nspname);
	strcpy(relname_out, foundMapEntry->relname);
	return true;
}


bool
timescale_is_chunk(const char *nspname_in, const char *relname_in)
{
	/* Chunk will be always present in _timescaledb_internal schema */
	if (streq(nspname_in, "_timescaledb_internal"))
	{
		return true;
	}

	/* Chunk will always start with _hyper_ prefix */
	if (strstr(relname_in, "_hyper_"))
	{
		return true;
	}

	return false;
}


bool
timescale_allow_statement(const char *nspname_in, const char *relname_in)
{
	const char *denylist[][2] = {
		{ "_timescaledb_catalog", NULL },
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
