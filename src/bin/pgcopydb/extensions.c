/*
 * src/bin/pgcopydb/extensions.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "copydb.h"
#include "log.h"
#include "schema.h"
#include "signals.h"


static bool copydb_copy_ext_sequence(PGSQL *src, PGSQL *dst, char *qname);
static bool
prepareTableSpecsForExtension(CopyTableDataSpec *tableSpecs,
							  CopyDataSpec *copySpecs,
							  SourceExtensionConfig *config);

/*
 * copydb_start_extension_process an auxilliary process that copies the
 * extension configuration table data from the source database into the target
 * database.
 */
bool
copydb_start_extension_data_process(CopyDataSpec *specs)
{
	if (specs->skipExtensions)
	{
		return true;
	}

	/*
	 * Flush stdio channels just before fork, to avoid double-output problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a worker process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: extension data");

			bool createExtensions = false;

			if (!copydb_copy_extensions(specs, createExtensions))
			{
				log_error("Failed to copy extensions configuration tables, "
						  "see above for details");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			break;
		}
	}

	/* now we're done, and we want async behavior, do not wait */
	return true;
}

/*
 * copydb_copy_ext_sequence copies sequence values from the source extension
 * configuration table into the target extension.
 */
static bool
copydb_copy_ext_sequence(PGSQL *src, PGSQL *dst, char *qname)
{
	SourceSequence seq = { 0 };

	strlcpy(seq.qname, qname, sizeof(seq.qname));

	if (!schema_get_sequence_value(src, &seq))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_set_sequence_value(dst, &seq))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * prepareTableSpecsForExtension prepares the table specs for the extension
 * configuration table.
 */
static bool
prepareTableSpecsForExtension(CopyTableDataSpec *tableSpecs,
							  CopyDataSpec *copySpecs,
							  SourceExtensionConfig *config)
{

	SourceTable table = { 0 };

	strlcpy(table.nspname, config->nspname, sizeof(table.nspname));
	strlcpy(table.relname, config->relname, sizeof(table.relname));
	sformat(table.qname, sizeof(table.qname),
			"%s.%s",
			config->nspname, config->relname);
	table.oid = config->oid;

	if (!copydb_init_table_specs(tableSpecs, copySpecs, &table, 0))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}

/*
 * copydb_copy_extensions copies extensions from the source instance into the
 * target instance.
 */
bool
copydb_copy_extensions(CopyDataSpec *copySpecs, bool createExtensions)
{
	int errors = 0;
	PGSQL dst = { 0 };

	/* make sure that we have our own process local connection */
	TransactionSnapshot snapshot = { 0 };

	if (!copydb_copy_snapshot(copySpecs, &snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	/* swap the new instance in place of the previous one */
	copySpecs->sourceSnapshot = snapshot;

	/* connect to the source database and set snapshot */
	if (!copydb_set_snapshot(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	ExtensionReqs *reqs = copySpecs->extRequirements;
	SourceExtensionArray *extensionArray = &(copySpecs->catalog.extensionArray);

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < extensionArray->count; i++)
	{
		SourceExtension *ext = &(extensionArray->array[i]);

		if (copydb_skip_extension(copySpecs, ext->extname))
		{
			continue;
		}

		if (createExtensions)
		{
			PQExpBuffer sql = createPQExpBuffer();

			char *extname = ext->extname;
			ExtensionReqs *req = NULL;

			HASH_FIND(hh, reqs, extname, strlen(extname), req);

			appendPQExpBuffer(sql,
							  "create extension if not exists \"%s\" cascade",
							  ext->extname);

			if (req != NULL)
			{
				appendPQExpBuffer(sql, " version \"%s\"", req->version);

				log_notice("%s", sql->data);
			}

			if (PQExpBufferBroken(sql))
			{
				log_error("Failed to build CREATE EXTENSION sql buffer: "
						  "Out of Memory");
				(void) destroyPQExpBuffer(sql);
			}

			log_info("Creating extension \"%s\"", ext->extname);

			if (!pgsql_execute(&dst, sql->data))
			{
				log_error("Failed to create extension \"%s\"", ext->extname);
			}

			(void) destroyPQExpBuffer(sql);
		}

		/* do we have to take care of extensions config table? */
		if (ext->config.count > 0)
		{
			for (int i = 0; i < ext->config.count; i++)
			{
				SourceExtensionConfig *config = &(ext->config.array[i]);

				CopyTableDataSpec tableSpecs = { 0 };

				if (!prepareTableSpecsForExtension(&tableSpecs, copySpecs, config))
				{
					/* errors have already been logged */
					return false;
				}

				/*
				 * Skip tables that have been entirely done already on a previous run.
				 */
				bool isDone = false;

				if (!copydb_table_create_lockfile(copySpecs, &tableSpecs, &isDone))
				{
					/* errors have already been logged */
					return false;
				}

				if (isDone)
				{
					log_info("Skipping table %s (%u), already done on a previous run",
							 tableSpecs.sourceTable->qname,
							 tableSpecs.sourceTable->oid);
					continue;
				}

				log_info("COPY extension \"%s\" "
						 "configuration table \"%s\".\"%s\"",
						 ext->extname,
						 config->nspname,
						 config->relname);

				/* apply extcondition to the source table */
				char qname[PG_NAMEDATALEN_FQ] = { 0 };

				sformat(qname, sizeof(qname), "%s.%s",
						config->nspname,
						config->relname);

				PGSQL *src = &(copySpecs->sourceSnapshot.pgsql);
				switch (config->relkind)
				{
					case 'r':
					{
						char *sqlTemplate = "(SELECT * FROM %s %s)";

						size_t sqlLen =
							strlen(sqlTemplate) +
							strlen(qname) +
							strlen(config->condition) +
							1;

						char *sql = (char *) calloc(sqlLen, sizeof(char));

						sformat(sql, sqlLen, sqlTemplate, qname, config->condition);

						bool truncate = false;
						uint64_t bytesTransmitted = 0;

						if (!pg_copy(src, &dst, sql, qname, truncate, &bytesTransmitted))
						{
							/* errors have already been logged */
							return false;
						}
						break;
					}
					case 'S':
					{
						log_info("COPY extension \"%s\" "
								"configuration sequence %s",
								ext->extname,
								qname);

						if (!copydb_copy_ext_sequence(src,
									&dst,
									qname))
						{
							/* errors have already been logged */
							return false;
						}

						break;
					}
					default:
					{
						/*
						 * According to the PostgreSQL documentation, extension
						 * configuration tables can only be of type table or
						 * sequence.
						 * https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES
						 */
						log_error("Unexpected configuration type '%c' found "
								  "for extension \"%s\" configuration table %s",
								  (char) config->relkind,
								  ext->extname,
								  qname);
						return false;
					}
				}

				if (!copydb_mark_table_as_done(copySpecs, &tableSpecs))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}
	}

	(void) pgsql_finish(&dst);

	return errors == 0;
}


/*
 * copydb_parse_extensions_requirements parses the requirements.json file that
 * is provided to either
 *
 *   $ pgcopydb copy extensions --requirements req.json
 *   $ pgcopydb clone ... --requirements req.json
 *
 * A sample file can be obtained via the command:
 *
 *   $ pgcopydb list extensions --requirements --json
 */
bool
copydb_parse_extensions_requirements(CopyDataSpec *copySpecs, char *filename)
{
	JSON_Value *json = json_parse_file(filename);
	JSON_Value *schema =
		json_parse_string("[{\"name\":\"foo\",\"version\":\"1.2.3\"}]");

	if (json_validate(schema, json) != JSONSuccess)
	{
		log_error("Failed to parse extensions requirements JSON file \"%s\"",
				  filename);
		return false;
	}

	json_value_free(schema);

	JSON_Array *jsReqArray = json_value_get_array(json);
	size_t count = json_array_get_count(jsReqArray);

	ExtensionReqs *reqs = NULL;

	for (int i = 0; i < count; i++)
	{
		ExtensionReqs *req = (ExtensionReqs *) calloc(1, sizeof(ExtensionReqs));

		if (req == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		JSON_Object *jsObj = json_array_get_object(jsReqArray, i);
		const char *name = json_object_get_string(jsObj, "name");
		const char *version = json_object_get_string(jsObj, "version");

		size_t len = strlcpy(req->extname, name, sizeof(req->extname));
		strlcpy(req->version, version, sizeof(req->version));

		HASH_ADD(hh, reqs, extname, len, req);
	}

	copySpecs->extRequirements = reqs;

	return true;
}

/*
 * copydb_skip_extension checks if the extension should be skipped.
 * It returns true if the extension should be skipped, false otherwise.
 */
bool copydb_skip_extension(CopyDataSpec *copySpecs, char *extname)
{
	if (copySpecs->skipExtensions)
	{
		log_info("Skipping extension \"%s\"", extname);
		return true;
	}

	SourceFilterExtensionList *filterExtensions = &(copySpecs->filters.excludeExtensionList);
	for (int i = 0; i < filterExtensions->count; i++)
	{
		SourceFilterExtension *filter = &(filterExtensions->array[i]);

		if (streq(filter->extname, extname))
		{
			return true;
		}
	}

	return false;
}
