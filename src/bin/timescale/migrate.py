#!/usr/bin/env python3
# This script orchestrates CDC based migration process using pgcopydb.

import csv
import os
import signal
import sys
import threading
import logging
import traceback

from multiprocessing import Pool
from pathlib import Path
from collections import defaultdict

from housekeeping import start_housekeeping
from health_check import health_checker
from utils import timeit, docker_command, dbname_from_uri, store_val, \
    get_stored_val, bytes_to_human, seconds_to_human, DBType, get_dbtype
from environ import LIVE_MIGRATION_DOCKER, env
from telemetry import telemetry_command, telemetry
from usr_signal import wait_for_event, IS_TTY
from exec import Command, run_cmd, run_sql, psql, print_logs_with_error, LogFile
from filter import Filter
from psql import psql as psql_cmd

logger = logging.getLogger(__name__)

REPLICATION_LAG_THRESHOLD_BYTES = 512000  # 500KiB.

def is_snapshot_valid():
    try:
        run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="""
                     BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                     SET TRANSACTION SNAPSHOT '$(cat $PGCOPYDB_DIR/snapshot)';
                     SELECT 1;
                     ROLLBACK;
                     """))
        return True
    except Exception:
        return False

def is_section_migration_complete(section):
    return os.path.exists(f"{env['PGCOPYDB_DIR']}/run/{section}-migration.done")

def mark_section_complete(section):
    Path(f"{env['PGCOPYDB_DIR']}/run/{section}-migration.done").touch()

def convert_matview_to_view(conn):
    query = """
BEGIN;

CREATE SCHEMA IF NOT EXISTS __live_migration;
CREATE OR REPLACE FUNCTION __live_migration.skip_dml_function() RETURNS TRIGGER AS $$
BEGIN
    -- Do nothing and return NULL to skip the DML operation
    RAISE INFO 'live-migration: Skipping % DML in MATERIALIZED VIEW %.%', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    mv RECORD;
    populated BOOLEAN;
BEGIN
    -- Create the audit table if it does not exist
    CREATE TABLE IF NOT EXISTS __live_migration.matview_audit (
        schemaname TEXT,
        matviewname TEXT,
        renamed BOOLEAN DEFAULT FALSE
    );

    SELECT COUNT(*) > 0 INTO populated FROM __live_migration.matview_audit;

    -- Populate the audit table with materialized views if not already populated
    IF populated = FALSE THEN
        INSERT INTO __live_migration.matview_audit (schemaname, matviewname)
        SELECT schemaname, matviewname
        FROM pg_matviews;
    END IF;

    -- Loop through all materialized views in the current schema that have not been renamed
    FOR mv IN
        SELECT schemaname, matviewname
        FROM __live_migration.matview_audit
        WHERE NOT renamed
    LOOP
        -- Rename the materialized view with a live_migration_ prefix
        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I RENAME TO %I', mv.schemaname, mv.matviewname, 'live_migration_' || mv.matviewname);

        -- Update the audit table to mark the view as renamed
        EXECUTE format('UPDATE __live_migration.matview_audit SET renamed = true WHERE schemaname = %L AND matviewname = %L', mv.schemaname, mv.matviewname);

        -- Create a view to replace the materialized view
        EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I', mv.schemaname, mv.matviewname, mv.schemaname, 'live_migration_' || mv.matviewname);
        EXECUTE format('CREATE TRIGGER skip_dml_trigger
                        INSTEAD OF INSERT OR UPDATE OR DELETE ON %I.%I
                        FOR EACH ROW EXECUTE FUNCTION __live_migration.skip_dml_function()', mv.schemaname, mv.matviewname);
    END LOOP;
END $$;

COMMIT;
"""
    psql_cmd(conn, query)
    return True

def restore_matview(conn):
    query = """
BEGIN;

DO $$
DECLARE
    mv RECORD;
BEGIN
    -- Loop through all materialized views in the current schema that have not been renamed
    FOR mv IN
        SELECT schemaname, matviewname
        FROM __live_migration.matview_audit
        WHERE renamed
    LOOP
        -- Drop the view if it exists
        EXECUTE format('DROP VIEW IF EXISTS %I.%I', mv.schemaname, mv.matviewname);

        -- Rename the materialized view with a live_migration_ prefix
        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I RENAME TO %I', mv.schemaname, 'live_migration_' || mv.matviewname, mv.matviewname);

        -- Update the audit table to mark the view as renamed
        EXECUTE format('UPDATE __live_migration.matview_audit SET renamed = false WHERE schemaname = %L AND matviewname = %L', mv.schemaname, mv.matviewname);

    END LOOP;
END $$;

COMMIT;
"""
    psql_cmd(conn, query)
    return True

@telemetry_command("create_follow")
def create_follow(resume: bool = False):
    logger.info(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}...")

    follow_command = "pgcopydb follow --dir $PGCOPYDB_DIR"
    if resume:
        # CDC with test_decoding uses a snapshot to retrieve catalog tables.
        # While resuming, we can't guarantee the availability of the initial
        # snapshot, but using --not-consistent to create a
        # temporary snapshot is acceptable, as it's only used for catalog access.
        follow_command += " --resume --not-consistent"

    log_file = LogFile("pgcopydb_follow")
    follow_proc = Command(command=follow_command, use_shell=True, log_file=log_file)
    def is_error_func_for_follow(log_line: str):
        if "pgcopydb.sentinel" in log_line:
            return False
        if "ERROR" in log_line or \
            "free(): double free detected" in log_line or \
            "no tuple identifier for" in log_line:
            return True
        return False
    health_checker.check_log_for_health("follow", log_file.stderr, is_error_func_for_follow)

    return follow_proc


def show_hypertable_creation_prompt():
    create_hypertable_message = """Now, let's transform your regular tables into hypertables.

Execute the following command for each table you want to convert on the target DB:
`SELECT create_hypertable('<table name>', '<time column name>', chunk_time_interval => INTERVAL '<chunk interval>');`

Refer to https://docs.timescale.com/use-timescale/latest/hypertables/create/#create-hypertables for more details.

Once you are done"""
    event = wait_for_event("c")
    if not IS_TTY and LIVE_MIGRATION_DOCKER:
        print(f"{create_hypertable_message}, send a SIGUSR1 signal with 'docker kill --s=SIGUSR1 <container_name>' to continue")
    elif not IS_TTY:
        print(f"{create_hypertable_message}, send a SIGUSR1 signal with 'kill -s=SIGUSR1 {os.getpid()}' to continue")
    else:
        print(f"{create_hypertable_message}, press 'c' (and ENTER) to continue")
    event.wait()


def monitor_db_sizes() -> threading.Event:
    DB_SIZE_SQL = "select pg_size_pretty(pg_database_size(current_database()))"
    src_size = get_stored_val("src_existing_data_size")
    if src_size is None:
        src_size = run_sql(execute_on_target=False, sql=DB_SIZE_SQL)[:-1]
        # We save the size of the source database for reuse when running `migrate` with the
        # --resume option. Without this step, the source size would need to be recalculated
        # during the `--resume` phase, leading to inaccurate progress.
        store_val("src_existing_data_size", src_size)

    stop_event = threading.Event()
    logger.info("Monitoring initial copy progress ...")
    def get_and_print_size():
        while not stop_event.is_set():
            tgt_size = run_sql(execute_on_target=True, sql=DB_SIZE_SQL)[:-1]
            logger.info(f"{tgt_size} copied to Target DB (Source DB is {src_size})")
            stop_event.wait(timeout=60)
    t = threading.Thread(target=get_and_print_size)
    t.daemon = True
    t.start()
    return stop_event


def skip_extensions_list(args):
    # Default known list of extensions that should be skipped
    skip_extensions_default = [
            "aiven_extras",
    ]

    if args.skip_extensions:
        return skip_extensions_default + args.skip_extensions
    else:
        return skip_extensions_default


def get_hypertable_dimensions(pguri) -> csv.DictReader:
    sql = """
select
    hypertable_schema as nspname,
    hypertable_name as relname,
    array_agg(FORMAT('%I', column_name::text)) as dimensions
from timescaledb_information.dimensions group by 1, 2;
 """
    dimensions = psql_cmd(conn=pguri,
                           sql=sql)
    return dimensions

def get_hypertable_incompatible_objects(pguri, hypertables: csv.DictReader,
                                        exclude_indexes) -> defaultdict[str, list[csv.DictReader]]:
    """
    Get list of indexes/constraints that are incompatible with hypertables.

    :param pguri: URI of the source database
    :param hypertables: List of hypertables
    :param exclude_indexes: List of indexes to exclude
    :return: Dictionary where key is the table name and value is the list of incompatible indexes/constraints
    """

    if exclude_indexes:
        index_names = map(lambda row: f"('{row['fq_index_name']}')", exclude_indexes)
        index_names = ", ".join(index_names)
    else:
        index_names = "(NULL)"

    index_filter_sql = f"""
index_filter(faq_indexname) as (
        VALUES {index_names}
)
"""
    if hypertables:
        hypertable_names = map(lambda row: f"""
(
    '{row['nspname']}', '{row['relname']}', '{row['dimensions']}'
)""", hypertables)
        hypertable_names = ", ".join(hypertable_names)
    else:
        hypertable_names = "(NULL, NULL, NULL)"
    hypertable_filter_sql = f"""
hypertable_filter(nspname, relname, dimensions) as (
        VALUES {hypertable_names}
)
"""

    sql = f"""
WITH {index_filter_sql}, {hypertable_filter_sql}, indexes AS (
    SELECT
        ns.nspname AS nspname,
        cls.relname AS relname,
        idxns.nspname AS idxnspname,
        idxcls.relname AS idxrelname,
        am.amname AS index_type,
        array_agg(FORMAT('%I', att.attname::text) ORDER BY array_position(idx.indkey, att.attnum)) AS columns,
        pg_get_indexdef(idx.indexrelid) AS index_definition,
        con.conname AS constraint_name,
        CASE WHEN con.oid IS NOT NULL THEN pg_get_constraintdef(con.oid) ELSE NULL END AS constraint_definition,
        idx.indisprimary as is_primary_key
    FROM
        pg_index idx
    JOIN
        pg_class cls ON cls.oid = idx.indrelid
    JOIN
        pg_namespace ns ON ns.oid = cls.relnamespace
    JOIN
        pg_class idxcls ON idxcls.oid = idx.indexrelid
    JOIN
        pg_namespace idxns ON idxns.oid = idxcls.relnamespace
    JOIN
        pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = ANY(idx.indkey)
    JOIN
        pg_am am ON idxcls.relam = am.oid
    LEFT JOIN
        pg_constraint con ON con.conindid = idx.indexrelid
    WHERE
        (idx.indisunique OR idx.indisprimary)
    GROUP BY
        ns.nspname, cls.relname, idxns.nspname, idxcls.relname, am.amname, idx.indisunique, idx.indexrelid, con.conname, con.oid
    ORDER BY
        ns.nspname, cls.relname, idxns.nspname, idxcls.relname
)
SELECT
    i.nspname,
    i.relname,
    i.idxnspname,
    i.idxrelname,
    FORMAT('%I.%I', i.nspname, i.relname) as fq_table_name,
    FORMAT('%I.%I', i.idxnspname, i.idxrelname) as fq_index_name,
    columns,
    index_definition,
    CASE WHEN constraint_name IS NOT NULL THEN FORMAT('%I', constraint_name) ELSE NULL END as constraint_name,
    constraint_definition,
    is_primary_key,
    i.columns @> hf.dimensions::text[] as is_compatible
FROM indexes i
JOIN hypertable_filter hf ON hf.nspname = i.nspname AND hf.relname = i.relname
LEFT JOIN index_filter idxf ON idxf.faq_indexname = FORMAT('%s.%s', i.idxnspname, i.idxrelname)
WHERE idxf.faq_indexname IS NULL
"""
    result = psql_cmd(conn=pguri, sql=sql)

    group_dimension = {}
    for row in hypertables:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        group_dimension[fq_table_name] = row['dimensions']

    group_by_table = defaultdict(list)
    for row in result:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        row['dimensions'] = group_dimension[fq_table_name]
        group_by_table[fq_table_name].append(row)

    return group_by_table


# This could be a innter function, but it can't be pickled for multiprocessing.
def _create_index(index, args):

    if args.skip_hypertable_incompatible_objects and index['is_compatible'] == 'f':
        logger.warn(f"Skipping incompatible index {index['fq_index_name']}")
        return True

    create_index = index['index_definition']

    sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{index['idxrelname']}' AND schemaname = '{index['idxnspname']}') THEN
            EXECUTE '{create_index}';
        END IF;
    END $$;
    """
    logger.info(f"Creating index {create_index}")
    psql_cmd(conn=args.target, sql=sql)
    return True


# This could be a innter function, but it can't be pickled for multiprocessing.
def _filter_constraint(table_name, args, hypertable_info):
    objs = hypertable_info[table_name]
    objs = filter(lambda i: i['constraint_name'], objs)
    for constraint in objs:
        if args.skip_hypertable_incompatible_objects and constraint['is_compatible'] == 'f':
            logger.warn(f"Skipping incompatible constraint {constraint['constraint_name']}")
            continue

        # Unlike indexes, constraints can't be created with IF NOT EXISTS.
        # We need to check if the constraint already exists before creating it.
        constraint_definition = f"ALTER TABLE {constraint['fq_table_name']} ADD CONSTRAINT {constraint['constraint_name']} {constraint['constraint_definition']}"
        sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON(c.connamespace=n.oid) WHERE conname = '{constraint['constraint_name']}' AND nspname = '{constraint['nspname']}') THEN
                EXECUTE '{constraint_definition}';
            END IF;
        END $$;
        """
        logger.info(f"Creating constraint {constraint_definition} if not exist...")
        psql_cmd(conn=args.target, sql=sql)
    return True


def try_creating_incompatible_objects(args, hypertable_info):
    logger.info("Creating indexes and constraints on the target hypertable ...")

    # First build all indexes in parallel followed by constraints. Otherwise,
    # constraint building will block the index building as it takes table lock.

    # Building indexes in parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        # Prepare the list of indexes to create, excluding constraints.
        indexes = []
        for table, info in hypertable_info.items():
            for obj in info:
                if not obj['constraint_name']:
                    indexes.append((obj, args))
        pool.starmap(_create_index, indexes)

    # Building constraints sequentially on each table, but in
    # parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        args = [(table, args, hypertable_info) for table in hypertable_info.keys()]
        pool.starmap(_filter_constraint, args)

    # TODO: Should we run ANALYZE on the hypertables?


def show_hypertable_incompatibility(hypertable_info, error = True):
    if error:
        log_func = logger.error
    else:
        log_func = logger.warn

    incompatible = False
    for table, info in hypertable_info.items():
        print_table_name = True
        for i in info:
            if i['is_compatible'] == 'f':
                idx_type = ''
                nspname = i['nspname']
                if i['constraint_name']:
                    is_primary_key = i['is_primary_key'] == 't'
                    idx_type = 'PRIMARY KEY' if is_primary_key else 'UNIQUE CONSTRAINT'
                    idxname = i['constraint_name']
                else:
                    idx_type = 'UNIQUE INDEX'
                    idxname = i['idxrelname']
                dimensions = i['dimensions']

                if print_table_name:
                    log_func(f"Table {table} has incompatible constraints/indexes:")
                    print_table_name = False

                log_func(f"""{nspname}.{idxname} {idx_type} doesn't include hypertable """
                         f"dimension {dimensions} as part of "
                         f"its definition.")

                incompatible = True

    if incompatible and error:
        message = f"""
{"*" * 72}
You can do one of the following to resolve the issue:
    1) Skip the compatibility check using the `--skip-hypertable-compatibility-check` flag and manually create the indexes/constraints on the target when the tool fails to create them. This is a most optimal way to resolve the issue.
    2) Fix the incompatible indexes/constraints on the source to include hypertable dimensions and restart the migration from the beginning.
    3) Skip the incompatible indexes/constraints using the `--skip-hypertable-incompatible-objects` flag. Beware, skipping them might slow down the replication of the UPDATE/DELETE operations on the target.
    4) Skip the incompatible indexes/constraints using the `--skip-index` flag and resume the migration.
{"*" * 72}
"""
        log_func(message)


def check_hypertable_incompatibility(args):
    dimensions = get_hypertable_dimensions(args.target)

    incompatible_tables = get_hypertable_incompatible_objects(args.source, dimensions, args.skip_index)

    return incompatible_tables


def hypertable_has_incompatible_objects(hypertable_info):
    for table, info in hypertable_info.items():
        for i in info:
            if i['is_compatible'] == 'f':
                return True

    return False


def filter_incompatible_index_constraint(incompatible_tables):
    incompatible_indexes = []
    for table, info in incompatible_tables.items():
        for i in info:
            nspname = i['nspname']
            idxname = i['idxrelname']
            indexname = f"{nspname}.{idxname}"
            incompatible_indexes.append(indexname)

    return incompatible_indexes


def prepare_filters(args):
    pgcopydb_args = []
    filter = Filter()
    if args.skip_table_data:
        logger.info("Excluding table data: %s", args.skip_table_data)
        filter.exclude_table_data(args.skip_table_data)

    if args.skip_index:
        logger.info("Excluding indexes: %s", args.skip_index)
        filter.exclude_indexes(args.skip_index)

    # empty list of skip_extensions ignores all extensions
    if args.skip_extensions is not None and len(args.skip_extensions) == 0:
        logger.warn("Ignoring all extensions")
        pgcopydb_args.append("--skip-extensions")
    else:
        skip_list = skip_extensions_list(args)
        logger.info("Skipping extensions: %s", skip_list)
        filter.exclude_extensions(skip_list)

    filter_file = str(args.dir / "filter.ini")

    # Apply filters
    with open(filter_file, "w") as f:
        filter.write(f)

    pgcopydb_args.append(f"--filters={filter_file}")
    return pgcopydb_args

@telemetry_command("migrate_existing_data_from_pg_to_tsdb")
def migrate_existing_data_from_pg_to_tsdb(args):
    filter_args = prepare_filters(args)

    with timeit("Dump schema"):
        dump_schema = " ".join(["pgcopydb",
                                "dump",
                                "schema",
                                "--dir",
                                "$PGCOPYDB_DIR/pgcopydb_clone",
                                "--snapshot",
                                "$(cat $PGCOPYDB_DIR/snapshot)",
                                "--resume",
                                ])
        run_cmd(dump_schema, LogFile("dump_schema"))

    with timeit("Restore pre-data"):
        restore_pre_data = " ".join(["pgcopydb",
                                     "restore",
                                     "pre-data",
                                     "--no-acl",
                                     "--no-owner",
                                     "--dir",
                                     "$PGCOPYDB_DIR/pgcopydb_clone",
                                     "--snapshot",
                                     "$(cat $PGCOPYDB_DIR/snapshot)",
                                     "--resume",
                                     ] + filter_args)
        run_cmd(restore_pre_data, LogFile("restore_pre_data"))

    if not is_section_migration_complete("hypertable-creation"):
        show_hypertable_creation_prompt()
        mark_section_complete("hypertable-creation")

    hypertable_info = check_hypertable_incompatibility(args)

    if hypertable_has_incompatible_objects(hypertable_info):
        if args.skip_hypertable_incompatible_objects or args.skip_hypertable_compatibility_check:
            show_hypertable_incompatibility(hypertable_info, error=False)
        else:
            show_hypertable_incompatibility(hypertable_info, error=True)
            sys.exit(1)

    stop_progress = monitor_db_sizes()

    # Always skip primary key, unique constraints and unique indexes
    # The way pgcopydb build the above objects is not compatible with hypertables.
    skip_index = filter_incompatible_index_constraint(hypertable_info)
    if args.skip_index:
        args.skip_index.extend(skip_index)
    else:
        args.skip_index = skip_index
    filter_args = prepare_filters(args)

    with timeit("Copy table data"):
        copy_table_data = " ".join(["pgcopydb",
                                 "clone",
                                 "--table-jobs",
                                 args.table_jobs,
                                 "--index-jobs",
                                 args.index_jobs,
                                 "--split-tables-larger-than='1 GB'",
                                 "--notice",
                                 "--dir",
                                 "$PGCOPYDB_DIR/pgcopydb_clone",
                                 "--resume",
                                 "--no-acl",
                                 "--no-owner",
                                 "--snapshot",
                                 "$(cat $PGCOPYDB_DIR/snapshot)",
                                 ] + filter_args)
        log = LogFile("pgcopydb_clone")
        run_cmd(copy_table_data, log)

    try_creating_incompatible_objects(args, hypertable_info)
    stop_progress.set()


@telemetry_command("migrate_roles")
def migrate_roles():
    logger.info(f"Dumping roles to {env['PGCOPYDB_DIR']}/roles.sql ...")
    with timeit():
        source_pg_uri = env["PGCOPYDB_SOURCE_PGURI"]
        source_dbname = dbname_from_uri(source_pg_uri)
        if source_dbname == "":
            raise ValueError("unable to extract dbname from uri")
        roles_file_path = f"{env['PGCOPYDB_DIR']}/roles.sql"

        dump_roles = " ".join([
            "pg_dumpall",
            "-d",
            '"$PGCOPYDB_SOURCE_PGURI"',
            "--quote-all-identifiers",
            "--roles-only",
            "--no-role-passwords",
            "--clean",
            "--if-exists",
            "-l",
            f'"{source_dbname}"',
            f'--file="{roles_file_path}"',
        ])
        run_cmd(dump_roles)

        # When using MST, Aiven roles modify parameters like "pg_qualstats.enabled" that are not permitted on cloud.
        # Hence, we remove Aiven roles assuming they are not being used for tasks other than ones specific to Aiven/MST.
        filter_stmts = f"""
sed -i -E \
-e '/DROP ROLE IF EXISTS "postgres";/d' \
-e '/DROP ROLE IF EXISTS "tsdbadmin";/d' \
-e '/CREATE ROLE "postgres";/d' \
-e '/ALTER ROLE "postgres"/d' \
-e '/CREATE ROLE "rds/d' \
-e '/ALTER ROLE "rds/d' \
-e '/TO "rds/d' \
-e '/GRANT "rds/d' \
-e 's/(NO)*SUPERUSER//g' \
-e 's/(NO)*REPLICATION//g' \
-e 's/(NO)*BYPASSRLS//g' \
-e 's/GRANTED BY "[^"]*"//g' \
-e '/CREATE ROLE "tsdbadmin";/d' \
-e '/ALTER ROLE "tsdbadmin"/d' \
-e 's/WITH ADMIN OPTION, INHERIT TRUE//g' \
-e 's/WITH ADMIN OPTION,//g' \
-e 's/WITH ADMIN OPTION//g' \
-e 's/GRANTED BY ".*"//g' \
-e '/GRANT "pg_.*" TO/d' \
-e '/CREATE ROLE "_aiven";/d' \
-e '/ALTER ROLE "_aiven"/d' \
{roles_file_path}"""
        run_cmd(filter_stmts)

        restore_roles_cmd = [
            "psql",
             "-X",
             "-d",
             '"$PGCOPYDB_TARGET_PGURI"',
             "-v",
             # Attempt whatever tsdbadmin can do, but don't fail if it fails.
             "ON_ERROR_STOP=0",
             "--echo-errors",
             "-f",
             roles_file_path,
        ]

        restore_roles_cmd = " ".join(restore_roles_cmd)
        log_file = LogFile("restore_roles")
        run_cmd(restore_roles_cmd, log_file)
        print_logs_with_error(log_path=log_file.stderr, after=3, tail=0)


@telemetry_command("migrate_existing_data")
def migrate_existing_data(args):
    logger.info("Copying table data ...")

    filter_args = prepare_filters(args)

    clone_table_data = [
        "pgcopydb",
        "clone",
        "--no-acl",
        "--no-owner",
        "--table-jobs",
        args.table_jobs,
        "--index-jobs",
        args.index_jobs,
        "--split-tables-larger-than='1 GB'",
        "--dir",
        "$PGCOPYDB_DIR/pgcopydb_clone",
        "--snapshot",
        "$(cat $PGCOPYDB_DIR/snapshot)",
        "--notice",
    ] + filter_args

    if args.resume:
        clone_table_data.append("--resume")

    clone_table_data = " ".join(clone_table_data)

    stop_progress = monitor_db_sizes()

    with timeit():
       run_cmd(clone_table_data, LogFile("pgcopydb_clone"))

    stop_progress.set()

@telemetry_command("wait_for_DBs_to_sync")
def wait_for_DBs_to_sync(follow_proc):
    def get_source_wal_lsn():
        return run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select pg_current_wal_lsn();")).strip()

    def get_target_replay_lsn():
        return run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI", sql="select pg_replication_origin_progress('pgcopydb', true);")).strip()

    def get_lsn_diff_bytes(lsn1, lsn2):
        return int(run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI", sql=f"select pg_wal_lsn_diff('{lsn1}', '{lsn2}');")))

    event = wait_for_event("c")

    LSN_UPDATE_INTERVAL_SECONDS=30
    REPLAY_CATCHUP_WINDOW_SECONDS=10 # if replay will catchup within this time, count it as being complete

    logger.info("Getting replication progress")
    wal_lsn, replay_lsn = get_source_wal_lsn(), get_target_replay_lsn()
    event.wait(timeout=LSN_UPDATE_INTERVAL_SECONDS)

    while not event.is_set() and follow_proc.process.poll() is None:
        prev_wal_lsn, prev_replay_lsn = wal_lsn, replay_lsn
        wal_lsn, replay_lsn = get_source_wal_lsn(), get_target_replay_lsn()
        wal_bytes_per_second = get_lsn_diff_bytes(wal_lsn, prev_wal_lsn) / LSN_UPDATE_INTERVAL_SECONDS
        replay_bytes_per_second = get_lsn_diff_bytes(replay_lsn, prev_replay_lsn) / LSN_UPDATE_INTERVAL_SECONDS
        wal_replay_lag_bytes = get_lsn_diff_bytes(wal_lsn, replay_lsn)

        stats = f"(source_wal_rate: {bytes_to_human(wal_bytes_per_second)}/s, target_replay_rate: {bytes_to_human(replay_bytes_per_second)}/s, replay_lag: {bytes_to_human(wal_replay_lag_bytes)})"

        net_replay_per_second = replay_bytes_per_second - wal_bytes_per_second

        replay_will_catch_up_soon = wal_replay_lag_bytes < REPLAY_CATCHUP_WINDOW_SECONDS * net_replay_per_second

        if wal_replay_lag_bytes < REPLICATION_LAG_THRESHOLD_BYTES or replay_will_catch_up_soon:
            logger.info(f"Target has caught up with source {stats}")
            if not IS_TTY and LIVE_MIGRATION_DOCKER:
                logger.info("\tTo stop replication, send a SIGUSR1 signal with 'docker kill -s=SIGUSR1 <container_name>'")
            elif not IS_TTY:
                logger.info(f"\tTo stop replication. To proceed, send a SIGUSR1 signal with 'kill -s=SIGUSR1 {os.getpid()}'")
            else:
                logger.info("\tTo stop replication, hit 'c' and then ENTER")
        elif net_replay_per_second <= 0:
            logger.warn(f"live-replay not keeping up with source load {stats}")
        elif net_replay_per_second > 0:
            arrival_seconds = wal_replay_lag_bytes / net_replay_per_second
            logger.info(f"Live-replay will complete in {seconds_to_human(arrival_seconds)} {stats}" )
        event.wait(timeout=LSN_UPDATE_INTERVAL_SECONDS)

    if follow_proc.process.poll() is not None and follow_proc.process.returncode != 0:
        raise Exception(f"pgcopydb follow exited with code: {follow_proc.process.returncode}")

@telemetry_command("copy_sequences")
def copy_sequences():
    run_cmd("pgcopydb copy sequences --resume --not-consistent", LogFile("copy_sequences"))

@telemetry_command("enable_user_background_jobs")
def enable_user_background_jobs():
    run_sql(execute_on_target=True,
            sql="""
            select public.alter_job(job_id, scheduled => true)
            from timescaledb_information.jobs
            where job_id >= 1000;
            select timescaledb_post_restore();
            """)

def get_caggs_count():
    return int(run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select count(*) from timescaledb_information.continuous_aggregates;")))

@telemetry_command("set_replica_identity_for_caggs")
def set_replica_identity_for_caggs(replica_identity: str = "DEFAULT"):
    sql = f"""
DO \$\$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT materialization_hypertable_schema, materialization_hypertable_name
             FROM timescaledb_information.continuous_aggregates
    LOOP
        EXECUTE 'ALTER TABLE ' ||
                quote_ident(r.materialization_hypertable_schema) || '.' ||
                quote_ident(r.materialization_hypertable_name) ||
                ' REPLICA IDENTITY {replica_identity}';
    END LOOP;
END;
\$\$;
    """
    run_sql(execute_on_target=False,
            sql=sql)


def replication_origin_exists():
    r = run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI",
                sql="select exists(select * from pg_replication_origin where roname='pgcopydb')"))
    return r == "t\n"


def timescaledb_pre_restore():
    logger.info("Timescale pre-restore ...")
    run_sql(execute_on_target=True, sql="select timescaledb_pre_restore();")


def timescaledb_post_restore():
    logger.info("Timescale post-restore ...")
    run_sql(execute_on_target=True,
            sql="""
            begin;
            select public.timescaledb_post_restore();
            -- disable all background jobs
            select public.alter_job(job_id, scheduled => false)
            from timescaledb_information.jobs
            where job_id >= 1000;
            commit;
            """)


def migrate(args):
    # Clean up pid files. This might cause issues in docker environment due
    # deterministic pid values.
    (args.dir / "pgcopydb.pid").unlink(missing_ok=True)
    (args.dir / "pgcopydb_clone" / "pgcopydb.pid").unlink(missing_ok=True)

    if not (args.dir / "snapshot").exists():
        logger.error("You must create a snapshot before starting the migration.")
        print("Run the following command to create a snapshot:")
        print(docker_command('live-migration-snapshot', 'snapshot'))
        sys.exit(1)

    # check whether the snapshot is valid if initial data migration
    # is not yet complete.
    if not is_section_migration_complete("initial-data-migration") and not is_snapshot_valid():
            logger.error("Invalid snapshot found. Snapshot process might have died or failed.")
            logger.info("Please restart the migration process.")
            print("Run the following command to clean the existing resources:")
            print(docker_command('live-migration-clean', 'clean', '--prune'))
            print()
            print("Run the following command to create a new snapshot:")
            print(docker_command('live-migration-snapshot', 'snapshot'))
            sys.exit(1)

    # resume but no previous migration found
    if not replication_origin_exists() and args.resume:
        logger.error("No resumable migration found.")
        print("To start the migration:")
        print(docker_command('live-migration-migrate', 'migrate'))
        sys.exit(1)

    # if replication origin exists, then the previous migration was incomplete
    if replication_origin_exists() and not args.resume:
        logger.error("Found an incomplete migration.")
        print("To resume the migration:")
        print(docker_command('live-migration-migrate', 'migrate', '--resume'))
        print()
        print("To start a new migration, clean up the existing resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(1)

    source_type = get_dbtype(args.source)
    target_type = get_dbtype(args.target)

    if args.pg_src:
        source_type = DBType.POSTGRES
    if args.pg_target:
        target_type = DBType.POSTGRES

    match (source_type, target_type):
        case (DBType.POSTGRES, DBType.POSTGRES):
            logger.info("Migrating from Postgres to Postgres ...")
        case (DBType.POSTGRES, DBType.TIMESCALEDB):
            logger.info("Migrating from Postgres to TimescaleDB ...")
        case (DBType.TIMESCALEDB, DBType.TIMESCALEDB):
            logger.info("Migrating from TimescaleDB to TimescaleDB ...")
        case (DBType.TIMESCALEDB, DBType.POSTGRES):
            logger.info("Migration from TimescaleDB to Postgres is not supported")
            sys.exit(1)

    caggs_count = 0
    if source_type == DBType.TIMESCALEDB:
        caggs_count = get_caggs_count()
        if caggs_count > 0:
            logger.info(f"Setting replica identity to FULL for {caggs_count} caggs ...")
            set_replica_identity_for_caggs('FULL')

    # reset endpos
    if args.resume:
        run_cmd("pgcopydb stream sentinel set endpos --dir $PGCOPYDB_DIR 0/0")

    housekeeping_thread, housekeeping_stop_event = None, None
    follow_proc = create_follow(resume=args.resume)
    try:
        if not is_section_migration_complete("roles") and not args.skip_roles:
            logger.info("Migrating roles from Source DB to Target DB ...")
            migrate_roles()
            mark_section_complete("roles")

        if not is_section_migration_complete("initial-data-migration"):
            logger.info("Migrating existing data from Source DB to Target DB ...")
            match (source_type, target_type):
                case (DBType.POSTGRES, DBType.POSTGRES):
                    migrate_existing_data(args)
                case (DBType.POSTGRES, DBType.TIMESCALEDB):
                    migrate_existing_data_from_pg_to_tsdb(args)
                case (DBType.TIMESCALEDB, DBType.TIMESCALEDB):
                    timescaledb_pre_restore()
                    migrate_existing_data(args)
                    # IMPORTANT: timescaledb_post_restore must come after post-data,
                    # otherwise there are issues restoring indexes and
                    # foreign key constraints to chunks.
                    timescaledb_post_restore()
                case (DBType.TIMESCALEDB, DBType.POSTGRES):
                    logger.error("Migration from TimescaleDB to Postgres is not supported")
                    sys.exit(1)
            mark_section_complete("initial-data-migration")

        (housekeeping_thread, housekeeping_stop_event) = start_housekeeping(env)

        logger.info("Converting materialized views to views ...")
        convert_matview_to_view(args.target)

        logger.info("Applying buffered transactions ...")
        run_cmd("pgcopydb stream sentinel set apply --dir $PGCOPYDB_DIR")

        wait_for_DBs_to_sync(follow_proc)

        run_cmd("pgcopydb stream sentinel set endpos --dir $PGCOPYDB_DIR --current")

        logger.info("Waiting for live-replay to complete ...")
        # TODO: Implement retry for follow.
        follow_proc.wait()
        follow_proc = None

    except KeyboardInterrupt:
        logger.info("Exiting ... (Ctrl+C)")

    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        logger.error(traceback.format_exc())
        telemetry.complete_fail()
        logger.error("An error occurred during the live migration. "
                     "Please report this issue to support@timescale.com "
                     "with all log files from the <volume-mount>/logs "
                     "directory.")
    else:
        logger.info("Copying sequences ...")
        copy_sequences()

        logger.info("Restoring materialized views ...")
        restore_matview(args.target)

        if source_type == DBType.TIMESCALEDB:
            logger.info("Enabling background jobs ...")
            enable_user_background_jobs()
            if caggs_count > 0:
                logger.info("Setting replica identity back to DEFAULT for caggs ...")
                set_replica_identity_for_caggs('DEFAULT')

        logger.info("Migration successfully completed.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean'))
        telemetry.complete_success()

    finally:
        # TODO: Use daemon threads for housekeeping and health_checker.
        health_checker.stop_all()
        if housekeeping_stop_event:
            housekeeping_stop_event.set()
        if housekeeping_thread and housekeeping_thread.is_alive():
            housekeeping_thread.join()
        # cleanup all subprocesses created by pgcopydb follow
        if follow_proc and follow_proc.process.poll() is None:
            os.killpg(os.getpgid(follow_proc.process.pid), signal.SIGINT)
            follow_proc.wait()
        logger.info("All processes have exited successfully.")
