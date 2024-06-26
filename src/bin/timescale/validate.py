import logging
import sys

from exec import run_sql, run_cmd, psql
from environ import env
from utils import get_dbtype, DBType

logger = logging.getLogger(__name__)

def get_pg_settings(setting: str):
    return run_sql(execute_on_target=False, sql=f"select setting from pg_settings where name = '{setting}'")[:-1]


def get_src_wal_level():
    return get_pg_settings("wal_level")


def get_src_snapshot_threshold():
    return get_pg_settings("old_snapshot_threshold")


def has_execute_perms_on_target(func_name: str) -> bool:
    return run_sql(execute_on_target=True, sql=f"SELECT has_function_privilege('pg_catalog.{func_name}', 'EXECUTE')")[:-1] == "t"


def has_necessary_perms_on_target() -> bool:
    has_perms = True
    for f in [
        "pg_replication_origin_oid(text)",
        "pg_replication_origin_create(text)",
        "pg_replication_origin_advance(text, pg_lsn)",
        "pg_replication_origin_progress(text, boolean)",
        "pg_replication_origin_drop(text)",
        "pg_replication_origin_session_setup(text)",
        "pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone)"]:
        if not has_execute_perms_on_target(f):
            has_perms = False
            logger.error(f"current_user does not have EXECUTE permission on pg_catalog.{f}")
    return has_perms


def has_tables_without_pkey_replident() -> list[str]:
    sql = """select array_agg(table_name) AS tables FROM (
            SELECT n.nspname || '.' || c.relname AS table_name
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary = true
            LEFT JOIN pg_constraint con ON con.conrelid = c.oid AND con.contype IN ('p', 'u')
            WHERE c.relkind = 'r' -- only consider ordinary tables
            AND n.nspname NOT IN (
                '_timescaledb_internal',
                '_timescaledb_config',
                '_timescaledb_catalog',
                '_timescaledb_cache',
                'timescaledb_experimental',
                'timescaledb_information',
                '_timescaledb_functions',
                'information_schema',
                'pg_catalog'
                ) -- exclude system tables
            AND i.indrelid IS NULL -- no primary key
            AND con.conrelid IS NULL -- no unique constraints
            AND c.relreplident = 'd' -- default replica identity (not explicitly set)
            GROUP BY n.nspname, c.relname
            ORDER BY n.nspname, c.relname
        ) subquery"""
    tables_with_no_pkey_and_replident = run_sql(execute_on_target=False, sql=sql)[:-1]
    if tables_with_no_pkey_and_replident == "":
        return []
    tables_with_no_pkey_and_replident = tables_with_no_pkey_and_replident[1:-1] # Remove the enclosing curly braces.
    tables_with_no_pkey_and_replident = tables_with_no_pkey_and_replident.split(",")
    return tables_with_no_pkey_and_replident


def check_timescaledb_version():
    result = run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select extversion from pg_extension where extname = 'timescaledb';"))
    source_ts_version = str(result)[:-1]

    result = run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI", sql="select extversion from pg_extension where extname = 'timescaledb';"))
    target_ts_version = str(result)[:-1]

    if source_ts_version != target_ts_version:
        # In case of PG to TS migration, this will be False as source_ts_version will be empty.
        logger.error(f"Source TimescaleDB version ({source_ts_version}) does not match Target TimescaleDB version ({target_ts_version})")
        sys.exit(1)


def validate_dbs():
    wal_level = get_src_wal_level()
    if wal_level != "logical":
        logger.error(f"Live migration requires Source DB 'wal_level' to be 'logical'. Found: '{wal_level}'")
        sys.exit(1)

    threshold = get_src_snapshot_threshold()
    if threshold != "-1":
        logger.error(f"Live migration requires 'old_snapshot_threshold' setting to be '-1' on Source DB. Found: '{threshold}'")
        sys.exit(1)

    if not has_necessary_perms_on_target():
        logger.error(f"Live migration requires the current user to have 'EXECUTE' permissions on pg_replication_origin functions in the Target DB")
        sys.exit(1)

    if get_dbtype(env["PGCOPYDB_SOURCE_PGURI"]) == DBType.TIMESCALEDB:
        check_timescaledb_version()
