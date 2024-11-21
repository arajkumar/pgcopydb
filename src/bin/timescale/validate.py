import os
import logging
import typing

from enum import Enum
from dataclasses import dataclass

from psql import psql as psql
from utils import get_dbtype, DBType, docker_command, get_terminal_width
from environ import LIVE_MIGRATION_DOCKER
from exception import ValidationError

logger = logging.getLogger(__name__)

def raise_if_volume_not_mounted(dir):
    if LIVE_MIGRATION_DOCKER and not os.path.ismount(dir):
        message = f"""
        Volume mount not found!
        Volume mount is required to store the state of the migration process
        to resume the interrupted migration.

        To proceed, mount a volume: '-v <host_dir>:{dir}'
        To create a snapshot, run the following command:
        {docker_command('live-migration-snapshot', 'snapshot')}
        """
        raise ValidationError(message)

def _has_replication_origin_permission(conn) -> bool:
    def _has_perm(func_name: str) -> bool:
        return psql(conn=conn,
                    sql=f"""
                    SELECT has_function_privilege('pg_catalog.{func_name}',
                                                  'EXECUTE') as res
                    """)[0]["res"] == "t"

    for f in [
        "pg_replication_origin_oid(text)",
        "pg_replication_origin_create(text)",
        "pg_replication_origin_advance(text, pg_lsn)",
        "pg_replication_origin_progress(text, boolean)",
        "pg_replication_origin_drop(text)",
        "pg_replication_origin_session_setup(text)",
        "pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone)",
        # "pg_switch_wal()", # for testing
        ]:
        if not _has_perm(f):
            return False

    return True


def _has_tables_without_replica_ident(source) -> list[dict]:
    # Exclude chunks from the check. Hypertables are already checked.
    if get_dbtype(source) == DBType.TIMESCALEDB:
       exclude_chunks = """
        LEFT JOIN _timescaledb_catalog.chunk c ON (t.nspname = c.schema_name AND t.relname = c.table_name)
        WHERE c.schema_name IS NULL AND c.table_name IS NULL
        """
    else:
        exclude_chunks = ""

    sql = f"""WITH tables AS (
            SELECT
            n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary = true
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
            AND c.relreplident = 'd' -- default replica identity (not explicitly set)
            GROUP BY n.nspname, c.relname
            ORDER BY n.nspname, c.relname
        )
        SELECT FORMAT('%I.%I', nspname, relname) AS table_name
        FROM tables t
        {exclude_chunks}
        """
    result = psql(conn=source, sql=sql)
    result = [r["table_name"] for r in result]
    return result

def _target_is_timescale_cloud(target) -> bool:
    # _timescaledb_catalog.metadata table has the following keys:
    # - forge_profile_id
    # - forge_service_id
    # - forge_env
    sql = """
        SELECT count(*) > 0 as cloud FROM _timescaledb_catalog.metadata
        WHERE key IN ('forge_profile_id', 'forge_service_id', 'forge_env')
    """
    return psql(conn=target, sql=sql)[0]["cloud"] == "t"


class Status(Enum):
    OK = 1
    WARN = 2
    FAIL = 3

@dataclass
class Check:
    check_message: str = ""
    sql: str = ""
    source_value: typing.Callable = None
    target_value: typing.Callable = None
    check: typing.Callable = None
    help: str = ""
    # If the check func fails, only warn, do not fail the check.
    warn_only: bool = False

    # result of the check
    status: Status = Status.OK
    message: str = ""

    def run(self):
        source, target = None, None
        if self.source_value:
            source = self.source_value(self.sql)
        if self.target_value:
            target = self.target_value(self.sql)

        ok = self.check(source, target)
        if ok:
            self.status = Status.OK
            self.message = self.check_message
        else:
            self.message = self.help.format(source=source, target=target)
            if self.warn_only:
                self.status = Status.WARN
            else:
                self.status = Status.FAIL

class Report:
    def __init__(self, checks: list[Check]):
        self.successes = [c for c in checks if c.status == Status.OK]
        self.warnings = [c for c in checks if c.status == Status.WARN]
        self.errors = [c for c in checks if c.status == Status.FAIL]

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def raise_on_error(self):
        SEPARATOR = "=" * get_terminal_width()
        if len(self.warnings) > 0:
            warning_message = f"Following compatibility checks have warnings:\n{SEPARATOR}\n"
            for w in self.warnings:
                message = f"{w.message}\n{SEPARATOR}\n"
                warning_message += message

            logger.warning(warning_message)

        if len(self.errors) > 0:
            error_message = f"Following compatibility checks have failed:\n{SEPARATOR}\n"
            for e in self.errors:
                message = f"{e.message}\n{SEPARATOR}\n"
                error_message += message

            raise ValidationError(error_message)

    def pretty_print(self):
        for s in self.successes:
            print('[✅]', s.message)

        if len(self.warnings) > 0:
            print()
            print("⚠️Following compatibility checks have warnings:")
            for w in self.warnings:
                print('[⚠️]', w.message)

        if len(self.errors) > 0:
            print()
            print("⛔Following compatibility checks failed:")
            for f in self.errors:
                print('[❌]', f.message)

def validate(args):
    report = check_db_compatibility(args=args)
    report.pretty_print()
    args.telemetry.mark_success()

def check_db_compatibility(args) -> Report:
    """
    Performs a series of compatibility checks between the source and target databases to
    determine if they meet the criteria for live migration. The function returns `True`
    if both databases are compatible for live migration. Additionally, it provides warnings
    about any extensions present in the source database that are not supported by Timescale Cloud.
    """
    source_uri = args.source
    target_uri = args.target

    checks = [
        Check(
            check_message="The user has execute permission on "
                          "pg_replication_origin functions in the target db",
            sql="SELECT current_user;",
            target_value=lambda x: str(psql(target_uri, x)[0]["current_user"]),
            check=lambda s, t: _has_replication_origin_permission(target_uri),
            help="The current user '{target}' does not have permission to "
                 "execute replication origin functions in the target db."
        ),
        Check(
            check_message="REPLICA IDENTITY or PRIMARY KEY on all tables",
            source_value=lambda _: _has_tables_without_replica_ident(source_uri),
            check=lambda s, _: len(s) == 0,
            help="Following tables do not have a primary key or "
                 "replica identity: \n"
                 "{source} \n"
                 "UPDATE and DELETE statements on these tables will not be "
                 "replicated to the target db.",
            warn_only=True
        ),
        Check(
            check_message="Postgres version in source db <= target db",
            sql="select current_setting('server_version_num')::float4 as version",
            source_value=lambda x: float(psql(source_uri, x)[0]["version"]),
            target_value=lambda x: float(psql(target_uri, x)[0]["version"]),
            check=lambda source, target: source <= target,
            help="Postgres version in source {source} is greater than "
                 "target {target}. This is not tested and take your own risk.",
            warn_only=True
        ),
        Check(
            check_message="Postgres version in source db >= 9",
            sql="select current_setting('server_version_num')::float4 as version",
            source_value=lambda x: float(psql(source_uri, x)[0]["version"]),
            target_value=None,
            check=lambda source, _: source >= 90000,
            help="Postgres version {source} does not support logical decoding."),
        Check(
            check_message="GUC 'wal_level' must be logical",
            sql="select current_setting('wal_level') as setting",
            source_value=lambda x: str(psql(source_uri, x)[0]["setting"]),
            target_value=None,
            check=lambda source, _: source == "logical",
            help="Source db 'wal_level' GUC must be set to 'logical'"),
        Check(
            check_message="GUC 'old_snapshot_threshold' must be -1",
            sql="select current_setting('old_snapshot_threshold') as setting",
            source_value=lambda x: int(psql(source_uri, x)[0]["setting"]),
            target_value=None,
            check=lambda source, _: source == -1,
            help="Source db 'old_snapshot_threshold' GUC must be set to -1"
        ),
        Check(
            check_message="Source db size should be below 12TB",
            sql="select pg_database_size(current_database()) as size",
            source_value=lambda x: int(psql(source_uri, x)[0]["size"]),
            target_value=None,
            check=lambda source, _: source < 12_000_000_000_000, # 12 TB.
            help="Live migration should not be used with source above 12TB to "
                 "avoid running out of space on Timescale cloud during migration.",
            warn_only=True,
        ),
        Check(
            check_message="Source db should not have native partitioning",
            sql="select count(*) as count from pg_partitioned_table",
            source_value=lambda x: int(psql(source_uri, x)[0]["count"]),
            target_value=None,
            check=lambda source, _: source == 0,
            help="Source db has native partitioning. You may not be able to "
                 "convert them to hypertables. But, you can still migrate the "
                 "data as Postgres partitioned tables and use your own solution "
                 "to convert them to hypertables.",
            warn_only=True,
        ),
        Check(
            check_message="Source db should ideally not have non-standard tablespaces",
            sql="select coalesce(array_agg(spcname), '{}'::text[]) as coalesce from pg_tablespace where spcname not in ('pg_default', 'pg_global')",
            source_value=lambda x: str(psql(source_uri, x)[0]["coalesce"]),
            target_value=None,
            check=lambda source, _: source == "{}",
            help="Non default table spaces found. While live migration works "
                 "when source db has non-standard/default tablespaces, "
                 "it doesn't migrate the non-default ones.",
            warn_only=True),
        Check(
            check_message="Source db should have only supported extensions",
            sql="""
        select coalesce(json_agg(json_build_object(extname, extversion)), '[]'::json) as agg FROM pg_extension
        where extname not in (
            'bloom', 'btree_gin', 'btree_gist', 'citext',
            'cube', 'dict_int', 'dict_xsyn', 'fuzzystrmatch',
            'hstore', 'intarray', 'isn', 'lo', 'ltree',
            'pg_stat_statements', 'pg_trgm', 'pgcrypto', 'pgpcre',
            'pgrouting', 'pgstattuple', 'pgvector', 'pg_buffercache',
            'plpgsql', 'postgis', 'postgis_raster', 'postgis_sfcgal',
            'postgis_tiger_geocoder', 'postgis_topology', 'seg',
            'tablefunc', 'tcn', 'timescaledb_toolkit',
            'timescaledb', 'tsm_system_rows', 'tsm_system_time',
            'unaccent', 'uuid-ossp',
           -- Though we don't support the following, it is still fine to
           -- include here as we skip them by default
           'aiven_extras', 'rds_tools'
        )
        """,
            source_value=lambda x: str(psql(source_uri, x)[0]["agg"]),
            target_value=None,
            check=lambda source, _: source == "[]",
            help="Unsupported extensions found on the source. Following "
                 "extensions are not supported on Timescale Cloud: {source}. "
                 "You can skip unsupported extension using --skip-extension "
                 "flag during migration.",
            warn_only = True,
        ),
        Check(
            check_message="Source db should not have tables with NaN, +- Infinity as values",
            sql="""
            select exists (
                select 1 from pg_stats where
                    schemaname not in (
                        '_timescaledb_internal', '_timescaledb_config', '_timescaledb_catalog', '_timescaledb_cache',
                        'timescaledb_experimental', 'timescaledb_information', '_timescaledb_functions',
                        'information_schema', 'pg_catalog')
                and
                    (
                        exists (
                            select 1 from unnest(most_common_vals::text::text[]) as v
                            where
                                v IN ('NaN', 'Infinity', '-Infinity')
                        )
                    or
                        exists (
                            select 1 from unnest(histogram_bounds::text::text[]) as h
                            where
                                h IN ('NaN', 'Infinity', '-Infinity')
                        )
                    )
            )
            """,
            source_value=lambda x: str(psql(source_uri, x)[0]["exists"]),
            target_value=None,
            check=lambda source, _: source == "f",
            help="NaN/Inf values found on the source. Currently the tool do not "
                 "replicate NaN/Infinity values. Don't use this tool if you "
                 "have such values."
    ),
    ]

    if get_dbtype(source_uri) == DBType.TIMESCALEDB:
        has_caggs_finalized = psql(source_uri,
                                "select true as exists from pg_attribute where "
                                "attrelid='timescaledb_information.continuous_aggregates'::regclass "
                                "and attname='finalized' and not attisdropped "
                                "and attnum > 0;")
        has_caggs_finalized = has_caggs_finalized and has_caggs_finalized[0]["exists"]
        tsdb_checks = []

        if _target_is_timescale_cloud(target_uri) and not args.force_timescaledb_public_schema:
            tsdb_checks.append(
                Check(
                    check_message="TimescaleDB extension installed on 'public' schema",
                    sql="select n.nspname nspname from pg_extension e join pg_namespace n on e.extnamespace = n.oid where extname = 'timescaledb'",
                    source_value=lambda x: str(psql(source_uri, x)[0]["nspname"]),
                    target_value=None,
                    check=lambda source, _: source == "public",
                    help="TimescaleDB extension on source is installed on "
                         "non public schema."
                         "TimescaleDB extension on source should be installed on "
                         "'public' schema. If not, you can still migrate to "
                         "Timescale Cloud by using "
                         "--force-timescaledb-public-schema flag when creating "
                         "snapshot. "
                         "Be aware that using this flag will cause issues if "
                         "existing queries references TimescaleDB objects "
                         "using the custom schema. You may need to update those "
                         "queries not to reference the custom schema and "
                         "set Postgres search_path to custom schema."
                )
            )

        if has_caggs_finalized:
            tsdb_checks.append(Check(
                check_message="Source db does not have old partial-form continuous aggregates",
                sql="select (count(*) > 0) as exists from timescaledb_information.continuous_aggregates where not finalized",
                source_value=lambda x: str(psql(source_uri, x)[0]["exists"]),
                target_value=None,
                check=lambda source, _: source == "f",
                help="Old partial-form continuous aggregates are not supported. "
                     "You should upgrade to new format before starting "
                     "live migration."
                     "To migrate, visit https://docs.timescale.com/api/latest/continuous-aggregates/cagg_migrate/"
            ))

        if not args.migrate_across_timescaledb_versions:
            tsdb_checks.append(
                Check(
                    check_message="TimescaleDB version matches between source and target db",
                    sql="select extversion from pg_extension where extname = 'timescaledb'",
                    source_value=lambda x: str(psql(source_uri, x)[0]["extversion"]),
                    target_value=lambda x: str(psql(target_uri, x)[0]["extversion"]),
                    check=lambda source, target: source == target,
                    help="TimescaleDB version ({source}) on source does not match "
                         "version target({target}). For more information, visit "
                         "https://docs.timescale.com/migrate/latest/live-migration/live-migration-faq",
                )
            )

        checks.extend(tsdb_checks)

    for c in checks:
        c.run()

    return Report(checks)
