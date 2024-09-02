# This script orchestrates CDC based migration process using pgcopydb.
import os
import threading
import logging
import textwrap
from pathlib import Path

import housekeeping

from health_check import health_checker
from utils import timeit, docker_command, dbname_from_uri, store_val, \
    get_stored_val, bytes_to_human, seconds_to_human, DBType, get_dbtype, \
    get_snapshot_id
from environ import LIVE_MIGRATION_DOCKER, env
from usr_signal import wait_for_event, IS_TTY
from filter import Filter
from exception import (
    ValidationError,
)
from validate import raise_if_volume_not_mounted

from exec import (
        Process,
        run_cmd,
        psql,
        print_logs_with_error,
        LogFile,
)
from catalog import (
        target,
        pgcopydb
)
from psql import psql as psql_cmd
from tsdb.postgres import create_hypertable_compatibility
from tsdb.cross_version import CrossVersionMigration
from tsdb.timescaledb import TimescaleDB

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

def create_follow(resume: bool = False):
    logger.info(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}...")

    follow_args = [
        "pgcopydb",
        "follow",
        "--dir",
        env["PGCOPYDB_DIR"],
    ]
    follow_retry_args = follow_args + ["--resume", "--not-consistent"]

    if resume:
        follow_args.extend(["--resume", "--not-consistent"])

    def is_error_func_for_follow(log_line: str):
        if "pgcopydb.sentinel" in log_line:
            return False
        if "ERROR" in log_line or \
            "free(): double free detected" in log_line or \
            "no tuple identifier for" in log_line:
            return True
        return False

    return Process(follow_args, "follow") \
        .with_logging() \
        .with_health_check(is_error_func_for_follow) \
        .with_retry(follow_retry_args) \
        .run()


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


def monitor_db_sizes(dir, source, target) -> threading.Event:
    DB_SIZE_SQL = "select pg_size_pretty(pg_database_size(current_database())) as size"
    src_size = get_stored_val(dir, "src_existing_data_size")
    if src_size is None:
        src_size = psql_cmd(source, sql=DB_SIZE_SQL)[0]["size"]
        # We save the size of the source database for reuse when running `migrate` with the
        # --resume option. Without this step, the source size would need to be recalculated
        # during the `--resume` phase, leading to inaccurate progress.
        store_val(dir, "src_existing_data_size", src_size)

    stop_event = threading.Event()
    logger.info("Monitoring initial copy progress ...")
    def get_and_print_size():
        while not stop_event.is_set():
            try:
                # During a disk resize event on the target instance (Timescale Cloud),
                # it will be temporarily unavailable. If we query the target database size
                # during this period of unavailability, it will cause the monitor_db_size() thread
                # to panic and stop execution. To prevent this, the database size query is placed
                # inside a try block to handle any temporary unavailability.
                tgt_size = psql_cmd(target, sql=DB_SIZE_SQL)[0]["size"]
            except Exception:
                pass
            else:
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
            "rds_tools",
    ]

    if args.skip_extensions:
        return skip_extensions_default + args.skip_extensions
    else:
        return skip_extensions_default


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

    if args.migrate_across_timescaledb_versions:
        # Exclude timescaledb extension and hypertable chunks.
        hypertables = TimescaleDB(args.source, get_snapshot_id(args.dir)).hypertables()
        chunks = [c.table_name for ht in hypertables for c in ht.chunks()]
        filter.exclude_table(chunks)
        filter.exclude_extensions(["timescaledb"])

    filter_file = str(args.dir / "filter.ini")

    # Apply filters
    with open(filter_file, "w") as f:
        filter.write(f)

    pgcopydb_args.append(f"--filters={filter_file}")
    return pgcopydb_args


def migrate_existing_data_across_ts_versions(args):
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
                                ] + filter_args)
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

    stop_progress = monitor_db_sizes(args.dir, args.source, args.target)

    pgcopydb_dir = args.dir / "pgcopydb_clone"

    with timeit("Copy table data"):
        clone_args = ["pgcopydb",
                        "clone",
                        "--table-jobs",
                        args.table_jobs,
                        "--index-jobs",
                        args.index_jobs,
                        "--split-tables-larger-than=1GB",
                        "--notice",
                        "--dir",
                        str(pgcopydb_dir),
                        "--resume",
                        "--no-acl",
                        "--no-owner",
                        "--fail-fast",
                        "--snapshot",
                        get_snapshot_id(env['PGCOPYDB_DIR']),
                    ] + filter_args
        (Process(clone_args, "clone")
            .with_logging()
            # Use the same args for retry as for the actual command since the actual command has --resume.
            .with_retry(retry_args=clone_args, max_retries=5, backoff=20)
            .run()
            .wait())

        cross_migration = CrossVersionMigration(args)

        if not cross_migration.validate():
            raise ValidationError("Cross version migration validation failed. Please resolve the issues before proceeding.")

        # Migrate hypertables from source to target. i.e. create hypertables
        # based on the dimension info available on source to target.
        cross_migration.migrate_hypertable_schema()

        # Migrate hypertable data from source to target
        # It suboptimal to do this post-data due to the existence of indexes
        # and constraints on the hypertable. However, this is the best we can do
        # given the current limitations of pgcopydb, because it truncates the
        # target table before copying data.
        cross_migration.migrate_hypertable_data()

    stop_progress.set()


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
                                ] + filter_args)
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

    compatibility = create_hypertable_compatibility(args)

    warn = args.skip_hypertable_incompatible_objects or args.skip_hypertable_compatibility_check
    error_shown = compatibility.warn_incompatibility(error=not warn)
    if error_shown:
        raise ValidationError("Please resolve the issues before proceeding.")

    stop_progress = monitor_db_sizes(args.dir, args.source, args.target)

    pgcopydb_dir = args.dir / "pgcopydb_clone"
    # Apply the filter directly into the catalog
    catalog = pgcopydb.Catalog(dir=pgcopydb_dir)
    compatibility.apply_filters(catalog)
    catalog.update()

    with timeit("Copy table data"):
        clone_args = ["pgcopydb",
                        "clone",
                        "--table-jobs",
                        args.table_jobs,
                        "--index-jobs",
                        args.index_jobs,
                        "--split-tables-larger-than=1GB",
                        "--notice",
                        "--dir",
                        str(pgcopydb_dir),
                        "--resume",
                        "--no-acl",
                        "--no-owner",
                        "--fail-fast",
                        "--snapshot",
                        get_snapshot_id(env['PGCOPYDB_DIR']),
                    ] + filter_args
        (Process(clone_args, "clone")
            .with_logging()
            # Use the same args for retry as for the actual command since the actual command has --resume.
            .with_retry(retry_args=clone_args, max_retries=5, backoff=20)
            .run()
            .wait())

    compatibility.create_incompatible_objects()
    stop_progress.set()


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


def migrate_existing_data(args, timescaledb: TimescaleDB = None):
    logger.info("Copying table data ...")

    filter_args = prepare_filters(args)

    clone_dir = str(args.dir / "pgcopydb_clone")

    with timeit("Dump schema"):
        dump_schema = " ".join(["pgcopydb",
                                "dump",
                                "schema",
                                "--dir",
                                clone_dir,
                                "--snapshot",
                                "$(cat $PGCOPYDB_DIR/snapshot)",
                                "--resume",
                                ] + filter_args)
        run_cmd(dump_schema, LogFile("dump_schema"))

    with timeit("Restore pre-data"):
        restore_pre_data = " ".join(["pgcopydb",
                                     "restore",
                                     "pre-data",
                                     "--no-acl",
                                     "--no-owner",
                                     "--dir",
                                     clone_dir,
                                     "--snapshot",
                                     "$(cat $PGCOPYDB_DIR/snapshot)",
                                     "--resume",
                                     ] + filter_args)
        run_cmd(restore_pre_data, LogFile("restore_pre_data"))

    if timescaledb:
       timescaledb.pre_restore()

    clone_args = [
        "pgcopydb",
        "clone",
        "--no-acl",
        "--no-owner",
        "--fail-fast",
        "--table-jobs",
        args.table_jobs,
        "--index-jobs",
        args.index_jobs,
        "--split-tables-larger-than=1GB",
        "--dir",
        clone_dir,
        "--snapshot",
        get_snapshot_id(env['PGCOPYDB_DIR']),
        "--notice",
        "--resume",
    ] + filter_args

    stop_progress = monitor_db_sizes(args.dir, args.source, args.target)

    with timeit():
       (Process(clone_args, "clone")
            .with_logging()
            # The primary aim for using a retry here is to handle disk resize events.
            # We believe 95% of disk retries should complete within 5 minutes.
            # Hence, the below retry is for a total of 5 minutes, i.e., (1+2+...5) * 20 = 300 seconds.
            .with_retry(retry_args=clone_args, max_retries=5, backoff=20)
            .run()
            .wait())


    if timescaledb:
        # IMPORTANT: timescaledb_post_restore must come after post-data,
        # otherwise there are issues restoring indexes and
        # foreign key constraints to chunks.
        timescaledb.post_restore(disable_jobs=True)

    stop_progress.set()

def wait_for_DBs_to_sync(follow: Process):
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

    while not event.is_set():
        if not follow.alive():
            raise Exception("Follow process died. Exiting ...")

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

def copy_sequences():
    run_cmd("pgcopydb copy sequences --resume --not-consistent",
            LogFile("copy_sequences"))

def get_caggs_count(conn) -> int:
    sql = """
        SELECT count(*) as count FROM timescaledb_information.continuous_aggregates;
    """
    result = psql_cmd(conn=conn, sql=sql)
    return int(result[0]["count"])

def set_replica_identity_for_caggs(conn, replica_identity: str = "DEFAULT"):
    sql = f"""
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT materialization_hypertable_schema, materialization_hypertable_name
             FROM timescaledb_information.continuous_aggregates
    LOOP
        EXECUTE FORMAT('ALTER TABLE %I.%I REPLICA IDENTITY {replica_identity}',
                r.materialization_hypertable_schema,
                r.materialization_hypertable_name)
    END LOOP;
END;
$$;
    """
    psql_cmd(conn=conn,
            sql=sql)


def replication_origin_exists(conn):
    sql = """
        SELECT
            (count(*) > 0) AS exists
        FROM pg_replication_origin WHERE roname='pgcopydb'
    """
    r = psql_cmd(conn=conn, sql=sql)
    return r[0]["exists"] == "t"


def validate(args):
    raise_if_volume_not_mounted(args.dir)

    if not (args.dir / "snapshot").exists():
        message = f"""
        You must create a snapshot before starting the migration.
        Run the following command to create a snapshot:
        {docker_command('live-migration-snapshot', 'snapshot')}
        """
        raise ValidationError(message)

    # check whether the snapshot is valid if initial data migration
    # is not yet complete.
    if not is_section_migration_complete("initial-data-migration") and not is_snapshot_valid():
            message = f"""
            Invalid snapshot found. Snapshot process might have died or failed.
            Please restart the migration process.
            Run the following command to clean the existing resources:
            {docker_command('live-migration-clean', 'clean', '--prune')}

            Run the following command to create a new snapshot:
            {docker_command('live-migration-snapshot', 'snapshot')}
            """
            raise ValidationError(message)

    # resume but no previous migration found
    if not replication_origin_exists(args.target) and args.resume:
        message = f"""
        No resumable migration found. To start the migration:
        {docker_command('live-migration-migrate', 'migrate')}
        """
        raise ValidationError(message)

    # if replication origin exists, then the previous migration was incomplete
    if replication_origin_exists(args.target) and not args.resume:
        message = f"""
        Found an incomplete migration. To resume the migration:
        {docker_command('live-migration-migrate', 'migrate', '--resume')}

        To start a new migration, clean up the existing resources:
        {docker_command('live-migration-clean', 'clean', '--prune')}
        """
        raise ValidationError(message)

def _migrate(args, follow):
    # Clean up pid files. This might cause issues in docker environment due
    # deterministic pid values.
    (args.dir / "pgcopydb.pid").unlink(missing_ok=True)
    (args.dir / "pgcopydb_clone" / "pgcopydb.pid").unlink(missing_ok=True)

    source_type = get_dbtype(args.source)
    target_type = get_dbtype(args.target)

    if args.pg_src:
        source_type = DBType.POSTGRES
    if args.pg_target:
        target_type = DBType.POSTGRES
    if args.migrate_across_timescaledb_versions:
        source_type = DBType.TIMESCALEDB_SKIP_VERSION
        target_type = DBType.TIMESCALEDB

    timescaledb: TimescaleDB = None
    match (source_type, target_type):
        case (DBType.POSTGRES, DBType.POSTGRES):
            logger.info("Migrating from Postgres to Postgres ...")
        case (DBType.POSTGRES, DBType.TIMESCALEDB):
            logger.info("Migrating from Postgres to TimescaleDB ...")
        case (DBType.TIMESCALEDB, DBType.TIMESCALEDB):
            logger.info("Migrating from TimescaleDB to TimescaleDB ...")
            timescaledb = TimescaleDB(args.target)
        case (DBType.TIMESCALEDB_SKIP_VERSION, DBType.TIMESCALEDB):
            logger.info("Migrating TimescaleDB to TimescaleDB(cross version) ....")
        case (DBType.TIMESCALEDB, DBType.POSTGRES):
            raise ValidationError("Migration from TimescaleDB to Postgres is not supported")

    caggs_count = 0
    if source_type == DBType.TIMESCALEDB:
        caggs_count = get_caggs_count(args.source)
        if caggs_count > 0:
            logger.info(f"Setting replica identity to FULL for {caggs_count} caggs ...")
            set_replica_identity_for_caggs(args.source, 'FULL')
            args.telemetry.progress(f"replica-idenitity-for-caggs:{caggs_count}")

    # reset endpos
    if args.resume:
        run_cmd("pgcopydb stream sentinel set endpos --dir $PGCOPYDB_DIR 0/0")
        args.telemetry.progress("reset sentinel on resume")

    if not is_section_migration_complete("roles") and not args.skip_roles:
        logger.info("Migrating roles from Source DB to Target DB ...")
        migrate_roles()
        mark_section_complete("roles")
        args.telemetry.progress("roles-migrated")

    if not is_section_migration_complete("initial-data-migration"):
        logger.info("Migrating existing data from Source DB to Target DB ...")
        match (source_type, target_type):
            case (DBType.POSTGRES, DBType.POSTGRES):
                migrate_existing_data(args=args, timescaledb=timescaledb)
            case (DBType.POSTGRES, DBType.TIMESCALEDB):
                migrate_existing_data_from_pg_to_tsdb(args)
            case (DBType.TIMESCALEDB, DBType.TIMESCALEDB):
                migrate_existing_data(args, timescaledb=timescaledb)
            case (DBType.TIMESCALEDB_SKIP_VERSION, DBType.TIMESCALEDB):
                migrate_existing_data_across_ts_versions(args)
            case (DBType.TIMESCALEDB, DBType.POSTGRES):
                raise ValidationError("Migration from TimescaleDB to Postgres is not supported")
        mark_section_complete("initial-data-migration")
        args.telemetry.progress("initial-data-migrated")

    housekeeping.start(args)
    target.convert_matview_to_view(args.target)
    args.telemetry.progress("patched-matviews")

    logger.info("Applying buffered transactions ...")
    run_cmd("pgcopydb stream sentinel set apply --dir $PGCOPYDB_DIR")
    args.telemetry.progress("apply-buffered-transactions")

    wait_for_DBs_to_sync(follow)
    args.telemetry.progress("db-synced")

    run_cmd("pgcopydb stream sentinel set endpos --dir $PGCOPYDB_DIR --current")
    args.telemetry.progress("stop live-replay")

    logger.info("Waiting for live-replay to complete ...")
    follow.wait()
    args.telemetry.progress("follow-completed")

    logger.info("Copying sequences ...")
    copy_sequences()
    args.telemetry.progress("sequences-copied")

    target.restore_matview(args.target)

    if source_type == DBType.TIMESCALEDB:
        logger.info("Enabling background jobs ...")
        timescaledb.enable_jobs()
        if caggs_count > 0:
            logger.info("Setting replica identity back to DEFAULT for caggs ...")
            set_replica_identity_for_caggs(args.source, 'DEFAULT')
            args.telemetry.progress("reset caggs replica identity")

def migrate(args):
    exit_code = 0
    follow = None
    try:
        args.telemetry.progress("begin")
        validate(args)
        args.telemetry.progress("validated")

        follow = create_follow(resume=args.resume)
        args.telemetry.progress("follow created")

        _migrate(args, follow)
    except KeyboardInterrupt:
        logger.info("Exiting ... (Ctrl+C)")
        args.telemetry.mark_interrupted()

    except ValidationError as e:
        message = textwrap.dedent(str(e))
        logger.error(message)
        args.telemetry.add_exception()
        exit_code = 1
    except Exception as e:
        logger.exception(e)
        args.telemetry.add_exception()
        exit_code = 1
    else:
        args.telemetry.mark_success()
        logger.info("Migration successfully completed.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean'))
        exit_code = 0

    finally:
        # TODO: Use daemon threads for housekeeping and health_checker.
        health_checker.stop_all()
        housekeeping.stop()
        # cleanup all subprocesses created by pgcopydb follow
        if follow:
            follow.terminate()

        if exit_code == 0:
            logger.info("All processes have exited successfully.")
        else:
            logger.error("An error occurred during the live migration. "
                         "Please report this issue to support@timescale.com "
                         "with all log files from the <volume-mount>/logs "
                         "directory.")
            logger.error("Exit code: %d", exit_code)

        return exit_code
