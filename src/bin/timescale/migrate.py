#!/usr/bin/env python3
# This script orchestrates CDC based migration process using pgcopydb.

import os
import signal
import sys
import threading
import logging

from enum import Enum
from pathlib import Path

from housekeeping import start_housekeeping
from health_check import health_checker
from utils import timeit, print_logs_with_error, docker_command, dbname_from_uri, store_val, get_stored_val
from environ import LIVE_MIGRATION_DOCKER, env
from telemetry import telemetry_command, telemetry
from usr_signal import wait_for_event, IS_TTY
from exec import Command, run_cmd, run_sql, psql
from utils import bytes_to_human, seconds_to_human

logger = logging.getLogger(__name__)

REPLICATION_LAG_THRESHOLD_BYTES = 512000  # 500KiB.

class DBType(Enum):
    POSTGRES = 1
    TIMESCALEDB = 2

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

@telemetry_command("check_source_is_timescaledb")
def get_dbtype(uri):
    result = run_cmd(psql(uri=uri, sql="select exists(select 1 from pg_extension where extname = 'timescaledb');"))
    if result == "t\n":
        return DBType.TIMESCALEDB
    return DBType.POSTGRES


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

    log_path = f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_follow"
    follow_proc = Command(command=follow_command, use_shell=True, log_path=log_path)
    def is_error_func_for_follow(log_line: str):
        if "pgcopydb.sentinel" in log_line:
            return False
        if "ERROR" in log_line or "free(): double free detected" in log_line:
            return True
        return False
    health_checker.check_log_for_health("follow", f"{log_path}_stderr.log", is_error_func_for_follow)

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


@telemetry_command("migrate_existing_data_from_pg")
def migrate_existing_data_from_pg(target_type: DBType):
    # TODO: Switch to the following simplified commands once we
    # figure out how to deal with incompatible indexes.
    # pgcopydb dump schema
    # pgcopydb restore pre-data --skip-extensions --no-acl --no-owner
    # <-- create hypertables -->
    # pgcopydb clone --skip-extensions --no-acl --no-owner
    if not is_section_migration_complete("pre-data-dump"):
        logger.info(f"Creating pre-data dump at {env['PGCOPYDB_DIR']}/dump ...")
        with timeit():
            pgdump_command = " ".join(["pg_dump",
                                       "-d",
                                       "$PGCOPYDB_SOURCE_PGURI",
                                       "--format=plain",
                                       "--quote-all-identifiers",
                                       "--no-tablespaces",
                                       "--no-owner",
                                       "--no-privileges",
                                       "--section=pre-data",
                                       "--file=$PGCOPYDB_DIR/pre-data-dump.sql",
                                       ])
            run_cmd(pgdump_command, f"{env['PGCOPYDB_DIR']}/logs/pre_data_dump")
        mark_section_complete("pre-data-dump")

    if not is_section_migration_complete("post-data-dump"):
        logger.info(f"Creating post-data dump at {env['PGCOPYDB_DIR']}/dump ...")
        with timeit():
            pgdump_command = " ".join(["pg_dump",
                                       "-d",
                                       "$PGCOPYDB_SOURCE_PGURI",
                                       "--format=plain",
                                       "--quote-all-identifiers",
                                       "--no-tablespaces",
                                       "--no-owner",
                                       "--no-privileges",
                                       "--section=post-data",
                                       "--file=$PGCOPYDB_DIR/post-data-dump.sql",
                                       ])
            run_cmd(pgdump_command, f"{env['PGCOPYDB_DIR']}/logs/post_data_dump")
        mark_section_complete("post-data-dump")

    if not is_section_migration_complete("pre-data-restore"):
        logger.info("Restoring pre-data ...")
        with timeit():
            log_path = f"{env['PGCOPYDB_DIR']}/logs/pre_data_restore"
            psql_command = " ".join(["psql",
                                     "-X",
                                     "-d",
                                     "$PGCOPYDB_TARGET_PGURI",
                                     "--echo-errors",
                                     "-v",
                                     "ON_ERROR_STOP=0",
                                     "-f",
                                     "$PGCOPYDB_DIR/pre-data-dump.sql",
                                     ])
            run_cmd(psql_command, f"{env['PGCOPYDB_DIR']}/logs/pre_data_restore")
            print_logs_with_error(log_path=f"{log_path}_stderr.log", after=3, tail=0)
        mark_section_complete("pre-data-restore")

    if (target_type == DBType.TIMESCALEDB and
        not is_section_migration_complete("hypertable-creation")):
        show_hypertable_creation_prompt()
        mark_section_complete("hypertable-creation")

    stop_progress = monitor_db_sizes()

    if not is_section_migration_complete("copy-table-data"):
        logger.info("Copying table data ...")
        copy_table_data = " ".join(["pgcopydb",
                                 "copy",
                                 "table-data",
                                 "--table-jobs",
                                 "8",
                                 "--split-tables-larger-than='1 GB'",
                                 "--notice",
                                 "--skip-vacuum",
                                 "--dir",
                                 "$PGCOPYDB_DIR/copy_table_data",
                                 "--snapshot",
                                 "$(cat $PGCOPYDB_DIR/snapshot)",
                                 ])
        with timeit():
            run_cmd(copy_table_data, f"{env['PGCOPYDB_DIR']}/logs/copy_table_data")
        mark_section_complete("copy-table-data")

    if not is_section_migration_complete("post-data-restore"):
        logger.info("Restoring post-data ...")
        with timeit():
            log_path = f"{env['PGCOPYDB_DIR']}/logs/post_data_restore"
            psql_command = " ".join(["psql",
                                     "-X",
                                     "-d",
                                     "$PGCOPYDB_TARGET_PGURI",
                                     "--echo-errors",
                                     "-v",
                                     "ON_ERROR_STOP=0",
                                     "-f",
                                     "$PGCOPYDB_DIR/post-data-dump.sql",
                                     ])
            run_cmd(psql_command, log_path)
            print_logs_with_error(log_path=f"{log_path}_stderr.log", after=3, tail=0)
        mark_section_complete("post-data-restore")

    if not is_section_migration_complete("analyze-db"):
        logger.info("Perform ANALYZE on target DB tables ...")
        with timeit():
            vaccumdb_command = " ".join(["vacuumdb",
                                        # There won't be anything to
                                        # vacuum after a fresh restore.
                                        "--analyze-only",
                                        "--analyze-in-stages",
                                        "--echo",
                                        "--dbname",
                                        "$PGCOPYDB_TARGET_PGURI",
                                        "--jobs",
                                        "8",
                                        ])
            run_cmd(vaccumdb_command, f"{env['PGCOPYDB_DIR']}/logs/analyze_db")
        mark_section_complete("analyze-db")
    stop_progress.set()


@telemetry_command("migrate_extensions")
def migrate_extensions():
    logger.info("Copy extension config tables ...")
    with timeit():
        copy_extensions = " ".join([
            "pgcopydb",
            "copy",
            "extensions",
            "--dir",
            "$PGCOPYDB_DIR/copy_extensions",
            "--snapshot",
            "$(cat $PGCOPYDB_DIR/snapshot)",
            "--resume",
            ])
        run_cmd(copy_extensions, f"{env['PGCOPYDB_DIR']}/logs/copy_extensions")


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
            "$PGCOPYDB_SOURCE_PGURI",
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
-e '/TO "tsdbadmin/d' \
-e '/CREATE ROLE "_aiven";/d' \
-e '/ALTER ROLE "_aiven"/d' \
{roles_file_path}"""
        run_cmd(filter_stmts)

        restore_roles_cmd = [
            "psql",
             "-X",
             "-d",
             "$PGCOPYDB_TARGET_PGURI",
             "-v",
             # Attempt whatever tsdbadmin can do, but don't fail if it fails.
             "ON_ERROR_STOP=0",
             "--echo-errors",
             "-f",
             roles_file_path,
        ]

        restore_roles_cmd = " ".join(restore_roles_cmd)
        log_path = f"{env['PGCOPYDB_DIR']}/logs/restore_roles"

        run_cmd(restore_roles_cmd, log_path)

        print_logs_with_error(log_path=f"{log_path}_stderr.log", after=3, tail=0)


@telemetry_command("migrate_existing_data_from_ts")
def migrate_existing_data_from_ts(args):
    logger.info("Copying table data ...")
    clone_table_data = [
        "pgcopydb",
        "clone",
        "--skip-extensions",
        "--no-acl",
        "--no-owner",
        "--table-jobs=8",
        "--index-jobs=8",
        "--split-tables-larger-than='1 GB'",
        "--dir",
        "$PGCOPYDB_DIR/pgcopydb_clone",
        "--snapshot",
        "$(cat $PGCOPYDB_DIR/snapshot)",
        "--notice",
        ]

    if args.resume:
        clone_table_data.append("--resume")

    clone_table_data = " ".join(clone_table_data)

    stop_progress = monitor_db_sizes()

    with timeit():
       run_cmd(clone_table_data, f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_clone")

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
    run_cmd("pgcopydb copy sequences --resume --not-consistent", f"{env['PGCOPYDB_DIR']}/logs/copy_sequences")

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

    source_pg_uri = env["PGCOPYDB_SOURCE_PGURI"]
    target_pg_uri = env["PGCOPYDB_TARGET_PGURI"]

    source_type = get_dbtype(source_pg_uri)
    target_type = get_dbtype(target_pg_uri)

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
        run_cmd("pgcopydb stream sentinel set endpos 0/0")

    housekeeping_thread, housekeeping_stop_event = None, None
    follow_proc = create_follow(resume=args.resume)
    try:
        if not is_section_migration_complete("roles") and not args.skip_roles:
            logger.info("Migrating roles from Source DB to Target DB ...")
            migrate_roles()
            mark_section_complete("roles")

        if not args.skip_initial_data:
            handle_timescaledb_extension = (source_type == DBType.TIMESCALEDB and target_type == DBType.TIMESCALEDB)

            if not is_section_migration_complete("extensions"):
                if handle_timescaledb_extension:
                    timescaledb_pre_restore()
                logger.info("Migrating extensions from Source DB to Target DB ...")
                migrate_extensions()
                mark_section_complete("extensions")

            if not is_section_migration_complete("initial-data-migration"):
                logger.info("Migrating existing data from Source DB to Target DB ...")
                if source_type == DBType.POSTGRES:
                    migrate_existing_data_from_pg(target_type)
                else:
                    migrate_existing_data_from_ts(args)
                mark_section_complete("initial-data-migration")

            if (handle_timescaledb_extension and
               not is_section_migration_complete("timescaledb-post-restore")):
                # Note: timescaledb_post_restore must come after post-data, otherwise there
                # are issues restoring indexes and foreign key constraints to chunks.
                timescaledb_post_restore()
                mark_section_complete("timescaledb-post-restore")

        (housekeeping_thread, housekeeping_stop_event) = start_housekeeping(env)

        logger.info("Applying buffered transactions ...")
        run_cmd("pgcopydb stream sentinel set apply")

        wait_for_DBs_to_sync(follow_proc)

        run_cmd("pgcopydb stream sentinel set endpos --current")

        logger.info("Waiting for live-replay to complete ...")
        # TODO: Implement retry for follow.
        follow_proc.wait()
        follow_proc = None

        logger.info("Copying sequences ...")
        copy_sequences()

        if source_type == DBType.TIMESCALEDB:
            logger.info("Enabling background jobs ...")
            enable_user_background_jobs()
            if caggs_count > 0:
                logger.info("Setting replica identity back to DEFAULT for caggs ...")
                set_replica_identity_for_caggs('DEFAULT')

    except KeyboardInterrupt:
        logger.info("Exiting ... (Ctrl+C)")
    except Exception as e:
        logger.info(f"Unexpected exception: {e}")
        telemetry.complete_fail()
    else:
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
