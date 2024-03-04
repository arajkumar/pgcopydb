#!/usr/bin/env python3
# This script orchestrates CDC based migration process using pgcopydb.

import argparse
import subprocess
import os
import time
import signal
import sys
import shutil
import tempfile
import threading
import uuid
import json

from enum import Enum
from pathlib import Path
from urllib.parse import urlparse
from time import perf_counter

from housekeeping import start_housekeeping
from health_check import health_checker

# If the process is not running on a TTY then reading from stdin will raise and
# exception. We read from stdin to start the switch-over phase. If it's not a
# TTY the switch-over can be started by sending a SIGUSR1 signal to the
# process.
IS_TTY = os.isatty(0)
DELAY_THRESHOLD = 30  # Megabytes.
SCRIPT_VERSION = "0.0.6"

env = os.environ.copy()

LIVE_MIGRATION_DOCKER = env.get('LIVE_MIGRATION_DOCKER') == 'true'

class DBType(Enum):
    POSTGRES = 1
    TIMESCALEDB = 2

class timeit:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.start
        print(f"=> Completed in {self.time:.1f}s")

class RedactedException(Exception):
    def __init__(self, err, redacted):
        super().__init__(str(err))
        self.err = err
        self.redacted = redacted


def print_logs_with_error(log_path: str = "", before: int = 0, after: int = 0, tail: int = 50):
    """
    Print error logs in the provided log_path along with tail
    of given number of log lines at all levels.
    """
    proc = subprocess.run(f"cat {log_path} | grep -i 'error\|warn' -A{after} -B{before}",
                                    shell=True,
                                    env=env,
                                    stderr=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    text=True)
    r = str(proc.stdout)
    if r != "":
        print(f"\n\n---------LOGS WITH ERROR FROM '{log_path}'---------")
        print(r)
        print("------------------END------------------")

    if tail > 0:
        proc = subprocess.run(f"tail -n {tail} {log_path}",
                                        shell=True,
                                        env=env,
                                        stderr=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        text=True)
        r = str(proc.stdout)
        if r != "":
            print(f"\n---------LAST {tail} LOG LINES FROM '{log_path}'---------")
            print(r)
            print("------------------END------------------")


def run_cmd(cmd: str, log_path: str = "", ignore_non_zero_code: bool = False) -> str:
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    if log_path != "":
        fname_stdout = f"{log_path}_stdout.log"
        stdout = open(fname_stdout, "w")
        fname_stderr = f"{log_path}_stderr.log"
        stderr = open(fname_stderr, "w")
    result = subprocess.run(cmd, shell=True, env=env, stderr=stderr, stdout=stdout, text=True)
    if result.returncode != 0 and not ignore_non_zero_code:
        if log_path != "":
            print_logs_with_error(log_path=f"{log_path}_stderr.log")
        cmd_name = cmd.split()[0]
        raise RedactedException(
            f"command '{cmd}' exited with {result.returncode} code. stderr={result.stderr}. stdout={result.stdout}",
            f"{cmd_name} exited with code {result.returncode}")
    return str(result.stdout)


def psql(uri: str, sql: str):
    return f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d "{uri}" -c " {sql} " """


def run_sql(execute_on_target: bool, sql: str):
    dest = "$PGCOPYDB_SOURCE_PGURI"
    if execute_on_target:
        dest = "$PGCOPYDB_TARGET_PGURI"
    return run_cmd(psql(dest, sql))


class TelemetryDBStats:
    def __init__(self, uri: str) -> None:
        num_hypertables_query = "select count(*) from timescaledb_information.hypertables"
        if not self.is_timescaledb(uri):
            num_hypertables_query = "select 0"
        query = f"""
select
    (select substring(current_setting('server_version') from '^[0-9\.]+')) as pg_version,
    (select extversion from pg_extension where extname='timescaledb') as ts_version,
    (select pg_database_size(current_database())) as size_approx,
    (select count(*) from pg_catalog.pg_tables where
        schemaname NOT LIKE 'pg_%' and schemaname not in (
            '_timescaledb_catalog', '_timescaledb_cache', '_timescaledb_config', '_timescaledb_internal', 'information_schema', 'timescaledb_experimental', 'timescaledb_information'
        )) as num_user_tables,
    ({num_hypertables_query}) as num_hypertables
"""
        result = run_cmd(psql(uri, query))[:-1]
        result = result.split("|")
        self.pg_version = result[0]
        self.ts_version = result[1]
        self.size_approx = int(result[2])
        self.num_user_tables = int(result[3])
        self.num_hypertables = int(result[4])

    def is_timescaledb(self, uri: str) -> bool:
        return run_cmd(psql(uri, "select exists(select 1 from pg_extension where extname='timescaledb')"))[:-1] == "t"

    def object(self) -> str:
        return {
            "pg_version": self.pg_version,
            "ts_version": self.ts_version,
            "db_size_approx": self.size_approx,
            "num_user_tables": self.num_user_tables,
            "num_hypertables": self.num_hypertables
        }


class Telemetry:
    tag = "migration"
    command = ""
    command_start = 0.0

    def __init__(self) -> None:
        source_db_stats = TelemetryDBStats(env["PGCOPYDB_SOURCE_PGURI"])
        self.migration = {
            "type": "migration",
            "start": time.time(),
            "success": False,
            "metadata": {
                "version": SCRIPT_VERSION,
                "method": "LIVE_MIGRATION",
                "migration_id": str(uuid.uuid4())
            },
            "command_by_duration_seconds": [],
            "total_duration_seconds": 0,
            "source_db_stats": source_db_stats.object(),
            "target_db_stats": {},
            "errors": []
        }

    def start_command(self, command: str):
        """
        starts the command and notes its start time.
        """
        if self.command != "" or self.command_start != 0.0:
            err = "previous command was not marked complete"
            self.register_runtime_error_and_write(err)
            raise ValueError(err)
        self.command = command
        self.command_start = time.time()

    def command_complete(self):
        """
        Marks the started command as complete and calculates the lapsed duration.
        """
        self.migration["command_by_duration_seconds"].append([self.command, int(time.time() - self.command_start)])
        self.command = ""
        self.command_start = 0.0

    def complete_fail(self):
        self.migration["success"] = False
        self.migration["total_duration_seconds"] = int(time.time() - self.migration["start"])
        self.write()

    def complete_success(self):
        target_db_stats = TelemetryDBStats(env["PGCOPYDB_TARGET_PGURI"])
        self.migration["success"] = True
        self.migration["total_duration_seconds"] = int(time.time() - self.migration["start"])
        self.migration["target_db_stats"] = target_db_stats.object()
        self.write()

    def register_runtime_error_and_write(self, err: str):
        target_db_stats = TelemetryDBStats(env["PGCOPYDB_TARGET_PGURI"])
        self.migration["target_db_stats"] = target_db_stats.object()
        self.migration["errors"].append(err)
        self.migration["success"] = False
        self.migration["total_duration_seconds"] = int(time.time() - self.migration["start"])
        self.write()

    def write(self):
        data = json.dumps(self.migration)
        sql = f"insert into _timescaledb_catalog.telemetry_event(tag, body) values('{self.tag}'::name, '{data}'::jsonb)"
        sql = sql.replace('"', '\\"')
        run_sql(execute_on_target=True, sql=sql)

telemetry = Telemetry()

def telemetry_command(title):
    def decorator(func):
        def wrapper_func(*args, **kwargs):
            telemetry.start_command(title)
            try:
                result = func(*args, **kwargs)
            except RedactedException as e:
                telemetry.register_runtime_error_and_write(e.redacted)
                raise e
            except ValueError as e:
                telemetry.register_runtime_error_and_write(e.__str__())
                raise e
            except Exception as e:
                telemetry.complete_fail()
                raise e
            else:
                telemetry.command_complete()
            return result
        return wrapper_func
    return decorator


class Command:
    def __init__(self, command: str = "", env=env, use_shell: bool = False, log_path: str = ""):
        self.command = command
        self.log_path = log_path
        if log_path == "":
            self.process = subprocess.Popen("exec " + self.command, shell=use_shell, env=env)
        else:
            fname_stdout = f"{log_path}_stdout.log"
            f_stdout = open(fname_stdout, "w")

            fname_stderr = f"{log_path}_stderr.log"
            f_stderr = open(fname_stderr, "w")

            self.process = subprocess.Popen("exec " + self.command, shell=use_shell, env=env,
                                            stdout=f_stdout, stderr=f_stderr,
                                            universal_newlines=True,
                                            bufsize=1)
        self.wait_thread = threading.Thread(target=self.process_wait)
        self.wait_thread.start()

    def process_wait(self):
        self.process.wait()
        code = self.process.returncode
        if code not in {-9, 0, 12}:
            # code -9 is for SIGKILL.
            # We ignore process execution that stops for SIGKILL since these are signals
            # given by self.terminate() and self.kill(). pgcopydb often requires these signals to stop.
            #
            # If we do not give these signals during the program execution (as and when required),
            # then pgcopydb hangs indefinitely (for some reason) even if it has completed its execution.
            #
            # code 12 is received when `pgcopydb follow`'s internal child processes exit due to an external signal.
            # We believe ignoring this code is safe.
            if self.log_path != "":
                print_logs_with_error(log_path=f"{self.log_path}_stderr.log")
            cmd_name = self.command.split()[0]
            raise RedactedException(
                f"command '{self.command}' exited with {self.process.returncode} code stderr={self.process.stderr}",
                f"{cmd_name} exited with code {self.process.returncode}")

    def wait(self):
        """
        wait waits for the process to complete with a zero code.
        """
        self.wait_thread.join()

    def terminate(self):
        self.process.terminate()

    def kill(self):
        self.process.kill()


def wait_for_keypress(event: threading.Event, key: str):
    """
    waits for the 'key' to be pressed. It returns a keypress_event queue
    that becomes non-empty when the required 'key' is pressed (and ENTER).
    """

    def key_e():
        while True:
            k = input()
            if k.lower() == key.lower():
                print(f'Received key press event: "{key}"')
                event.set()
                break

    key_event = threading.Thread(target=key_e)
    # we don't want the key_event thread to block the program from exiting.
    key_event.daemon = True
    key_event.start()


def wait_for_sigusr1(event: threading.Event):
    def handler(signum, frame):
        print("Received signal: SIGUSR1")
        event.set()
    signal.signal(signal.SIGUSR1, handler)


def pgcopydb_init_env(args):
    global env

    for key in ["PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"]:
        if key not in env or env[key] == "" or env[key] is None:
            raise ValueError(f"${key} not found")

    if args.dir:
        env["PGCOPYDB_DIR"] = str(args.dir.absolute())

def create_dirs(work_dir: Path):
    global env
    (work_dir / "logs").mkdir(parents=True, exist_ok=True)
    # This is needed because cleanup doesn't support --dir
    pgcopydb_dir = Path(tempfile.gettempdir()) / "pgcopydb"
    pgcopydb_dir.mkdir(parents=True, exist_ok=True)

@telemetry_command("check_source_is_timescaledb")
def get_dbtype(uri):
    result = run_cmd(psql(uri=uri, sql="select exists(select 1 from pg_extension where extname = 'timescaledb');"))
    if result == "t\n":
        return DBType.TIMESCALEDB
    return DBType.POSTGRES

@telemetry_command("check_timescaledb_version")
def check_timescaledb_version():
    result = run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select extversion from pg_extension where extname = 'timescaledb';"))
    source_ts_version = str(result)[:-1]

    result = run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI", sql="select extversion from pg_extension where extname = 'timescaledb';"))
    target_ts_version = str(result)[:-1]

    if source_ts_version != target_ts_version:
        raise ValueError(f"Source TimescaleDB version ({source_ts_version}) does not match Target TimescaleDB version ({target_ts_version})")

def replication_origin_exists():
    r = run_cmd(psql(uri="$PGCOPYDB_TARGET_PGURI",
                sql="select exists(select * from pg_replication_origin where roname='pgcopydb')"))
    return r == "t\n"

def is_snapshot_valid():
    try:
        run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="""
                     BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                     SET TRANSACTION SNAPSHOT '$(cat $PGCOPYDB_DIR/snapshot)';
                     SELECT 1;
                     ROLLBACK;
                     """))
        return True
    except:
        return False

def is_section_migration_complete(section):
    return os.path.exists(f"{env['PGCOPYDB_DIR']}/run/{section}-migration.done")

def mark_section_complete(section):
    Path(f"{env['PGCOPYDB_DIR']}/run/{section}-migration.done").touch()

@telemetry_command("cleanup")
def cleanup(args):
    print("Cleaning up ...")
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR")
    print("Done")

    if args.prune:
        dir = str(args.dir.absolute())
        print(f"Pruning {dir}...")
        shutil.rmtree(dir, ignore_errors=True)
        print("Done")

@telemetry_command("create_follow")
def create_follow(resume: bool = False):
    print(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}...")

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


def dbname_from_uri(uri: str) -> str:
    if "dbname=" in uri:
        parameters = uri.split()
        for param in parameters:
            key, value = param.split("=")
            if key == "dbname":
                return value
    else:
        # Input => uri: 'postgres://tsdb:abcd@a.timescaledb.io:26479/rbac_test?sslmode=require'
        # result.path[1]: '/rbac_test'
        result = urlparse(uri)
        return result.path[1:]


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


@telemetry_command("migrate_existing_data_from_pg")
def migrate_existing_data_from_pg(target_type: DBType):
    # TODO: Switch to the following simplified commands once we
    # figure out how to deal with incompatible indexes.
    # pgcopydb dump schema
    # pgcopydb restore pre-data --skip-extensions --no-acl --no-owner
    # <-- create hypertables -->
    # pgcopydb clone --skip-extensions --no-acl --no-owner
    if not is_section_migration_complete("pre-data-dump"):
        print(f"Creating pre-data dump at {env['PGCOPYDB_DIR']}/dump ...")
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
        print(f"Creating post-data dump at {env['PGCOPYDB_DIR']}/dump ...")
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
        print("Restoring pre-data ...")
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

    if not is_section_migration_complete("copy-table-data"):
        print("Copying table data ...")
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
        print("Restoring post-data ...")
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
        print("Perform ANALYZE on target DB tables ...")
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

@telemetry_command("migrate_extensions")
def migrate_extensions(target_type):
    if target_type == DBType.TIMESCALEDB:
        print("Timescale pre-restore ...")
        run_sql(execute_on_target=True, sql="select timescaledb_pre_restore();")

    print("Copy extension config tables ...")
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

    if target_type == DBType.TIMESCALEDB:
        print("Timescale post-restore ...")
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

@telemetry_command("migrate_roles")
def migrate_roles():
    print(f"Dumping roles to {env['PGCOPYDB_DIR']}/roles.sql ...")
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

        restore_roles = f"""psql -X -d $PGCOPYDB_TARGET_PGURI -v ON_ERROR_STOP=1 --echo-errors -f {roles_file_path}"""
        run_cmd(restore_roles)


@telemetry_command("migrate_existing_data_from_ts")
def migrate_existing_data_from_ts():
    print("Copying table data ...")
    clone_table_data = " ".join([
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
        "--resume",
        "--notice",
        ])

    with timeit():
       run_cmd(clone_table_data, f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_clone")

def wait_for_event(key):
    event = threading.Event()
    wait_for_sigusr1(event)
    if IS_TTY:
        wait_for_keypress(event, key)

    return event

@telemetry_command("wait_for_DBs_to_sync")
def wait_for_DBs_to_sync(follow_proc):
    def get_delay():
        delay_bytes = int(run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) delay from pgcopydb.sentinel;")))
        return int(delay_bytes / (1024 * 1024))

    event = wait_for_event("c")
    while not event.is_set() and follow_proc.process.poll() is None:
        delay_mb = get_delay()
        diff = f"[WATCH] Source DB - Target DB => {delay_mb}MB"
        if not IS_TTY and LIVE_MIGRATION_DOCKER:
            print(f'{diff}. To proceed, send a SIGUSR1 signal with "docker kill -s=SIGUSR1 <container_name>"')
        elif not IS_TTY:
            print(f'{diff}. To proceed, send a SIGUSR1 signal with "kill -s=SIGUSR1 {os.getpid()}"')
        else:
            print(f'{diff}. Press "c" (and ENTER) to proceed')

        event.wait(timeout=5)

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

def migrate(args):
    # Clean up pid files. This might cause issues in docker environment due
    # deterministic pid values.
    (args.dir / "pgcopydb.pid").unlink(missing_ok=True)

    if not (args.dir / "snapshot").exists():
        print("You must create a snapshot before starting the migration.")
        print()
        print("Run the following command to create a snapshot:")
        print(docker_command('live-migration-snapshot', 'snapshot'))
        sys.exit(1)

    # check whether the snapshot is valid if initial data migration
    # is not yet complete.
    if not is_section_migration_complete("initial-data-migration") and not is_snapshot_valid():
            print("Invalid snapshot found. Snapshot process might have died or failed.")
            print("Please restart the migration process.")
            print()
            print("Run the following command to clean the existing resources:")
            print(docker_command('live-migration-clean', 'clean', '--prune'))
            print()
            print("Run the following command to create a new snapshot:")
            print(docker_command('live-migration-snapshot', 'snapshot'))
            sys.exit(1)

    # resume but no previous migration found
    if not replication_origin_exists() and args.resume:
        print("No resumable migration found.")
        print()
        print("If you want to start the migration:")
        print(docker_command('live-migration-migrate', 'migrate'))
        sys.exit(1)

    # if replication origin exists, then the previous migration was incomplete
    if replication_origin_exists() and not args.resume:
        print("Found an incomplete migration.")
        print()
        print("If you want to resume the migration:")
        print(docker_command('live-migration-migrate', 'migrate', '--resume'))
        print()
        print("If you want to start a new migration, you should clean the existing resources:")
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
            print("Migrating from Postgres to Postgres ...")
        case (DBType.POSTGRES, DBType.TIMESCALEDB):
            print("Migrating from Postgres to TimescaleDB ...")
        case (DBType.TIMESCALEDB, DBType.TIMESCALEDB):
            print("Migrating from TimescaleDB to TimescaleDB ...")
            check_timescaledb_version()
        case (DBType.TIMESCALEDB, DBType.POSTGRES):
            print("Migration from TimescaleDB to Postgres is not supported")
            sys.exit(1)

    caggs_count = 0
    if source_type == DBType.TIMESCALEDB:
        caggs_count = get_caggs_count()
        if caggs_count > 0:
            print(f"Setting replica identity to FULL for {caggs_count} caggs ...")
            set_replica_identity_for_caggs('FULL')

    # reset endpos
    if args.resume:
        run_cmd("pgcopydb stream sentinel set endpos 0/0")

    housekeeping_thread, housekeeping_stop_event = None, None
    follow_proc = create_follow(resume=args.resume)
    try:
        if not args.skip_initial_data:
            if not is_section_migration_complete("roles"):
                print("Migrating roles from Source DB to Target DB ...")
                migrate_roles()
                mark_section_complete("roles")

            if not is_section_migration_complete("extensions"):
                print("Migrating extensions from Source DB to Target DB ...")
                migrate_extensions(target_type)
                mark_section_complete("extensions")

            if not is_section_migration_complete("initial-data-migration"):
                print("Migrating existing data from Source DB to Target DB ...")
                if source_type == DBType.POSTGRES:
                    migrate_existing_data_from_pg(target_type)
                else:
                    migrate_existing_data_from_ts()
                mark_section_complete("initial-data-migration")

        (housekeeping_thread, housekeeping_stop_event) = start_housekeeping(env)

        print("Applying buffered transactions ...")
        run_cmd("pgcopydb stream sentinel set apply")

        wait_for_DBs_to_sync(follow_proc)

        run_cmd("pgcopydb stream sentinel set endpos --current")

        print("Waiting for live-replay to complete ...")
        # TODO: Implement retry for follow.
        follow_proc.wait()
        follow_proc = None

        print("Copying sequences ...")
        copy_sequences()

        if source_type == DBType.TIMESCALEDB:
            print("Enabling background jobs ...")
            enable_user_background_jobs()
            if caggs_count > 0:
                print("Setting replica identity back to DEFAULT for caggs ...")
                set_replica_identity_for_caggs('DEFAULT')

    except KeyboardInterrupt:
        print("Exiting ... (Ctrl+C)")
    except Exception as e:
        print("Unexpected exception:", e)
        telemetry.complete_fail()
    else:
        print("Migration successfully completed.")
        print()
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
        print("All processes have exited successfully.")

def main():
    parser = argparse.ArgumentParser(description='''Live migration moves your PostgreSQL/TimescaleDB to Timescale Cloud with minimal downtime.''', add_help=False)
    parser.add_argument('-h', '--help', action='help',
                        help='Show this help message and exit')

    parser.add_argument('-v', '--version', action='version',
                        version=SCRIPT_VERSION,
                        help='Show the version of live-migration tool')

    subparsers = parser.add_subparsers(dest='command',
                                       help='Subcommand help',
                                       title='Subcommands')

    common = argparse.ArgumentParser(add_help=False)
    def _dir_default():
        dir = os.environ.get('PGCOPYDB_DIR', '')
        if not dir:
            dir = f'{tempfile.gettempdir()}/pgcopydb'
        return Path(dir)
    common.add_argument('-h', '--help', action='help',
                        help='Show this help message and exit')

    common.add_argument('--dir', type=Path,
                        help='Working directory',
                        default=_dir_default())

    # snapshot
    parser_snapshot = subparsers.add_parser('snapshot',
                                            help='Create a snapshot',
                                            parents=[common],
                                            add_help=False)
    parser_snapshot.add_argument('--plugin', type=str,
                                 help='Output plugin (Default: wal2json)',
                                 default='wal2json',
                                 choices=['wal2json', 'test_decoding'])

    parser_clean = subparsers.add_parser('clean', help='Clean up resources',
                                         parents=[common],
                                         add_help=False)
    parser_clean.add_argument('--prune', action='store_true', help='Prune the working directory')

    # migrate
    parser_migrate = subparsers.add_parser('migrate',
                                           help='Start the migration',
                                           parents=[common],
                                           add_help=False)
    parser_migrate.add_argument('--resume', action='store_true', help='Resume the migration')
    # internal: for testing purposes only
    parser_migrate.add_argument('--pg-src',
                                action='store_true',
                                help=argparse.SUPPRESS,
                                default=(os.environ.get('POSTGRES_SOURCE') == "true"))

    # internal: for testing purposes only
    parser_migrate.add_argument('--pg-target',
                                action='store_true',
                                help=argparse.SUPPRESS,
                                default=(os.environ.get('POSTGRES_TARGET') == "true"))
    # TODO: Remove this once we know the existing customers who are still
    # using the old migration image < v0.0.5.
    parser_migrate.add_argument('--skip-initial-data',
                                action='store_true',
                                help=('Skip initial data migration. '
                                      'This is provided for backward '
                                      'compatibility with the old migration '
                                      'process which did not support granular '
                                      'resume over the section.'))

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    pgcopydb_init_env(args)
    create_dirs(args.dir)

    match args.command:
        case 'snapshot':
            snapshot(args)
        case 'clean':
            clean(args)
        case 'migrate':
            migrate(args)

def docker_command(name, *args):
    if LIVE_MIGRATION_DOCKER:
        return rf"""
    docker run -it --rm --name {name} \
    -e PGCOPYDB_SOURCE_PGURI=$SOURCE \
    -e PGCOPYDB_TARGET_PGURI=$TARGET \
    -v ~/live-migration:{env['PGCOPYDB_DIR']} \
    timescale/live-migration:v{SCRIPT_VERSION} \
    {" ".join(args)}
""".rstrip()
    else:
        return sys.argv[0]

def snapshot(args):
    if (args.dir / "snapshot").exists():
        print("Snapshot file already exists.")
        print("Snapshot process is either running or not cleaned up properly.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(1)

    print("Creating snapshot ...")
    # Clean up pid files. This might cause issues in docker environment due
    # deterministic pid values.
    (args.dir / "pgcopydb.snapshot.pid").unlink(missing_ok=True)

    dir = str(args.dir.absolute())
    snapshot_command = [
        "pgcopydb",
        "snapshot",
        "--follow",
        "--plugin",
        args.plugin,
        "--dir",
        dir,
    ]

    process = subprocess.Popen(snapshot_command,
                               stdout=subprocess.PIPE,
                               text=True)
    snapshot_id = ''
    while process.poll() is None and snapshot_id == '':
        snapshot_id = process.stdout.readline().strip()

    if snapshot_id != '':
        print(f"Snapshot {snapshot_id} created successfully.")
        print()
        print("You can now start the migration process by running the following command:")
        print(docker_command("live-migration-migrate", "migrate"))

        # TODO: Terminate the snapshot once the migration switches to
        # live replication.
        try:
            process.wait()
        except KeyboardInterrupt:
            process.terminate()
            process.wait()
    else:
        print("Snapshot creation failed.")
        print("You may need to cleanup and retry the snapshot creation.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(process.returncode)

def clean(args):
    cleanup(args)

if __name__ == "__main__":
    main()
