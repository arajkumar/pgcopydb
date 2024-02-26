#!/usr/bin/env python3
# This script orchestrates CDC based migration process using pgcopydb.

import subprocess
import os
import time
import signal
import sys
import shutil
import tempfile
import threading
import queue
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
    run_cmd(psql(dest, sql))


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
                                            stdout=f_stdout, stderr=f_stderr)
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


def wait_for_keypress(notify_queue, key: str) -> queue.Queue:
    """
    waits for the 'key' to be pressed. It returns a keypress_event queue
    that becomes non-empty when the required 'key' is pressed (and ENTER).
    """

    def key_e():
        while True:
            k = input()
            if k.lower() == key.lower():
                print(f'Received key press event: "{key}"')
                notify_queue.put(1)
                notify_queue.task_done()
                return
    key_event = threading.Thread(target=key_e)
    key_event.start()


def wait_for_sigusr1(notify_queue) -> queue.Queue:

    def handler(signum, frame):
        print("Received signal")
        notify_queue.put(1)
        notify_queue.task_done()
    signal.signal(signal.SIGUSR1, handler)


def install_signal_handler():
    def handler(signum, frame):
        print("Received signal")
        run_cmd("killall -SIGINT pgcopydb", ignore_non_zero_code=True)
        # 130 is the exit code for SIGINT
        # https://tldp.org/LDP/abs/html/exitcodes.html
        sys.exit(130)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

def pgcopydb_init_env(source_type: DBType):
    global env

    work_dir = os.getenv("PGCOPYDB_DIR")
    if work_dir is not None:
        work_dir = Path(work_dir)
    else:
        work_dir = Path(tempfile.gettempdir()) / "pgcopydb"

    for key in ["PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"]:
        if key not in env or env[key] == "" or env[key] is None:
            raise ValueError(f"${key} not found")

    env["PGCOPYDB_DIR"] = str(work_dir.absolute())

    if "PGCOPYDB_OUTPUT_PLUGIN" not in env:
        env["PGCOPYDB_OUTPUT_PLUGIN"] = "wal2json"

    return work_dir

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

def source_has_incomplete_migration():
    r = run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI",
                sql="select exists(select 1 from pg_replication_slots where slot_name = 'pgcopydb');"))
    return r == "t\n"

def is_initial_data_copy_complete():
    return os.path.exists(f"{env['PGCOPYDB_DIR']}/run/restore-post.done")

def mark_initial_data_copy_complete():
    Path(f"{env['PGCOPYDB_DIR']}/run/restore-post.done").touch()

@telemetry_command("cleanup")
def cleanup():
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR --slot-name pgcopydb")

    shutil.rmtree(str(env["PGCOPYDB_DIR"]), ignore_errors=True)


@telemetry_command("create_snapshot_and_follow")
def create_snapshot_and_follow(resume: bool = False):
    snapshot_proc = None
    # Previous run would have already completed the initial data copy
    # hence creation of snapshot is not needed.
    if not resume:
        print("Creating snapshot ...")
        snapshot_command = "pgcopydb snapshot --follow --dir $PGCOPYDB_DIR"
        log_path = f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_snapshot"
        snapshot_proc = Command(command=snapshot_command, use_shell=True, log_path=log_path)
        def is_error_func_for_snapshot(log_line: str):
            if any(word in log_line.lower() for word in ["error", "failed"]):
                return True
            return False
        health_checker.check_log_for_health("snapshot", f"{log_path}_stderr.log", is_error_func_for_snapshot)
        time.sleep(10)

    print(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}/cdc using {env['PGCOPYDB_OUTPUT_PLUGIN']} ...")

    follow_command = f"pgcopydb follow --dir $PGCOPYDB_DIR --snapshot $(cat {env['PGCOPYDB_DIR']}/snapshot)"
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
    return (snapshot_proc, follow_proc)


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


@telemetry_command("migrate_existing_data_from_pg")
def migrate_existing_data_from_pg(target_type: DBType):
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
                                   "--snapshot=$(cat ${PGCOPYDB_DIR}/snapshot)",
                                   ])
        run_cmd(pgdump_command, f"{env['PGCOPYDB_DIR']}/logs/pre_data_dump")

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
                                   "--snapshot=$(cat ${PGCOPYDB_DIR}/snapshot)",
                                   ])
        run_cmd(pgdump_command, f"{env['PGCOPYDB_DIR']}/logs/post_data_dump")

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
        run_cmd(psql_command, log_path)
        print_logs_with_error(log_path=f"{log_path}_stderr.log", after=3, tail=0)

    if target_type == DBType.TIMESCALEDB:
        create_hypertable_message = """Now, let's transform your regular tables into hypertables.

Execute the following command for each table you want to convert on the target DB:
    `SELECT create_hypertable('<table name>', '<time column name>', chunk_time_interval => INTERVAL '<chunk interval>');`

    Refer to https://docs.timescale.com/use-timescale/latest/hypertables/create/#create-hypertables for more details.

Once you are done"""
        key_event_queue = queue.Queue(maxsize=1)
        wait_for_sigusr1(key_event_queue)
        if IS_TTY:
            wait_for_keypress(key_event_queue, key="c")
        if not IS_TTY and LIVE_MIGRATION_DOCKER:
            print(f"{create_hypertable_message}, send a SIGUSR1 signal with 'docker kill --s=SIGUSR1 <container_name>' to continue")
        elif not IS_TTY:
            print(f"{create_hypertable_message}, send a SIGUSR1 signal with 'kill -s=SIGUSR1 {os.getpid()}' to continue")
        else:
            print(f"{create_hypertable_message}, press 'c' (and ENTER) to continue")
        key_event_queue.get()

    copy_table_data = " ".join(["pgcopydb",
                             "copy",
                             "table-data",
                             "--snapshot",
                             "$(cat ${PGCOPYDB_DIR}/snapshot)",
                             "--table-jobs",
                             "8",
                             "--split-tables-larger-than='10 GB'",
                             "--restart",
                             "--dir",
                             "$PGCOPYDB_DIR/copy_table_data",
                             ])
    print("Copying table data ...")
    with timeit():
        run_cmd(copy_table_data, f"{env['PGCOPYDB_DIR']}/logs/copy_table_data")

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
    print("Timescale pre-restore ...")
    run_sql(execute_on_target=True, sql="select timescaledb_pre_restore();")

    with timeit():
        copy_extensions = " ".join([
                "pgcopydb",
                "copy",
                "extensions",
                "--dir",
                "$PGCOPYDB_DIR/copy_extension",
                "--snapshot",
                "$(cat ${PGCOPYDB_DIR}/snapshot)",
                "--restart",
                ])
        run_cmd(copy_extensions, f"{env['PGCOPYDB_DIR']}/logs/copy_extensions")

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

    print("Copying table data ...")
    clone_table_data = " ".join([
        "pgcopydb",
        "clone",
        "--skip-extensions",
        # todo: remove this once we remove explit vacuum calls
        "--skip-vacuum",
        "--no-acl",
        "--no-owner",
        "--drop-if-exists",
        "--table-jobs=8",
        "--index-jobs=8",
        "--split-tables-larger-than='10 GB'",
        "--dir",
        "$PGCOPYDB_DIR/pgcopydb_clone",
        "--restart",
        "--snapshot",
        "$(cat ${PGCOPYDB_DIR}/snapshot)",
        "--notice",
        ])

    with timeit():
       run_cmd(clone_table_data, f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_clone")

@telemetry_command("wait_for_DBs_to_sync")
def wait_for_DBs_to_sync():
    def get_delay():
        delay_bytes = int(run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) delay from pgcopydb.sentinel;")))
        return int(delay_bytes / (1024 * 1024))

    analyzed = False
    analyze_cmd = 'psql -A -t -d $PGCOPYDB_TARGET_PGURI -c " analyze verbose; " '
    key_event_queue = queue.Queue(maxsize=1)
    wait_for_sigusr1(key_event_queue)
    if IS_TTY:
        wait_for_keypress(key_event_queue, key="c")

    while True:
        delay_mb = get_delay()
        diff = f"[WATCH] Source DB - Target DB => {delay_mb}MB"
        if not analyzed:
            print(diff)
        else:
            if not IS_TTY and LIVE_MIGRATION_DOCKER:
                print(f'{diff}. To proceed, send a SIGUSR1 signal with "docker kill -s=SIGUSR1 <container_name>"')
            elif not IS_TTY:
                print(f'{diff}. To proceed, send a SIGUSR1 signal with "kill -s=SIGUSR1 {os.getpid()}"')
            else:
                print(f'{diff}. Press "c" (and ENTER) to proceed')
        if not analyzed and delay_mb < DELAY_THRESHOLD:
            print("Starting analyze on Target DB. This might take a long time to complete ...")
            run_cmd(analyze_cmd, log_path=f"{env['PGCOPYDB_DIR']}/logs/target_analyze")
            print("Analyze complete")
            analyzed = True
            continue
        if not key_event_queue.empty():
            break
        time.sleep(10)

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

def cleanup_pid_files(work_dir: Path, source_type: DBType):
    global env

    print("Cleaning up pid files ...")

    (work_dir / "pgcopydb.snapshot.pid").unlink(missing_ok=True)
    (work_dir / "pgcopydb.pid").unlink(missing_ok=True)

if __name__ == "__main__":
    install_signal_handler()

    # The following env vars forces TimescaleDB to be treated as Postgres.
    # It is useful for testing purposes.
    force_postgres_source = os.getenv("POSTGRES_SOURCE") == "true"
    force_postgres_target = os.getenv("POSTGRES_TARGET") == "true"

    source_pg_uri = env["PGCOPYDB_SOURCE_PGURI"]
    target_pg_uri = env["PGCOPYDB_TARGET_PGURI"]

    source_type = get_dbtype(source_pg_uri)
    target_type = get_dbtype(target_pg_uri)

    if force_postgres_source:
        source_type = DBType.POSTGRES
    if force_postgres_target:
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

    work_dir = pgcopydb_init_env(source_type)

    skip_initial_data_copy = False

    caggs_count = 0
    if source_type == DBType.TIMESCALEDB:
        caggs_count = get_caggs_count()
        if caggs_count > 0:
            print(f"Setting replica identity to FULL for {caggs_count} caggs ...")
            set_replica_identity_for_caggs('FULL')

    if source_has_incomplete_migration():
        if is_initial_data_copy_complete():
            skip_initial_data_copy = True
            run_cmd("killall -SIGINT pgcopydb", ignore_non_zero_code=True)

            # this is needed for docker environment, where the process
            # always gets deterministic pid while starting.
            cleanup_pid_files(work_dir, source_type)
            print("Resuming migration ...")
        else:
            print("Cleaning up incomplete migration artifacts ...")
            run_cmd("killall -SIGINT pgcopydb", ignore_non_zero_code=True)
            cleanup()
            shutil.rmtree(str(work_dir), ignore_errors=True)
            print("Cleanup the target DB and attempt migration again")
            sys.exit(1)
    else:
        shutil.rmtree(str(work_dir), ignore_errors=True)

    create_dirs(work_dir)

    snapshot_proc, follow_proc = create_snapshot_and_follow(resume=skip_initial_data_copy)

    if skip_initial_data_copy:
        print("Skipping roles & initial data migration during resume ...")
    else:
        print("Migrating roles from Source DB to Target DB ...")
        migrate_roles()

        print("Migrating existing data from Source DB to Target DB ...")

        if source_type == DBType.POSTGRES:
            migrate_existing_data_from_pg(target_type)
        else:
            migrate_existing_data_from_ts()

        mark_initial_data_copy_complete()

        snapshot_proc.terminate()

    (housekeeping_thread, housekeeping_stop_event) = start_housekeeping(env)

    print("Applying buffered transactions ...")
    run_cmd("pgcopydb stream sentinel set apply")

    wait_for_DBs_to_sync()

    print("Stopping live-replay ...")
    run_cmd("pgcopydb stream sentinel set endpos --current")

    print("Waiting for live-replay to complete ...")
    follow_proc.wait()

    print("Copying sequences ...")
    copy_sequences()

    if source_type == DBType.TIMESCALEDB:
        print("Enabling background jobs ...")
        enable_user_background_jobs()
        print("Setting replica identity back to DEFAULT for caggs ...")
        if caggs_count > 0:
            set_replica_identity_for_caggs('DEFAULT')

    print("Cleaning up ...")
    cleanup()
    health_checker.stop_all()
    print("Migration successfully completed")

    telemetry.complete_success()
    housekeeping_stop_event.set()
    housekeeping_thread.join()
    sys.exit(0)
