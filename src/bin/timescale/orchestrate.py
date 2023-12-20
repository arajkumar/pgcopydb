#!/usr/bin/env python3
# This script orchestrates CDC based migration process from TimescaleDB to Timescale.
import subprocess
import os
import time
import signal
import re
import sys
import shutil
import tempfile
import threading
import queue
import uuid
import json

from housekeeping import start_housekeeping
from pathlib import Path
from urllib.parse import urlparse
from time import perf_counter

# If the process is not running on a TTY then reading from stdin will raise and
# exception. We read from stdin to start the switch-over phase. If it's not a
# TTY the switch-over can be started by sending a SIGUSR1 signal to the
# process.
IS_TTY = os.isatty(0)
DELAY_THRESHOLD = 30  # Megabytes.
SCRIPT_VERSION = "0.0.1"
IS_POSTGRES_SOURCE = False

env = os.environ.copy()

LIVE_MIGRATION_DOCKER = env.get('LIVE_MIGRATION_DOCKER') == 'true'

class timeit:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.start
        print(f"Completed in {self.time:.1f}s")

class RedactedException(Exception):
    def __init__(self, err, redacted):
        super().__init__(str(err))
        self.err = err
        self.redacted = redacted


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
        cmd_name = cmd.split()[0]
        raise RedactedException(
            f"command '{cmd}' exited with {result.returncode} code. stderr={result.stderr}. stdout={result.stdout}",
            f"{cmd_name} exited with code {result.returncode}")
    return str(result.stdout)


def psql(uri: str, sql: str):
    return f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d {uri} -c " {sql} " """


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

    def terminate_process_including_children(self):
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

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

def extract_lsn_and_snapshot(log_line: str):
    """
    This function takes a log_line from pgcopydb snapshot and returns a LSN and snapshot ID.
    Example:
    11:35:31.327 40612 INFO   Created logical replication slot "switchover" with plugin "wal2json" at D2/CD013570 and exported snapshot 0000000E-0001B0E1-1

    Output => D2/CD013570, 0000000E-0001B0E1-1
    """
    lsn_pattern = r"at\s([A-F0-9/]+)"
    snapshot_pattern = r"snapshot\s([A-F0-9/-]+)"

    lsn_match = re.search(lsn_pattern, log_line)
    snapshot_match = re.search(snapshot_pattern, log_line)

    lsn = lsn_match.group(1) if lsn_match else None
    snapshot_id = snapshot_match.group(1) if snapshot_match else None

    return lsn, snapshot_id


def pgcopydb_init_env():
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
    if not IS_POSTGRES_SOURCE:
        switch_over_dir = (work_dir / "switch_over").absolute()
        env["PGCOPYDB_SWITCH_OVER_DIR"] = str(switch_over_dir)
        env["PGCOPYDB_METADATA_DIR"] = str((switch_over_dir / "caggs_metadata").absolute())

    return work_dir

def create_dirs(work_dir: Path):
    global env
    (work_dir / "logs").mkdir(parents=True, exist_ok=True)
    if not IS_POSTGRES_SOURCE:
        os.makedirs(env["PGCOPYDB_METADATA_DIR"], exist_ok=True)

    # This is needed because cleanup doesn't support --dir
    pgcopydb_dir = Path(tempfile.gettempdir()) / "pgcopydb"
    pgcopydb_dir.mkdir(parents=True, exist_ok=True)

@telemetry_command("check_source_is_timescaledb")
def check_source_is_timescaledb():
    result = run_cmd(psql(uri="$PGCOPYDB_SOURCE_PGURI", sql="select exists(select 1 from pg_extension where extname = 'timescaledb');"))
    return result == "t\n"

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

def is_caggs_metadata_copy_complete():
    return os.path.exists(f"{env['PGCOPYDB_DIR']}/run/caggs-restore-post.done")

def mark_caggs_metadata_copy_complete():
    Path(f"{env['PGCOPYDB_DIR']}/run/caggs-restore-post.done").touch()

def mark_initial_data_copy_complete():
    Path(f"{env['PGCOPYDB_DIR']}/run/restore-post.done").touch()

@telemetry_command("cleanup")
def cleanup():
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR --slot-name pgcopydb")
    if not IS_POSTGRES_SOURCE:
        run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_SWITCH_OVER_DIR --slot-name switchover")
        shutil.rmtree(str(env["PGCOPYDB_SWITCH_OVER_DIR"]), ignore_errors=True)

    shutil.rmtree(str(env["PGCOPYDB_DIR"]), ignore_errors=True)


@telemetry_command("create_snapshot_and_follow")
def create_snapshot_and_follow(resume: bool = False):
    snapshot_proc = None
    # Previous run would have already completed the initial data copy
    # hence creation of snapshot is not needed.
    if not resume:
        print("Creating snapshot ...")
        snapshot_command = "pgcopydb snapshot --follow --dir $PGCOPYDB_DIR --plugin wal2json"
        snapshot_proc = Command(command=snapshot_command, use_shell=True,
                                log_path=f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_snapshot")

        time.sleep(10)

    print(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}/cdc ...")
    follow_command = f"pgcopydb follow --dir $PGCOPYDB_DIR --plugin wal2json --snapshot $(cat {env['PGCOPYDB_DIR']}/snapshot)"
    if resume:
        follow_command += " --resume"
    follow_proc = Command(command=follow_command, use_shell=True,
                          log_path=f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_follow")
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
def migrate_existing_data_from_pg():
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

    print("Restoring pre-data ...")
    with timeit():
        psql_command = " ".join(["psql",
                                 "-X",
                                 "-d",
                                 "$PGCOPYDB_TARGET_PGURI",
                                 "--echo-errors",
                                 "-f",
                                 "$PGCOPYDB_DIR/pre-data-dump.sql",
                                 ])
        run_cmd(psql_command, f"{env['PGCOPYDB_DIR']}/logs/pre_data_restore", ignore_non_zero_code=True)

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
                             "--restart",
                             "--dir",
                             "$PGCOPYDB_DIR/table-data-copy",
                             ])
    print("Copying table data ...")
    with timeit():
        run_cmd(copy_table_data, f"{env['PGCOPYDB_DIR']}/logs/copy_table_data")

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

    print("Restoring post-data ...")
    with timeit() as t:
        psql_command = " ".join(["psql",
                                 "-X",
                                 "-d",
                                 "$PGCOPYDB_TARGET_PGURI",
                                 "--echo-errors",
                                 "-v",
                                 "ON_ERROR_STOP=1",
                                 "-f",
                                 "$PGCOPYDB_DIR/post-data-dump.sql",
                                 ])
        run_cmd(psql_command, f"{env['PGCOPYDB_DIR']}/logs/post_data_restore", ignore_non_zero_code=True)

@telemetry_command("migrate_roles")
def migrate_roles():
    print(f"Dumping roles to {env['PGCOPYDB_DIR']}/roles.sql ...")
    with timeit():
        source_pg_uri = env["PGCOPYDB_SOURCE_PGURI"]
        source_dbname = dbname_from_uri(source_pg_uri)
        if source_dbname == "":
            raise ValueError("unable to extract dbname from uri")
        roles_file_path = f"{env['PGCOPYDB_DIR']}/roles.sql"

        dump_roles = f"""pg_dumpall -d $PGCOPYDB_SOURCE_PGURI --quote-all-identifiers --roles-only --no-role-passwords -l {source_dbname} --file={roles_file_path}"""
        run_cmd(dump_roles)

        # When using MST, Aiven roles modify parameters like "pg_qualstats.enabled" that are not permitted on cloud.
        # Hence, we remove Aiven roles assuming they are not being used for tasks other than ones specific to Aiven/MST.
        filter_stmts = f"""
sed -i -E \
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
    print(f"Creating a dump at {env['PGCOPYDB_DIR']}/dump ...")
    start = time.time()

    run_sql(execute_on_target=True, sql="select timescaledb_pre_restore();")

    pgdump_command = f"pg_dump -d $PGCOPYDB_SOURCE_PGURI --jobs 8 --no-owner --no-privileges --no-tablespaces --quote-all-identifiers --format d --file $PGCOPYDB_DIR/dump --exclude-table _timescaledb_catalog.continuous_agg*  -v --snapshot $(cat {env['PGCOPYDB_DIR']}/snapshot)"
    run_cmd(pgdump_command, f"{env['PGCOPYDB_DIR']}/logs/pg_dump_existing_data")
    time_taken = (time.time() - start) / 60
    print(f"Completed in {time_taken:.1f}m")

    print(f"Restoring from {env['PGCOPYDB_DIR']}/dump ...")
    start = time.time()

    pgrestore_command = "pg_restore -d $PGCOPYDB_TARGET_PGURI --no-owner --no-privileges --no-tablespaces -v --format d $PGCOPYDB_DIR/dump"
    run_cmd(pgrestore_command, f"{env['PGCOPYDB_DIR']}/logs/pg_restore_existing_data", ignore_non_zero_code=True)
    time_taken = (time.time() - start) / 60
    print(f"Completed in {time_taken:.1f}m")
    run_sql(execute_on_target=True,
            sql="begin; select public.timescaledb_post_restore(); select public.alter_job(id::integer, scheduled => false) from _timescaledb_config.bgw_job where application_name like 'Refresh Continuous%'; commit;")

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
                print(f'{diff}. To proceed, send a SIGUSR1 signal with "docker kill --s=SIGUSR1 <container_name>"')
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

@telemetry_command("wait_for_LSN_to_sync")
def wait_for_LSN_to_sync():
    switchover_snapshot_command = "pgcopydb snapshot --follow --dir $PGCOPYDB_SWITCH_OVER_DIR --plugin wal2json --slot-name switchover"
    switchover_snapshot_proc = Command(command=switchover_snapshot_command, use_shell=True,
                                       log_path=f"{env['PGCOPYDB_DIR']}/logs/switchover_snapshot")
    switchover_snapshot_log = f"{env['PGCOPYDB_DIR']}/logs/switchover_snapshot_stderr.log"

    time.sleep(10)  # Wait for snapshot to create.

    end_pos_lsn = ""
    with open(switchover_snapshot_log, 'r') as file:
        line_count = 0
        for line in file:
            line_count += 1
            if line_count == 2:
                log_line = line.strip()
                end_pos_lsn, _ = extract_lsn_and_snapshot(log_line)
                break

    print(f"Last LSN (Log Sequence Number) to be streamed => {end_pos_lsn}")
    run_cmd(f"pgcopydb stream sentinel set endpos {end_pos_lsn}")

    print(f"Streaming upto {end_pos_lsn} (Last LSN) ...")
    endpos_follow_command = "pgcopydb follow --dir $PGCOPYDB_DIR --plugin wal2json --resume"
    endpos_follow_proc = Command(endpos_follow_command, use_shell=True,
                                 log_path=f"{env['PGCOPYDB_DIR']}/logs/endpos_follow")

    endpos_follow_log = f"{env['PGCOPYDB_DIR']}/logs/endpos_follow_stderr.log"

    def is_endpos_streamed(log_line: str):
        pattern_1 = r"Streamed up to write_lsn [A-F0-9/]+, flush_lsn [A-F0-9/]+, stopping"
        pattern_2 = r"Follow mode is now done, reached replay_lsn [A-F0-9/]+ with endpos [A-F0-9/]+"
        return bool(re.search(pattern_1, log_line)) or bool(re.search(pattern_2, log_line))

    while True:
        time.sleep(5)
        file = open(endpos_follow_log, 'r')
        done = False
        for line in file:
            if is_endpos_streamed(line.strip()):
                print("End position LSN has been streamed successfully")
                done = True
                break
        if done:
            break

    # todo: we can remove the pattern matching the log file
    # and wait for the endpos_follow_proc to complete.
    endpos_follow_proc.terminate_process_including_children()
    return switchover_snapshot_proc


@telemetry_command("copy_sequences")
def copy_sequences():
    run_cmd("pgcopydb copy sequences --resume --not-consistent", f"{env['PGCOPYDB_DIR']}/logs/copy_sequences")


@telemetry_command("migrate_caggs_metadata")
def migrate_caggs_metadata():
    print(f"Dumping metadata from Source DB to {env['PGCOPYDB_METADATA_DIR']} ...")
    metadata_pg_dump_command = f"pg_dump -d $PGCOPYDB_SOURCE_PGURI --no-owner --no-privileges --no-tablespaces --quote-all-identifiers --format d --file $PGCOPYDB_METADATA_DIR --table _timescaledb_catalog.continuous_agg*  -v --snapshot $(cat {env['PGCOPYDB_SWITCH_OVER_DIR']}/snapshot)"
    start = time.time()
    run_cmd(metadata_pg_dump_command, f"{env['PGCOPYDB_DIR']}/logs/pg_dump_metadata")
    time_taken = (time.time() - start) / 60
    print(f"Completed in {time_taken:.1f}m")

    print(f"Restoring metadata from {env['PGCOPYDB_METADATA_DIR']} to Target DB ...")
    metadata_pg_restore_command = "pg_restore -d $PGCOPYDB_TARGET_PGURI --no-owner --no-privileges --no-tablespaces --data-only --format d $PGCOPYDB_METADATA_DIR"
    start = time.time()
    run_cmd(metadata_pg_restore_command, f"{env['PGCOPYDB_DIR']}/logs/pg_restore_metadata")
    time_taken = (time.time() - start) / 60
    print(f"Completed in {time_taken:.1f}m")


def cleanup_pid_files(work_dir: Path):
    global env

    print("Cleaning up pid files ...")

    (work_dir / "pgcopydb.snapshot.pid").unlink(missing_ok=True)
    (work_dir / "pgcopydb.pid").unlink(missing_ok=True)

    if not IS_POSTGRES_SOURCE:
        switchover_dir = Path(env["PGCOPYDB_SWITCH_OVER_DIR"])
        (switchover_dir / "pgcopydb.snapshot.pid").unlink(missing_ok=True)
        (switchover_dir / "pgcopydb.pid").unlink(missing_ok=True)

@telemetry_command("enable_caggs_policies")
def enable_caggs_policies():
    run_sql(execute_on_target=True,
            sql="select alter_job(job_id, scheduled => true) from timescaledb_information.jobs where application_name like 'Refresh Continuous%';")
    run_sql(execute_on_target=True,
            sql="select timescaledb_post_restore();")

if __name__ == "__main__":
    install_signal_handler()

    force_postgres_source = os.getenv("POSTGRES_SOURCE") == "true"
    if force_postgres_source or not check_source_is_timescaledb():
        print("Migrating from Postgres to Timescale ...")
        IS_POSTGRES_SOURCE = True
    else:
        print("Migrating from Timescale to Timescale ...")
        print("Verifying TimescaleDB version in Source DB and Target DB ...")
        check_timescaledb_version()

    work_dir = pgcopydb_init_env()

    skip_initial_data_copy = False

    if source_has_incomplete_migration():
        if is_initial_data_copy_complete():
            skip_initial_data_copy = True
            run_cmd("killall -SIGINT pgcopydb", ignore_non_zero_code=True)

            # this is needed for docker environment, where the process
            # always gets deterministic pid while starting.
            cleanup_pid_files(work_dir)
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

        if IS_POSTGRES_SOURCE:
            migrate_existing_data_from_pg()
        else:
            migrate_existing_data_from_ts()

        mark_initial_data_copy_complete()

        snapshot_proc.terminate()

    (housekeeping_thread, housekeeping_stop_event) = start_housekeeping(env)

    print("Applying buffered transactions ...")
    run_cmd("pgcopydb stream sentinel set apply")

    wait_for_DBs_to_sync()

    if IS_POSTGRES_SOURCE:
        print("Stopping live-replay ...")
        run_cmd("pgcopydb stream sentinel set endpos --current")

        print("Waiting for live-replay to complete ...")
        follow_proc.wait()
    else:
        print("Waiting for live-replay to stop ...")
        follow_proc.terminate_process_including_children()
        print("Stopped")

    # At this moment, user has finished verifying data integrity. He is ready
    # to resume the migration process and move into the switch-over phase.
    # Note, the traffic remains halted ATM.

    if not IS_POSTGRES_SOURCE:
        print("Syncing last LSN in Source DB to Target DB ...")
        switchover_snapshot_proc = wait_for_LSN_to_sync()

    print("Copying sequences ...")
    copy_sequences()

    if not IS_POSTGRES_SOURCE:
        if is_caggs_metadata_copy_complete():
            print("Skipping caggs metadata migration ...")
        else:
            print("Migrating caggs metadata ...")
            migrate_caggs_metadata()
            mark_caggs_metadata_copy_complete()

        switchover_snapshot_proc.terminate()

        print("Enabling continuous aggregates refresh policies ...")
        enable_caggs_policies()

    print("Cleaning up ...")
    cleanup()
    print("Migration successfully completed")

    telemetry.complete_success()
    housekeeping_stop_event.set()
    housekeeping_thread.join()
    sys.exit(0)
