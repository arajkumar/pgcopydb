# This script orchestrates CDC based migration process from TimescaleDB to Timescale.
import subprocess
import threading
import os
import time
import signal
import queue
import re
import sys

DELAY_THRESHOLD = 30 # Megabytes.

env = os.environ.copy()
for key in ["PGCOPYDB_DIR", "PGCOPYDB_SWITCH_OVER_DIR", "PGCOPYDB_METADATA_DIR", "PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"]:
    if env[key] == "" or env[key] == None:
        raise ValueError(f"${key} not found")

# Stop any running pgcopydb process.
print("Killing pgcopydb processes")
subprocess.run(["killall", "-9", "pgcopydb"])
subprocess.run(["rm", "-R",
                f"{env['PGCOPYDB_DIR']}/cdc",
                f"{env['PGCOPYDB_DIR']}/compare",
                f"{env['PGCOPYDB_DIR']}/pgcopydb.*",
                f"{env['PGCOPYDB_DIR']}/run",
                f"{env['PGCOPYDB_DIR']}/schema",
                f"{env['PGCOPYDB_DIR']}/snapshot",
                f"{env['PGCOPYDB_DIR']}/dump",
                f"{env['PGCOPYDB_DIR']}/logs"])
subprocess.run(["mkdir", f"{env['PGCOPYDB_DIR']}/logs"])

class CommandThread(threading.Thread):
    def __init__(self, command: str = "", env = env, use_shell: bool = False, log_path: str = ""):
        super().__init__()
        self.command = command
        self.process = None
        self.env = env
        self.use_shell = use_shell
        self.log_path = log_path
        self.pid = None

    def run(self):
        if self.log_path == "":
            self.process = subprocess.Popen(self.command, shell=self.use_shell, env=self.env)
        else:
            fname_stdout = f"{self.log_path}_stdout.log"
            f_stdout = open(fname_stdout, "w")

            fname_stderr = f"{self.log_path}_stderr.log"
            f_stderr = open(fname_stderr, "w")

            self.process = subprocess.Popen(self.command, shell=self.use_shell, env=self.env,
                                            stdout=f_stdout, stderr=f_stderr)
        self.pid = self.process.pid
        self.process.wait()

    def send_signal(self, signal_type):
        if self.process is not None:
            self.process.send_signal(signal_type)

    def get_pid(self):
        return self.pid

def run_cmd(cmd: str):
    if subprocess.run(cmd, shell=True, env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE).returncode != 0:
        raise RuntimeError(f"error running cmd: '{cmd}'")

result = subprocess.run("psql -A -t -d $PGCOPYDB_SOURCE_PGURI -c \" select extversion from pg_extension where extname = 'timescaledb'; \" ", shell=True, env=env, stdout=subprocess.PIPE, text=True)
source_ts_version = str(result.stdout)[:-1]
result = subprocess.run("psql -A -t -d $PGCOPYDB_TARGET_PGURI -c \" select extversion from pg_extension where extname = 'timescaledb'; \" ", shell=True, env=env, stdout=subprocess.PIPE, text=True)
target_ts_version = str(result.stdout)[:-1]
if source_ts_version != target_ts_version:
    raise RuntimeError(f"Source TimescaleDB version ({source_ts_version}) does not match Target TimescaleDB version ({target_ts_version})")

cleanup_command = "pgcopydb stream cleanup"
cleanup_thread = CommandThread(command=cleanup_command, use_shell=True,
                                log_path=f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_cleanup")
cleanup_thread.start()
cleanup_thread.join()

snapshot_command = "pgcopydb snapshot --follow --dir $PGCOPYDB_DIR --plugin wal2json"
snapshot_thread = CommandThread(command=snapshot_command, use_shell=True,
                                log_path=f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_snapshot")
print("Creating snapshot ...")
snapshot_thread.start()

time.sleep(10) # 10 seconds of wait should be enough to create a snapshot.

follow_command = f"pgcopydb follow --dir $PGCOPYDB_DIR --plugin wal2json --snapshot $(cat {env['PGCOPYDB_DIR']}/snapshot)"
follow_thread = CommandThread(command=follow_command, use_shell=True,
                              log_path=f"{env['PGCOPYDB_DIR']}/logs/pgcopydb_follow")
print(f"Buffering live transactions from Source DB to {env['PGCOPYDB_DIR']}/cdc ...")
follow_thread.start()

run_cmd('psql -A -d $PGCOPYDB_TARGET_PGURI -c "select timescaledb_pre_restore()"')

pgdump_command = f"pg_dump --jobs 8 $PGCOPYDB_SOURCE_PGURI --format d --file $PGCOPYDB_DIR/dump --exclude-table _timescaledb_catalog.continuous_agg*  -v --snapshot $(cat {env['PGCOPYDB_DIR']}/snapshot)"
pgdump_thread = CommandThread(command=pgdump_command, use_shell=True,
                              log_path=f"{env['PGCOPYDB_DIR']}/logs/pgdump")
print(f"Creating a dump at {env['PGCOPYDB_DIR']}/dump ...")
start = time.time()
pgdump_thread.start()
pgdump_thread.join()
time_taken = time.time() - start
print(f"Completed in {time_taken:.0f}s")

pgrestore_command = "pg_restore -d $PGCOPYDB_TARGET_PGURI -v --format d $PGCOPYDB_DIR/dump"
pgrestore_thread = CommandThread(command=pgrestore_command, use_shell=True,
                                 log_path=f"{env['PGCOPYDB_DIR']}/logs/pgrestore")
print(f"Restoring from {env['PGCOPYDB_DIR']}/dump ...")
start = time.time()
pgrestore_thread.start()
pgrestore_thread.join()
time_taken = time.time() - start
print(f"Completed in {time_taken:.0f}s")

postrestore_command = "psql -A -d $PGCOPYDB_TARGET_PGURI -c \"begin; select public.timescaledb_post_restore(); select public.alter_job(id::integer, scheduled => false) from _timescaledb_config.bgw_job where application_name like 'Refresh Continuous%'; commit; \" "
run_cmd(postrestore_command)

snapshot_thread.send_signal(signal_type=signal.SIGINT)

print("Applying buffered transactions ...")
run_cmd("pgcopydb stream sentinel set apply")

def wait_for_signal(sig):
    received_signal = False

    def handler(signum, frame):
        nonlocal received_signal
        received_signal = True

    signal.signal(sig, handler)
    print(f"PID: {os.getpid()} - Waiting for signal {sig}")
    while not received_signal:
        signal.pause()

    print(f"Received signal {sig}")

analyze_queue = queue.Queue()
def analyze_routine():
    analyze_command = "psql -A -t -d $PGCOPYDB_TARGET_PGURI -c \" analyze verbose; \" "
    run_cmd(analyze_command)
    analyze_queue.put(1)
    return

analyze_started = False
analyze_finished = False
analyze_thread = threading.Thread(target=analyze_routine)
while True:
    delay = "psql -A -t -d $PGCOPYDB_SOURCE_PGURI -c \" select pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) delay from pgcopydb.sentinel \" "
    result = subprocess.run(delay, shell=True, env=env, stdout=subprocess.PIPE, text=True)
    delay_bytes = int(result.stdout)
    delay_mb = int(delay_bytes / (1024 * 1024))
    print(f"[WATCH] Source DB - Target DB => {delay_mb}MB")
    if not analyze_started and delay_mb < DELAY_THRESHOLD:
        print(f"Source DB - Target DB below {DELAY_THRESHOLD}MB threshold, analyzing Target DB ...")
        analyze_started = True
        analyze_thread.start()
    if not analyze_finished and not analyze_queue.empty():
        print("Waiting for analyze ...")
        analyze_thread.join()
        analyze_finished = True
    if analyze_finished and delay_mb > 0:
        print("[ACTION NEEDED] When you are ready, stop the traffic on your Source DB so that 'Source DB - Target DB' can be 0")
    if analyze_finished and delay_mb == 0:
        # At this moment, the historical and live data has been migrated.
        # The user is yet to verify the intigrity of historical + live data for which he needs to stop the traffic.
        # The moment he think he is ready for verification, he will stop the traffic and signal a SIGHUP, and we
        # will respond with stopping the live replay process.
        print("Source DB and Target DB are in sync.")
        print("[ACTION NEEDED] Waiting for SIGHUP to stop live-replay process so that you can check data intigrity ...")
        wait_for_signal(signal.SIGHUP)
        print("[SIGNAL] Received SIGHUP")
        break
    time.sleep(10)

print("Waiting for pgcopydb follow to stop ...")
pid = follow_thread.get_pid()
os.kill(pid, signal.SIGKILL)
follow_thread.join()
print("Stopped")

print("[ACTION NEEDED] Now, you should check intigrity of your data. Once you are confident, you need to give SIGCONT to continue")

# At this moment, user has finished verifying data intigrity. He is ready to resume the migration process
# and move into the switch-over phase. Note, the traffic remains halted ATM.
wait_for_signal(signal.SIGCONT)
print("[SIGNAL] Received SIGCONT")

switchover_snapshot_command = "pgcopydb snapshot --follow --dir $PGCOPYDB_SWITCH_OVER_DIR --plugin wal2json --slot-name switchover"
switchover_snapshot_thread = CommandThread(command=switchover_snapshot_command, use_shell=True,
                                           log_path=f"{env['PGCOPYDB_DIR']}/logs/switchover_snapshot")
print("Starting switchover snapshot ...")
switchover_snapshot_thread.start()

time.sleep(10) # Wait for snapshot to create.

switchover_snapshot_log = f"{env['PGCOPYDB_DIR']}/logs/switchover_snapshot_stderr.log"

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

end_pos_lsn = ""
switchover_snapshot_id = ""
with open(switchover_snapshot_log, 'r') as file:
    line_count = 0
    for line in file:
        line_count += 1
        if line_count == 2:
            log_line = line.strip()
            end_pos_lsn, switchover_snapshot_id = extract_lsn_and_snapshot(log_line)
            break

print(f"Last LSN (Log Sequence Number) to be streamed => {end_pos_lsn}")
run_cmd(f"pgcopydb stream sentinel set endpos {end_pos_lsn}")

endpos_follow_command = "pgcopydb follow --dir $PGCOPYDB_SWITCH_OVER_DIR --plugin wal2json --resume"
endpos_follow_thread = CommandThread(endpos_follow_command, use_shell=True,
                                     log_path=f"{env['PGCOPYDB_DIR']}/logs/endpos_follow")

print(f"Streaming upto {end_pos_lsn} (Last LSN) ...")
endpos_follow_thread.start()
endpos_follow_log = f"{env['PGCOPYDB_DIR']}/logs/endpos_follow_stderr.log"

def is_endpos_streamed(log_line: str):
    pattern_1 = r"Streamed up to write_lsn [A-F0-9/]+, flush_lsn [A-F0-9/]+, stopping"
    pattern_2 = r"Current endpos [A-F0-9/]+ was previously reached at [A-F0-9/]+"
    return bool(re.search(pattern_1, log_line)) or bool(re.search(pattern_2, log_line))

while True:
    time.sleep(5)
    file = open(endpos_follow_log, 'r')
    done = False
    for line in file:
        l = line.strip()
        if is_endpos_streamed(l):
            print("End position LSN has been streamed successfully")
            done = True
            break
    if done:
        break

print("Waiting for 'pgcopydb follow' for Last LSN to complete ...")
endpos_follow_thread.join()

copy_sequences_command = "pgcopydb copy sequences"
copy_sequences_thread = CommandThread(copy_sequences_command, use_shell=True,
                                      log_path=f"{env['PGCOPYDB_DIR']}/logs/copy_sequences")
print("Copying sequences ...")
copy_sequences_thread.start()
copy_sequences_thread.join()

metadata_pg_dump_command = f"pg_dump $PGCOPYDB_SOURCE_PGURI --format d --file $PGCOPYDB_METADATA_DIR --table _timescaledb_catalog.continuous_agg*  -v --snapshot $(cat ${env['PGCOPYDB_SWITCH_OVER_DIR']}/snapshot)"
metadata_pg_dump_thread = CommandThread(metadata_pg_dump_command, use_shell=True,
                                        log_path=f"{env['PGCOPYDB_DIR']}/logs/pg_dump_metadata")
print(f"Dumping metadata from Source DB to {env['PGCOPYDB_METADATA_DIR']} ...")
start = time.time()
metadata_pg_dump_thread.start()
metadata_pg_dump_thread.join()
time_taken = time.time() - start
print(f"Completed in {time_taken:.0f}s")

metadata_pg_restore_command = "pg_restore -d $PGCOPYDB_TARGET_PGURI --format d $PGCOPYDB_METADATA_DIR"
metadata_pg_restore_thread = CommandThread(metadata_pg_restore_command, use_shell=True,
                                        log_path=f"{env['PGCOPYDB_DIR']}/logs/pg_restore_metadata")
print(f"Restoring metadata from {env['PGCOPYDB_METADATA_DIR']} to Target DB ...")
start = time.time()
metadata_pg_restore_thread.start()
metadata_pg_restore_thread.join()
time_taken = time.time() - start
print(f"Completed in {time_taken:.0f}s")

print("Enabling continuous aggregates refresh policies ...")
enable_cagg_jobs = "psql -A -t -d $PGCOPYDB_TARGET_PGURI -c \" select alter_job(job_id, scheduled => true) from timescaledb_information.jobs where application_name like 'Refresh Continuous%'; \" "
run_cmd(enable_cagg_jobs)

print("Running all jobs ...")
run_all_jobs = "psql -A -t -d $PGCOPYDB_TARGET_PGURI -c \" select timescaledb_post_restore(); \" "
run_cmd(run_all_jobs)

print("Migration successfully completed")
sys.exit(0)
