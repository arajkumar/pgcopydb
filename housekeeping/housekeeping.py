#!/usr/bin/env python3
import os
import json
import subprocess
import time

ORIGIN = 'pgcopydb'
BUFFER = 3+1 # Number of files to buffer from being deleted.

env = os.environ.copy()
for key in ["PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"]:
    if key not in env or env[key] == "" or env[key] == None:
        raise ValueError(f"${key} not found")

def run_cmd(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"command '{cmd}' exited with {result.returncode} code ")
    return str(result.stdout)

def run_sql_source(sql: str) -> str:
    return run_cmd(f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d $PGCOPYDB_SOURCE_PGURI -c " {sql} " """)

def run_sql_target(sql: str) -> str:
    return run_cmd(f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d $PGCOPYDB_TARGET_PGURI -c " {sql} " """)

# Test connection.
run_sql_source("select 1")
run_sql_target("select 1")

HOUSEKEEPING_INTERVAL = int(os.getenv("HOUSEKEEPING_INTERVAL", 300))
print(f"Performing housekeeping every {HOUSEKEEPING_INTERVAL}s ...")

WORK_DIR = os.getenv("PGCOPYDB_DIR")
if WORK_DIR is None:
    raise Exception('cannot do housekeeping: $PGCOPYDB_DIR is not set')
WORK_DIR = f'{WORK_DIR}/cdc'
print(f"Using {WORK_DIR} as CDC directory ...")

get_last_replicated_origin_sql = f"select pg_replication_origin_progress('{ORIGIN}', false)"
def get_last_replicated_origin():
    lsn = run_sql_target(get_last_replicated_origin_sql)[:-1]
    wal_file_name = run_sql_source(f"select pg_walfile_name('{lsn}'::pg_lsn)")[:-1]
    return lsn, wal_file_name

def is_txn_state_file(f):
    CONTENT_PREFIX = '{"xid"'
    file = open(f, "r")
    num_line = 0
    is_first_line_txn = False
    for line in file:
        num_line += 1
        if num_line == 1 and line.startswith(CONTENT_PREFIX):
            is_first_line_txn = True
        if num_line > 1:
            return False
    return num_line == 1 and is_first_line_txn

def get_files_in_dir(directory):
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and (f.endswith('.sql') or f.endswith('.json.partial'))]

def get_txn_state_file_in_dir(directory):
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.endswith('.json') and is_txn_state_file(os.path.join(directory, f))]

def is_replicated(lsn_in_file, lsn_replicated) -> bool:
    if lsn_in_file == "" or lsn_replicated == "":
        return False
    result = run_sql_target(f"select '{lsn_in_file}'::pg_lsn < '{lsn_replicated}'::pg_lsn")
    return bool(result)

def state_file_lsn(f):
    file = open(f, "r")
    line = file.readline().strip()
    state_record = json.loads(line)
    return state_record["commit_lsn"]

def filename_no_ext(f):
    if f.endswith(".partial"):
        f = f.rstrip(".partial")
    return os.path.splitext(f)[0]

def delete_file(filename):
    try:
        os.remove(filename)
    except FileNotFoundError:
        return

def sort_files_by_name(abs_files):
    files_map = {}
    files_name_only_list = []
    for f in abs_files:
        file_name_only = f
        # Safety: Trim the first 8 digits (T section) so that string to int base 16 conversion does not panic
        # First 8 digits are trivial since they represent the timeline of the database. We do not expect the
        # timeline to change during the migration process.
        file_name_only = file_name_only[8:]
        files_map[file_name_only] = f
        files_name_only_list.append(file_name_only)
    files_name_only_list.sort(key=lambda x: int(x, 16)) # Sort the file names (which are hexadecimal numbers).
    result = [files_map[f] for f in files_name_only_list] # Convert sorted file names to absolute file name.
    return result

def sleep():
    print(f"Sleeping for {HOUSEKEEPING_INTERVAL}s")
    time.sleep(HOUSEKEEPING_INTERVAL)

while True:
    lsn_replicated, present_wal_file = get_last_replicated_origin()
    print(f"Last LSN replicated: {lsn_replicated}; Present WAL file: {present_wal_file}")

    print("Scanning for stale files ...")
    files = get_files_in_dir(WORK_DIR)
    if len(files) == 0:
        print("No stale files found")
        sleep()
        continue
    files_no_ext = []
    for f in files:
        files_no_ext.append(filename_no_ext(f))
    sorted_files = sort_files_by_name(files_no_ext)

    # Find the present wal file
    current_wal_file = ''
    current_wal_file_index = 0
    for i, f in enumerate(sorted_files):
        if f == present_wal_file:
            current_wal_file_index = i
            break
    delete_upto_index = current_wal_file_index - BUFFER
    if delete_upto_index >= 0:
        print(f"Found {delete_upto_index+1} file(s) to be deleted")
        for i, f in enumerate(sorted_files):
            if i <= delete_upto_index and f != filename_no_ext(present_wal_file):
                print(f"Deleting {f}.sql & {f}.json ...")
                delete_file(f'{WORK_DIR}/{f}.sql')
                delete_file(f'{WORK_DIR}/{f}.json')

    print("Scanning for stale transaction files...")
    state_files = get_txn_state_file_in_dir(WORK_DIR)
    count = 0
    for f in state_files:
        lsn = state_file_lsn(f'{WORK_DIR}/{f}')
        if is_replicated(lsn, lsn_replicated):
            delete_file(f'{WORK_DIR}/{f}')
            count += 1
    if count > 0:
        print(f"Cleaned up {count} transaction files")
    sleep()
