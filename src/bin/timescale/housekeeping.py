#!/usr/bin/env python3
#
# Housekeeping is a script designed to remove stale WAL and transaction files (.sql & .json)
# from the `$PGCOPYDB_DIR/cdc` directory. These files have already been applied to the target
# and hence are no longer needed.
#
# The script operates by maintaining an ordered list of all files in the working directory,
# sorted by their hexadecimal names. It then retrieves the current WAL file from the source DB
# and locates its index within the ordered list of files. Once the index is identified,
# the script removes all preceding files, retaining a buffer of three files for safety of large txns.
# This operation is performed every 5 minutes by default.
#

import os
import json
import threading
import logging
from pathlib import Path

from psql import psql

ORIGIN = 'pgcopydb'
BUFFER = 3 + 1  # Number of files to buffer from being deleted.

logger = logging.getLogger(__name__)

HOUSEKEEPING_INTERVAL = int(os.getenv("HOUSEKEEPING_INTERVAL", 300))

def origin_progress(conn, origin):
    sql = f"select pg_replication_origin_progress('{origin}', false) as origin"
    r = psql(conn, sql)
    return r[0]["origin"]

def walfile_name(conn, lsn):
    sql = f"select pg_walfile_name('{lsn}'::pg_lsn) as wal_file_name"
    r = psql(conn, sql)
    return r[0]["wal_file_name"]

def is_txn_state_file(f: Path) -> bool:
    CONTENT_PREFIX = '{"xid"'
    is_first_line_txn = False
    with f.open("r") as file:
        for num_line, line in enumerate(file, start=1):
            if num_line == 1 and line.startswith(CONTENT_PREFIX):
                is_first_line_txn = True
            # txn_state_file should only have 1 line. If we found more than 1
            # line, we should return false as we are not sure if it is a
            # txn_state_file.
            if num_line > 1:
                return False
    return is_first_line_txn


def get_files_in_dir(directory: Path) -> [Path]:
    return [f for f in directory.iterdir() if f.is_file() and (f.suffix == '.sql' or f.suffix == '.json.partial')]


def get_txn_state_file_in_dir(directory: Path) -> [Path]:
    return [f for f in directory.iterdir() if f.is_file() and f.suffix == '.json' and is_txn_state_file(f)]


def is_replicated(conn, lsn_in_file, lsn_replicated) -> bool:
    if lsn_in_file == "" or lsn_replicated == "":
        return False
    sql = f"select ('{lsn_in_file}'::pg_lsn < '{lsn_replicated}'::pg_lsn) as replicated"
    r = psql(conn, sql)
    return r[0]["replicated"] == 't'


def state_file_lsn(f: Path) -> str:
    with f.open("r") as file:
        line = file.readline().strip()
    state_record = json.loads(line)
    return state_record["commit_lsn"]


def filename_no_ext(f: Path) -> str:
    if f.suffix == ".partial":
        f = f.with_suffix('')
    return f.stem


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
    files_name_only_list.sort(key=lambda x: int(x, 16))  # Sort the file names (which are hexadecimal numbers).
    result = [files_map[f] for f in files_name_only_list]  # Convert sorted file names to absolute file name.
    return result


def housekeeping(stop_event, args):
    cdc_dir = args.dir / "cdc"

    if not cdc_dir.is_dir():
        raise ValueError(f"cdc directory {cdc_dir} is not valid")

    logger.info(f"Performing housekeeping every {HOUSEKEEPING_INTERVAL}s ...")
    while not stop_event.is_set():
        lsn_replicated = origin_progress(args.target, ORIGIN)
        present_wal_file = walfile_name(args.source, lsn_replicated)

        files = get_files_in_dir(cdc_dir)
        if len(files) == 0:
            stop_event.wait(timeout=HOUSEKEEPING_INTERVAL)
            continue
        files_no_ext = [filename_no_ext(f) for f in files]
        sorted_files = sort_files_by_name(files_no_ext)

        # Find the present wal file
        current_wal_file_index = 0
        for i, f in enumerate(sorted_files):
            if f == present_wal_file:
                current_wal_file_index = i
                break
        delete_upto_index = current_wal_file_index - BUFFER
        if delete_upto_index >= 0:
            logger.debug(f"Found {delete_upto_index+1} file(s) to be deleted")
            for i, f in enumerate(sorted_files):
                if i <= delete_upto_index and f != filename_no_ext(Path(present_wal_file)):
                    sql_file = cdc_dir / f"{f}.sql"
                    json_file = cdc_dir / f"{f}.json"
                    sql_file.unlink(missing_ok=True)
                    json_file.unlink(missing_ok=True)
                    logger.debug(f"Deleted {f}.{{sql,json}} ...")

        state_files = get_txn_state_file_in_dir(cdc_dir)
        count = 0
        for f in state_files:
            lsn = state_file_lsn(f)
            if is_replicated(args.target, lsn, lsn_replicated):
                f.unlink(missing_ok=True)
                count += 1
        if count > 0:
            logger.info(f"Cleaned up {count} transaction files")
        stop_event.wait(timeout=HOUSEKEEPING_INTERVAL)


stop_event = None
housekeeping_thread = None

def start(args):
    global stop_event, housekeeping_thread

    stop_event = threading.Event()
    housekeeping_thread = threading.Thread(
        target=housekeeping,
        kwargs={'stop_event': stop_event, 'args': args}
    )

    housekeeping_thread.start()
    return (housekeeping_thread, stop_event)

def stop():
    global stop_event, housekeeping_thread

    if stop_event:
        stop_event.set()

    if housekeeping_thread and housekeeping_thread.is_alive():
        housekeeping_thread.join()
