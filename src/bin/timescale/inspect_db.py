import time
import csv

from psql import psql as psql_cmd

def get_activity(conn: str) -> list[dict]:
    sql = """
        select
            to_char(current_timestamp, 'HH24:MI:SS') AS time,
            pid,
            query_id,
            state,
            current_timestamp - xact_start as running_since,
            query
        from pg_stat_activity
        where
            datname = current_database() and
            application_name like '%pgcopydb%' and
            trim(query) <> '' and
            trim(query) not in (
                'BEGIN',
                'COMMIT',
                'select pg_current_wal_flush_lsn()',
                'select pg_current_wal_insert_lsn()',
                'select pg_replication_origin_xact_setup($1, $2)',
                'select pg_replication_origin_progress($1, $2)'
            ) and
            not (
                query like 'SET statement_timeout TO%' or
                query like 'SET client_encoding TO%' or
                query like 'SET session_replication_role TO%'
            )
        order by pid
        """
    return psql_cmd(conn, sql)


def trim(s: str, num_chars: int, direction_back: bool = True) -> str:
    if len(s) > num_chars:
        if direction_back:
            num_chars -= len("...")
            return s[:num_chars] + "..."
        else:
            num_chars += len("...")
            return "..." + s[:-num_chars]
    return s


def _target_activity(args):
    print("{:^10} | {:^10} | {:^10} | {:^10} | {:^20} | {:^70}".format("TIME", "PID", "QUERY ID", "STATE", "RUNNING SINCE", "QUERY"))
    print("-" * 10, "|", "-" * 10, "|", "-" * 10, "|", "-" * 10, "|", "-" * 20, "|", "-" * 70)
    while True:
        rows = get_activity(args.target)
        for row in rows:
            print("{:^10} | {:^10} | {:^10} | {:^10} | {:^20} | {:<70}".format(
                row["time"],
                row["pid"],
                trim(row["query_id"], 10, False),
                row["state"].upper(),
                row["running_since"],
                trim(str(row["query"]).strip(), 70)))
        time.sleep(args.interval)

def target_activity(args):
    try:
        _target_activity(args)
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)
