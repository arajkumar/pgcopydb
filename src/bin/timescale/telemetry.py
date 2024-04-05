import time
import uuid
import json

from environ import env
from exec import run_cmd, run_sql, psql
from version import SCRIPT_VERSION
from exception import RedactedException
from utils import get_dbtype, DBType

class TelemetryDBStats:
    def __init__(self, uri: str) -> None:
        num_hypertables_query = "select count(*) from timescaledb_information.hypertables"
        if get_dbtype(uri) == DBType.POSTGRES:
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
    """
    A decorator that wraps a function and records telemetry in the
    event of an error during the function's runtime.
    """
    def decorator(func):
        def filter_invalid_chars(s: str) -> str:
            # We filter ", ', ` from exceptions to prevent issues when writing telemetry.
            return s.replace("'", " ").replace('"', " ").replace("`", " ")

        def wrapper_func(*args, **kwargs):
            telemetry.start_command(title)
            try:
                result = func(*args, **kwargs)
            except RedactedException as e:
                telemetry.register_runtime_error_and_write(filter_invalid_chars(e.redacted))
                raise e
            except ValueError as e:
                telemetry.register_runtime_error_and_write(filter_invalid_chars(e.__str__()))
                raise e
            except Exception as e:
                telemetry.complete_fail()
                raise e
            else:
                telemetry.command_complete()
            return result
        return wrapper_func
    return decorator