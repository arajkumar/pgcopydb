import os
import time
import json
import traceback
import base64

from dataclasses import dataclass
from version import SCRIPT_VERSION
from psql import psql
from utils import get_dbtype, DBType

@dataclass
class DatabaseStats:
    pg_version: str
    ts_version: str
    size_approx: int
    num_user_tables: int
    num_hypertables: int

    def __post_init__(self):
        if self.size_approx:
            self.size_approx = int(self.size_approx)
        if self.num_user_tables:
            self.num_user_tables = int(self.num_user_tables)
        if self.num_hypertables:
            self.num_hypertables = int(self.num_hypertables)

    @classmethod
    def fetch(cls, uri: str) -> 'DatabaseStats':
        if get_dbtype(uri) == DBType.POSTGRES:
            num_hypertables_query = "select 0"
        else:
            num_hypertables_query = "select count(*) from timescaledb_information.hypertables"

        query = fr"""
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
        result = psql(uri, sql=query)[0]
        return DatabaseStats(**result)

class Telemetry:
    def __init__(self, command, source, target, id):
        self.source = source
        self.target = target
        self.send_only_on_failure = True

        source_db_stats = DatabaseStats.fetch(source)
        self.payload = {
            "type": "migration",
            "start": time.time(),
            # TODO: Remove the success key from the payload once
            # dashboard is updated to use the status key
            "success": False,
            "status": "not started",
            "metadata": {
                "version": SCRIPT_VERSION,
                "method": "LIVE_MIGRATION",
                "migration_id": id,
                "cpu_count": os.cpu_count(),
            },
            "command_by_duration_seconds": [],
            "commmand": command,
            "total_duration_seconds": 0,
            "source_db_stats": source_db_stats.__dict__,
            "target_db_stats": {},
            "progress": "",
            "errors": []
        }

    def send_on_success(self):
        self.send_only_on_failure = False

    def progress(self, message):
        self.payload["progress"] = message

    def mark_interrupted(self):
        self.payload["success"] = False
        self.payload["status"] = "interrupted"

    def mark_success(self):
        self.payload["success"] = True
        self.payload["status"] = "completed"

    def set_status(self, status):
        self.payload["status"] = status

    def mask_secrets(self, text: str) -> str:
        # Replace the secrets with placeholders
        secrets = [
                (self.source, "postgres://SOURCE"),
                (self.target, "postgres://TARGET"),
        ]

        for secret, placeholder in secrets:
            text = text.replace(secret, placeholder)

        return text

    def add_exception(self):
        exception = self.mask_secrets(traceback.format_exc())

        # Encode the exception to base64 to avoid any JSON parsing issues
        exception = base64.b64encode(exception.encode()).decode()

        self.payload["errors"].append(exception)
        self.payload["status"] = "error"
        self.payload["success"] = False

    def write(self):
        if self.payload["success"] and self.send_only_on_failure:
            return

        self.payload["total_duration_seconds"] = int(time.time() - self.payload["start"])
        target_db_stats = DatabaseStats.fetch(self.target)
        self.payload["target_db_stats"] = target_db_stats.__dict__
        self.payload["end"] = time.time()

        data = json.dumps(self.payload)

        sql = f"""
        insert into
            _timescaledb_catalog.telemetry_event(tag, body)
            values('migration'::name, '{data}'::jsonb)
        """
        psql(conn=self.target, sql=sql)
        # TODO: Also send this to new telemetry endpoint
