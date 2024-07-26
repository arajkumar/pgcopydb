"""
Helper for TimescaleDB catalog tables and information views.
"""

from dataclasses import dataclass

from psql import psql

class Hypertable:
    def __init__(self, conn, snapshot, **kwargs):
        self.conn = conn
        self.snapshot = snapshot

        self.hypertable_schema = kwargs.get("hypertable_schema")
        self.hypertable_name = kwargs.get("hypertable_name")
        self.fq_table_name = kwargs.get("fq_table_name")
        self.table_name = kwargs.get("table_name")
        self.num_dimensions = int(kwargs.get("num_dimensions", 0))

    def __str__(self):
        return f"{self.fq_table_name}"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def fetch(cls, tsdb: 'TimescaleDB') -> list['Hypertable']:
        """
        Fetch hypertables from the database.
        """
        sql = """
        SELECT
            hypertable_schema,
            hypertable_name,
            FORMAT('%I.%I', hypertable_schema, hypertable_name) as fq_table_name,
            FORMAT('%s.%s', hypertable_schema, hypertable_name) as table_name,
            num_dimensions
        FROM
            timescaledb_information.hypertables;
        """
        result = psql(tsdb.conn, sql, snapshot=tsdb.snapshot)
        return [Hypertable(conn=tsdb.conn, snapshot=tsdb.snapshot, **r) for r in result]

    def truncate(self, conn):
        """
        Truncate the hypertable.
        """
        sql = f"TRUNCATE TABLE {self.fq_table_name};"
        psql(conn, sql)

    def chunks(self):
        """
        Get chunks for the hypertable.
        """
        return Chunk.fetch(self)

    def dimensions(self):
        """
        Get dimensions for the hypertable.
        """
        return Dimension.fetch(self)

@dataclass
class Dimension:
    hypertable_schema: str
    hypertable_name: str
    dimension_number: int
    column_name: str
    column_type: str
    dimension_type: str
    time_interval: str
    integer_interval: int
    integer_now_func: str
    num_partitions: int

    def __str__(self):
        return f"{self.hypertable_schema}.{self.hypertable_name}.{self.column_name}"

    def __repr__(self):
        return self.__str__()

    def __post_init__(self):
        if self.num_partitions:
            self.num_partitions = int(self.num_partitions)
        if self.dimension_number:
            self.dimension_number = int(self.dimension_number)
        if self.integer_interval:
            self.integer_interval = int(self.integer_interval)

    @classmethod
    def fetch(cls, hypertable: Hypertable) -> list['Dimension']:
        """
        Get dimensions for the hypertable.
        """
        sql = f"""
        SELECT
            hypertable_schema,
            hypertable_name,
            dimension_number,
            column_name,
            column_type,
            dimension_type,
            time_interval,
            integer_interval,
            integer_now_func,
            num_partitions
        FROM
            timescaledb_information.dimensions
        WHERE
            hypertable_schema = '{hypertable.hypertable_schema}' AND
            hypertable_name = '{hypertable.hypertable_name}'
        ORDER BY
            dimension_number;
        """
        result = psql(hypertable.conn, sql, snapshot=hypertable.snapshot)
        return [Dimension(**r) for r in result]

@dataclass
class Chunk:
    chunk_schema: str
    chunk_name: str
    hypertable_schema: str
    hypertable_name: str
    fq_table_name: str
    table_name: str

    def __str__(self):
        return f"{self.fq_table_name} ({self.num_dimensions} dimensions)"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def fetch(cls, hypertable: Hypertable) -> list['Chunk']:
        """
        Fetchs chunks for the hypertable.
        """
        sql = f"""
        SELECT
            chunk_schema,
            chunk_name,
            hypertable_schema,
            hypertable_name,
            FORMAT('%I.%I', chunk_schema, chunk_name) as fq_table_name,
            FORMAT('%s.%s', chunk_schema, chunk_name) as table_name
        FROM
            timescaledb_information.chunks
        WHERE
            hypertable_schema = '{hypertable.hypertable_schema}' AND
            hypertable_name = '{hypertable.hypertable_name}';
        """
        result = psql(hypertable.conn, sql, snapshot=hypertable.snapshot)
        return [Chunk(**r) for r in result]

class TimescaleDB:

    def __init__(self, conn, snapshot):
        self.conn = conn
        self.snapshot = snapshot

    def _has_feature(self, feature_table: str, where: str = 'true') -> bool:
        sql = f"""
        SELECT
            count(*) > 0 as has_feature
        FROM
            timescaledb_information.{feature_table}
        WHERE
            {where};
        """
        result = psql(self.conn, sql, snapshot=self.snapshot)
        return result[0]["has_feature"] == "t"

    def has_caggs(self) -> bool:
        """
        Check the existence of continuous aggregates.
        """
        return self._has_feature("continuous_aggregates")

    def has_compression_settings(self) -> bool:
        """
        Check the existence of compressions.
        """
        return self._has_feature("compression_settings")

    def has_user_jobs(self) -> bool:
        """
        Check the existence of jobs.
        """
        return self._has_feature("jobs", where="job_id >= 1000")

    def hypertables(self) -> list[Hypertable]:
        """
        Get hypertables.
        """
        return Hypertable.fetch(self)
