"""
Handles the migration of hypertables from one TimescaleDB version to another.
"""

import logging
import subprocess

from multiprocessing import Pool

from psql import psql as psql_cmd
from utils import get_snapshot_id
from .timescaledb import TimescaleDB

logger = logging.getLogger(__name__)


def _migrate_hypertable_config(hypertables, target):
    stmts = []
    for hypertable in hypertables:
        for dimension in hypertable.dimensions():
            table = hypertable.fq_table_name
            if dimension.time_interval:
                interval_def = f"'{dimension.time_interval}'::interval"
            elif dimension.integer_interval:
                interval_def = f"{dimension.integer_interval}::integer"
            elif dimension.integer_now_func:
                interval_def = f"partition_func => {dimension.integer_now_func}"
            else:
                interval_def = None

            if dimension.dimension_type.lower() == 'time':
                if interval_def:
                    dim = f"by_range('{dimension.column_name}', {interval_def})"
                else:
                    raise ValueError("Time dimension must have an interval definition.")
            else:
                # Space dimension
                if interval_def is None:
                    dim = f"by_hash('{dimension.column_name}', {dimension.num_partitions})"
                else:
                    dim = f"by_hash('{dimension.column_name}', {dimension.num_partitions}, {interval_def})"

            if dimension.dimension_number == 1:
                # TODO: Migrate associated_schema_name and associated_table_prefix.
                stmt = f"""
                    SELECT create_hypertable('{table}', {dim}, migrate_data => false, create_default_indexes => false, if_not_exists => true);
            """
            else:
                stmt = f"SELECT add_dimension('{table}', {dim}, if_not_exists => true);"

            stmts.append(stmt)

    sql = f"""
    BEGIN;
    {"".join(stmts)}
    COMMIT;
    """
    result = psql_cmd(conn=target, sql=sql)
    if len(result) < 1:
        raise ValueError("Failed to migrate hypertable configuration.")

def _psql_process(conn, sql, *args, **kwargs):
    return subprocess.Popen(
            ['psql',
             '--no-align',
             '--quiet',
             '--dbname',
             conn,
             '--command',
             sql
            ], *args, **kwargs)

def _migrate_hypertable_data(args, hypertable):
    # We want to mimic the following behavior using subprocess
    # psql -d $SOURCE -c "COPY (SELECT * FROM hypertable) TO STDOUT" | psql -d $TARGET -c "COPY hypertable FROM STDIN"
    logger.info(f"Migrating data for hypertable {hypertable} ...")

    # First truncate the target hypertable
    # TODO: We can avoid truncating the hypertable whos data is already
    # copied by tracking the copy status in __live_migration.copy_status
    hypertable.truncate(args.target)

    snapshot_id = get_snapshot_id(args.dir)
    copy_from = f"""
    BEGIN;
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, DEFERRABLE;
    SET TRANSACTION SNAPSHOT '{snapshot_id}';
    COPY (SELECT * FROM {hypertable.fq_table_name}) TO STDOUT;
    COMMIT;
    """

    source_psql = _psql_process(args.source, copy_from, stdout=subprocess.PIPE)
    target_psql = _psql_process(args.target,
                                f"COPY {hypertable.fq_table_name} FROM STDIN",
                                stdin=source_psql.stdout)
    # Close the source_psql stdout to signal the end of the data stream
    source_psql.stdout.close()
    # Wait for the target_psql to finish
    try:
        target_psql.communicate()
    except KeyboardInterrupt:
        source_psql.kill()
        source_psql.wait()
        target_psql.kill()
        target_psql.wait()
        raise

class CrossVersionMigration:
    def __init__(self, args):
        self.args = args
        self.snapshot_id = get_snapshot_id(args.dir)
        self.timescaledb = TimescaleDB(args.source, self.snapshot_id)
        self.hypertables = self.timescaledb.hypertables()

    def validate(self):
        has_errors = False

        # TODO: Add support for migrating compression settings.
        if self.timescaledb.has_compression_settings():
            logger.error("Source TimescaleDB has compression settings. Compression settings will not be migrated.")
            has_errors = True

        # TODO: Add support for migrating user defined jobs like
        # retention policies, background jobs, etc.
        if self.timescaledb.has_user_jobs():
            logger.warn("Source TimescaleDB has jobs. Jobs will not be migrated.")

        # TODO: Add support for migrating continuous aggregates.
        if self.timescaledb.has_caggs():
            logger.error("Source TimescaleDB has continuous aggregates. Continuous aggregates will not be migrated.")
            has_errors = True

        for ht in self.hypertables:
            for dimension in ht.dimensions():
                if dimension.dimension_type.lower() not in ('time', 'space'):
                    logger.error(f"Dimension {dimension} is not supported.")
                    has_errors = True

        if has_errors:
            logger.error("Errors found in source configuration.")
            return False

        return True

    def migrate_hypertable_schema(self):
        # Since we filter only the chunks, not the root table(hypertable), pgcopydb
        # post-data migrates the ts_insert_blocker trigger to the target.
        # We need to drop the trigger on the target hypertable to avoid failure
        # when creating the hypertable using create_hypertable function.
        self._drop_hypertable_insert_blocker()

        # Migrate logical hypertable schema.
        _migrate_hypertable_config(self.hypertables, self.args.target)

    def migrate_hypertable_data(self):
        # Migrate data for each hypertable using parallel processes.
        # TODO: Currently the parallelism is at the hypertable level. We can
        # further parallelize the data migration by chunk. However, we need to
        # pre-create the chunks on the target before migrating the data.
        with Pool(processes=int(self.args.table_jobs)) as pool:
            pool.starmap(_migrate_hypertable_data, [(self.args, ht) for ht in self.hypertables])

    def _drop_hypertable_insert_blocker(self):
        for hypertable in self.hypertables:
            fq_table_name = hypertable.fq_table_name
            sql = f"""
                DROP TRIGGER IF EXISTS ts_insert_blocker ON {fq_table_name};
            """
            psql_cmd(conn=self.args.target, sql=sql)
