import atexit
import argparse
import os
import tempfile
import sys
import logging
import datetime

from pathlib import Path

from utils import create_dirs
from version import SCRIPT_VERSION, nudge_user_to_update
from snapshot import snapshot
from migrate import migrate
from clean import clean
from environ import pgcopydb_init_env
from inspect_db import target_activity
from catalog import target
from validate import validate
from telemetry import Telemetry
from utils import migration_id, guess_table_jobs, guess_index_jobs

def setup_logging(work_dir: Path):
    logging.Formatter.formatTime = (lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(record.created).isoformat(sep="T", timespec="milliseconds"))
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    time_suffix = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S.%f")
    file_handler = logging.FileHandler(work_dir / f"logs/main_{time_suffix}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def main():
    parser = argparse.ArgumentParser(description='Live migration moves your PostgreSQL/TimescaleDB to Timescale Cloud with minimal downtime.', add_help=False)
    parser.add_argument('-h', '--help', action='help',
                        help='Show this help message and exit')

    parser.add_argument('-v', '--version', action='version',
                        version=SCRIPT_VERSION,
                        help='Show the version of live-migration tool')

    subparsers = parser.add_subparsers(dest='command',
                                       help='Subcommand help',
                                       title='Subcommands')

    common = argparse.ArgumentParser(add_help=False)
    def _dir_default():
        dir = os.environ.get('PGCOPYDB_DIR', f'{tempfile.gettempdir()}/pgcopydb')
        return Path(dir)

    def _pg_uri_from_env(env_var1: str, env_var2: str):
        pg_uri = os.environ.get(env_var1)
        if pg_uri is None:
            pg_uri = os.environ.get(env_var2)
        return pg_uri

    common.add_argument('-h', '--help', action='help',
                        help='Show this help message and exit')

    common.add_argument('--dir', type=Path,
                        help='Working directory',
                        default=_dir_default())
    common.add_argument('--source', type=str,
                        help='Source connection string',
                        default=_pg_uri_from_env('SOURCE', 'PGCOPYDB_SOURCE_PGURI'))
    common.add_argument('--target', type=str,
                        help='Target connection string',
                        default=_pg_uri_from_env('TARGET', 'PGCOPYDB_TARGET_PGURI'))
    common.add_argument('--migrate-across-timescaledb-versions',
                        action='store_true',
                        help='Migrate data across different TimescaleDB '
                             'versions. This flag is applicable only when '
                             'migrating data from TimescaleDB to '
                             'TimescaleDB.')
    common.add_argument('--force-timescaledb-public-schema',
                        default=False,
                        action='store_true',
                        help='Force migrate extension installed on '
                             'non-public schema in source database to '
                             'public schema in Timescale cloud.')




    # snapshot
    parser_snapshot = subparsers.add_parser('snapshot',
                                            help='Create a snapshot',
                                            parents=[common],
                                            add_help=False)
    parser_snapshot.add_argument('--plugin', type=str,
                                 help='Output plugin (Default: wal2json)',
                                 default='wal2json',
                                 choices=['wal2json', 'test_decoding'])
    parser_snapshot.add_argument('--skip-compatibility-checks',
                                 default=False,
                                 action='store_true',
                                 help='Skip compatibility checks when creating '
                                      'a snapshot. Use this flag at your own '
                                      'risk as it may lead unexpected failures'
                                      ' during migration.'
                                )
    parser_snapshot.add_argument('--ignore-compatibility-errors',
                                 default=False,
                                 action='store_true',
                                 help='Ignore the errors from the compatibility '
                                      'checks when creating a snapshot. This '
                                      'will allow the snapshot to be created '
                                      'even when the compatibility checks fail. '
                                      'Use this flag at your own risk as it may '
                                      'lead to unexpected failures during '
                                      'migration.'
                                 )

    parser_clean = subparsers.add_parser('clean', help='Clean up resources',
                                         parents=[common],
                                         add_help=False)
    parser_clean.add_argument('--prune', action='store_true', help='Prune the working directory')

    # migrate
    parser_migrate = subparsers.add_parser('migrate',
                                           help='Start the migration',
                                           parents=[common],
                                           add_help=False)
    parser_migrate.add_argument('--resume', action='store_true', help='Resume the migration')
    parser_migrate.add_argument('--drop-if-exists',
                                action='store_true',
                                help='Drop the created objects on the target '
                                'database if it exists. Useful when you want '
                                'to restart the migration from scratch and reuse '
                                'the same target instance.')
    parser_migrate.add_argument('--skip-roles', action='store_true', help='Skip roles migration')
    parser_migrate.add_argument('--table-jobs', type=str,
                                help='Number of parallel jobs to copy "existing data" from source db to target db')
    parser_migrate.add_argument('--index-jobs', type=str,
                                help='Number of parallel jobs to create indexes in target db')

    parser_migrate.add_argument('--skip-extensions', nargs='*',
                                help='Skips the given extensions during migration. Empty list skips all extensions.')
    parser_migrate.add_argument('--skip-table-data',
                                '--exclude-table-data',
                                dest='skip_table_data',
                                nargs='+',
                                help='Skips data from the given table during '
                                     'migration. However, the table schema '
                                     'will be migrated. To skip data from a '
                                     'Hypertable, you will need to specify a '
                                     'list of schema qualified chunks belonging '
                                     'to the Hypertable. Currently, this flag '
                                     'does not skip data during live replay from '
                                     'the specified table. Values for this '
                                     'flag must be schema qualified. '
                                     'Eg: --skip-table-data public.metrics public.metrics_New')
    parser_migrate.add_argument('--skip-index',
                                '--exclude-index',
                                dest='skip_index',
                                nargs='+',
                                help='Skips the given indexes during migration. '
                                     'Values for this flag must be schema '
                                     'qualified. '
                                     'Eg: --skip-index public.metrics_pkey public.Metrics_time_idx')
    parser_migrate.add_argument('--skip-hypertable-compatibility-check',
                                action='store_true',
                                help='Skip the index/constraint compatibility '
                                     'check during migration. This flag is '
                                     'applicable only when migrating data '
                                     'from Postgres to TimescaleDB.')
    parser_migrate.add_argument('--skip-hypertable-incompatible-objects',
                                action='store_true',
                                help='Automatically skip incompatible indexes '
                                     'and constraints during migration. This '
                                     'flag is applicable only when migrating '
                                     'data from Postgres to TimescaleDB.')

    # internal: for testing purposes only
    parser_migrate.add_argument('--pg-src',
                                action='store_true',
                                help=argparse.SUPPRESS,
                                default=(os.environ.get('POSTGRES_SOURCE') == "true"))

    # internal: for testing purposes only
    parser_migrate.add_argument('--pg-target',
                                action='store_true',
                                help=argparse.SUPPRESS,
                                default=(os.environ.get('POSTGRES_TARGET') == "true"))

    # inspect
    parser_inspect = subparsers.add_parser('inspect',
                               parents=[common],
                               add_help=False,
                               help='Inspect real-time migration activity on target')
    parser_inspect.add_argument('--interval', default=1, type=int,
                               help='Interval in seconds for refreshing live migration querying activity. ' \
                                    'Defaults to 1 second.')

    subparsers.add_parser('validate',
                          parents=[common],
                          help='Run pre-migration compatibility checks',
                          add_help=False)
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    pgcopydb_init_env(args)
    create_dirs(args.dir)

    def _add_jobs_args(args):
        if args.table_jobs is None:
            args.table_jobs = str(guess_table_jobs(args.target))
        if args.index_jobs is None:
            args.index_jobs = str(guess_index_jobs(args.target))

    logger = setup_logging(args.dir)

    id = migration_id(args.dir)
    logger.info(f"Running live-migration: {SCRIPT_VERSION} id: {id}")
    nudge_user_to_update()

    telemetry = Telemetry(args.command,
                          args.source,
                          args.target,
                          id,
    )
    # Write telemetry on exit
    atexit.register(telemetry.write)
    # Inject telemetry into args
    args.telemetry = telemetry

    target.init(args.target)

    match args.command:
        case 'snapshot':
            snapshot(args)
        case 'clean':
            clean(args)
        case 'migrate':
            # Telemetry by default sends event only on failure.
            # Let's enable telemetry on success as well for migrate command.
            telemetry.send_on_success()
            _add_jobs_args(args)
            exit_code = migrate(args)
            sys.exit(exit_code)
        case 'inspect':
            target_activity(args=args)
        case 'validate':
            validate(args)

if __name__ == "__main__":
    main()
