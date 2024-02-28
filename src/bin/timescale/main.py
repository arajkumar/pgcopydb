import argparse
import os
import tempfile
import sys

from pathlib import Path

from utils import create_dirs
from version import SCRIPT_VERSION
from snapshot import snapshot
from migrate import migrate
from clean import clean
from environ import pgcopydb_init_env

def main():
    parser = argparse.ArgumentParser(description='''Live migration moves your PostgreSQL/TimescaleDB to Timescale Cloud with minimal downtime.''', add_help=False)
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
        dir = os.environ.get('PGCOPYDB_DIR', '')
        if not dir:
            dir = f'{tempfile.gettempdir()}/pgcopydb'
        return Path(dir)
    common.add_argument('-h', '--help', action='help',
                        help='Show this help message and exit')

    common.add_argument('--dir', type=Path,
                        help='Working directory',
                        default=_dir_default())

    # snapshot
    parser_snapshot = subparsers.add_parser('snapshot',
                                            help='Create a snapshot',
                                            parents=[common],
                                            add_help=False)
    parser_snapshot.add_argument('--plugin', type=str,
                                 help='Output plugin (Default: wal2json)',
                                 default='wal2json',
                                 choices=['wal2json', 'test_decoding'])

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
    # TODO: Remove this once we know the existing customers who are still
    # using the old migration image < v0.0.5.
    parser_migrate.add_argument('--skip-initial-data',
                                action='store_true',
                                help=('Skip initial data migration. '
                                      'This is provided for backward '
                                      'compatibility with the old migration '
                                      'process which did not support granular '
                                      'resume over the section.'))

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    pgcopydb_init_env(args)
    create_dirs(args.dir)

    match args.command:
        case 'snapshot':
            snapshot(args)
        case 'clean':
            clean(args)
        case 'migrate':
            migrate(args)


if __name__ == "__main__":
    main()