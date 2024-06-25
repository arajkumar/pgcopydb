import os
import logging
import sys

env = os.environ.copy()
LIVE_MIGRATION_DOCKER = env.get('LIVE_MIGRATION_DOCKER') == 'true'

logger = logging.getLogger(__name__)

def pgcopydb_init_env(args):
    global env

    if not args.source:
        logger.error("Source database URI not provided.")
        logger.error("Use environment variable PGCOPYDB_SOURCE_PGURI or --source")
        sys.exit(1)

    if not args.target:
        logger.error("Target database URI not provided.")
        logger.error("Use environment variable PGCOPYDB_TARGET_PGURI or --target")
        sys.exit(1)

    # export source and target database connection strings
    env["PGCOPYDB_SOURCE_PGURI"] = args.source
    env["PGCOPYDB_TARGET_PGURI"] = args.target
    env["SOURCE"] = args.source
    env["TARGET"] = args.target

    if args.dir:
        env["PGCOPYDB_DIR"] = str(args.dir.absolute())
