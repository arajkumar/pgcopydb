import shutil
import logging

from telemetry import telemetry_command
from exec import run_cmd, run_sql

logger = logging.getLogger(__name__)


@telemetry_command("cleanup")
def cleanup(args):
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR")
    logger.info("Cleaned logical decoding artifacts from source and target database ...")

    if args.prune:
        dir = str(args.dir.absolute())
        shutil.rmtree(dir, ignore_errors=True)
        logger.info(f"Pruned {dir}...")
        # Dropping pgcopydb schema is under '--prune' since '--prune' flag is used when
        # the user no longer intends to resume the migration process.
        run_sql(execute_on_target=True, sql="drop schema if exists pgcopydb cascade")

def clean(args):
    cleanup(args)
