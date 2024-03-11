import shutil
import logging

from telemetry import telemetry_command
from exec import run_cmd

logger = logging.getLogger(__name__)


@telemetry_command("cleanup")
def cleanup(args):
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR")
    logger.info("Cleaned logical decoding artifacts from source and target database ...")

    if args.prune:
        dir = str(args.dir.absolute())
        shutil.rmtree(dir, ignore_errors=True)
        logger.info(f"Pruned {dir}...")

def clean(args):
    cleanup(args)
