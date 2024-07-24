import shutil
import logging

from telemetry import telemetry_command
from exec import run_cmd
from catalog import target

logger = logging.getLogger(__name__)


@telemetry_command("clean")
def clean(args):
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR")
    logger.info("Cleaned logical decoding artifacts from source and target database ...")

    target.clean(args.target)

    if args.prune:
        dir = str(args.dir.absolute())
        shutil.rmtree(dir, ignore_errors=True)
        logger.info(f"Pruned {dir}...")
