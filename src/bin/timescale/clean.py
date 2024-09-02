import logging
import os
from pathlib import Path

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
        # Prune all files except logs directory. This is useful when we want to
        # keep logs for debugging purposes.
        log_dir = str(Path(dir, "logs"))
        for path, subdirs, files in os.walk(dir, topdown=False):
            for f in files:
                # skip files in logs directory
                f = Path(path, f)
                if str(f).startswith(log_dir):
                    continue
                f.unlink()

            for dir in subdirs:
                dir = Path(path, dir)
                # skip logs directory
                if str(dir) == log_dir:
                    continue
                dir.rmdir()
        logger.info("Pruned all files except logs directory ...")
