import sys
import subprocess
import logging
import os

from utils import docker_command
from validate import check_db_compatibility
from environ import LIVE_MIGRATION_DOCKER

logger = logging.getLogger(__name__)

def snapshot(args):
    if (args.dir / "snapshot").exists():
        logger.error("Snapshot file already exists.")
        logger.error("Snapshot process is either running or not cleaned up properly.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(1)

    logger.info("Running compatibility checks. This will take few seconds ...")
    report = check_db_compatibility(args=args)
    report.log()
    if report.has_errors():
        if args.ignore_compatibility_checks:
            logger.warn("Ignoring failed compatibility checks failed between source and target databases ...")
        else:
            logger.error("Live migration compatibility checks failed between source and target databases. Please resolve them to proceed.")
            sys.exit(1)

    if LIVE_MIGRATION_DOCKER and not os.path.ismount(args.dir):
        logger.error("Volume mount not found. To proceed, mount a volume: '-v <host_dir>:%s'", args.dir)
        print("To create a snapshot, run the following command:")
        print(docker_command('live-migration-snapshot', 'snapshot'))
        sys.exit(1)

    logger.info("Creating snapshot ...")
    # Clean up pid files. This might cause issues in docker environment due
    # deterministic pid values.
    (args.dir / "pgcopydb.snapshot.pid").unlink(missing_ok=True)

    dir = str(args.dir.absolute())
    snapshot_command = [
        "pgcopydb",
        "snapshot",
        "--follow",
        "--plugin",
        args.plugin,
        "--dir",
        dir,
    ]

    process = subprocess.Popen(snapshot_command,
                               stdout=subprocess.PIPE,
                               text=True)
    snapshot_id = ''
    while process.poll() is None and snapshot_id == '':
        snapshot_id = process.stdout.readline().strip()

    if snapshot_id != '':
        logger.info(f"Snapshot {snapshot_id} created successfully.")
        print("You can now start the migration process by running the "
              "following command in a separate terminal:")
        print(docker_command("live-migration-migrate", "migrate"))
        print()
        print("Please do not close this terminal, or the migration "
              "process will fail.")

        # TODO: Terminate the snapshot once the migration switches to
        # live replication.
        try:
            process.wait()
        except KeyboardInterrupt:
            process.terminate()
            process.wait()

        logger.info("Snapshot process completed.")
    else:
        logger.error("Snapshot creation failed.")
        logger.error("You may need to cleanup and retry the snapshot creation.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(process.returncode)

