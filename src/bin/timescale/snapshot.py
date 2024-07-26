import sys
import subprocess
import logging
import os

from utils import docker_command
from usr_signal import wait_for_event
from validate import validate_dbs, has_tables_without_pkey_replident
from environ import LIVE_MIGRATION_DOCKER

logger = logging.getLogger(__name__)

def warn_and_wait_for_pkey_replident(tables: list[str]):
    logger.warn("The following tables in the Source DB have neither a primary key nor a REPLICA IDENTITY (FULL/INDEX)")
    logger.warn("UPDATE and DELETE statements on these tables will not be replicated to the Target DB")
    for table in tables:
        logger.warn(f"\t- {table}")
    # Wait for the user to acknowledge the warning mentioned above before proceeding.
    print("Press 'c' and ENTER to continue")
    event = wait_for_event("c")
    event.wait()


def snapshot(args):
    if (args.dir / "snapshot").exists():
        logger.error("Snapshot file already exists.")
        logger.error("Snapshot process is either running or not cleaned up properly.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(1)

    validate_dbs(args)

    if LIVE_MIGRATION_DOCKER and not os.path.ismount(args.dir):
        logger.error("Volume mount not found. To proceed, mount a volume: '-v <host_dir>:%s'", args.dir)
        sys.exit(1)

    tables_without_pkey_replident = has_tables_without_pkey_replident()
    if len(tables_without_pkey_replident) > 0:
        warn_and_wait_for_pkey_replident(tables_without_pkey_replident)

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
        print("You can now start the migration process by running the following command:")
        print(docker_command("live-migration-migrate", "migrate"))

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

