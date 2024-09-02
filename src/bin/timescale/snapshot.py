import sys
import subprocess
import textwrap
import logging

from exception import ValidationError
from utils import docker_command
from validate import check_db_compatibility, raise_if_volume_not_mounted

logger = logging.getLogger(__name__)

def _snapshot(args):
    if (args.dir / "snapshot").exists():
        message = f"""
        Snapshot file already exists.
        Snapshot process is either running or not cleaned up properly.
        Run the following command to clean up resources:
        {docker_command('live-migration-clean', 'clean', '--prune')}
        """
        raise ValidationError(message)

    raise_if_volume_not_mounted(args.dir)

    logger.info("Running compatibility checks. This will take few seconds ...")
    report = check_db_compatibility(args=args)
    report.log()
    args.telemetry.progress("completed-compatibility-checks")
    if report.has_errors():
        if args.ignore_compatibility_checks:
            logger.warn("Ignoring failed compatibility checks failed between source and target databases ...")
        else:
            raise ValidationError("Compatibility checks failed. Use --ignore-compatibility-checks to ignore them.")


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

def snapshot(args):
    try:
        _snapshot(args)
        args.telemetry.mark_success()
    except Exception as e:
        args.telemetry.add_exception()
        message = textwrap.dedent(str(e))
        logger.error(message)
        sys.exit(1)
