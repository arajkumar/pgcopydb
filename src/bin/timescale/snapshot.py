import sys
import textwrap
import logging

from exec import Process
from exception import ValidationError
from utils import docker_command
from validate import (
    check_db_compatibility,
    raise_if_volume_not_mounted,
    wal2json_with_numeric_as_string_support,
)

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

    if args.skip_compatibility_checks:
        logger.warning("Skipping compatibility checks. This may lead to unexpected failures.")
    else:
        try:
            logger.info("Running compatibility checks. This will take few seconds ...")
            report = check_db_compatibility(args=args)
            report.raise_on_error()
        except ValidationError as e:
            args.telemetry.add_exception()
            message = textwrap.dedent(str(e))
            logger.error(message)
            if args.ignore_compatibility_errors:
                message = """
                          Ignoring compatibility errors. This may lead other
                          issues or unexpected behavior.
                          """
                logger.warning(textwrap.dedent(message))
            else:
                logger.error("Please fix the compatibility issues and retry.")
                sys.exit(1)
        else:
            args.telemetry.progress("completed-compatibility-checks")


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

    if args.plugin == "wal2json":
        if wal2json_with_numeric_as_string_support(args.source):
            logger.info("Using wal2json with numeric as string support.")
            snapshot_command.append("--wal2json-numeric-as-string")
        else:
            logger.warning("wal2json does not support numeric as string. This may lead to data loss.")
    else:
        logger.warning("Using test_decoding plugin. This is not fully tested.")

    process = Process(snapshot_command, "snapshot").with_logging().run()
    snapshot_id = ''
    while process.alive() and snapshot_id == '':
        with open(process.log_file.stdout) as f:
            snapshot_id = f.readline().strip()

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
