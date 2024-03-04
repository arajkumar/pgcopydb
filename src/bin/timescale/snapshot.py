import sys
import subprocess

from utils import docker_command

def snapshot(args):
    if (args.dir / "snapshot").exists():
        print("Snapshot file already exists.")
        print("Snapshot process is either running or not cleaned up properly.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(1)

    print("Creating snapshot ...")
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
        print(f"Snapshot {snapshot_id} created successfully.")
        print()
        print("You can now start the migration process by running the following command:")
        print(docker_command("live-migration-migrate", "migrate"))

        # TODO: Terminate the snapshot once the migration switches to
        # live replication.
        try:
            process.wait()
        except KeyboardInterrupt:
            process.terminate()
            process.wait()
    else:
        print("Snapshot creation failed.")
        print("You may need to cleanup and retry the snapshot creation.")
        print("Run the following command to clean up resources:")
        print(docker_command('live-migration-clean', 'clean', '--prune'))
        sys.exit(process.returncode)