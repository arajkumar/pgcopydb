import shutil

from telemetry import telemetry_command
from exec import run_cmd

@telemetry_command("cleanup")
def cleanup(args):
    print("Cleaning up ...")
    run_cmd("pgcopydb stream cleanup --dir $PGCOPYDB_DIR")
    print("Done")

    if args.prune:
        dir = str(args.dir.absolute())
        print(f"Pruning {dir}...")
        shutil.rmtree(dir, ignore_errors=True)
        print("Done")

def clean(args):
    cleanup(args)