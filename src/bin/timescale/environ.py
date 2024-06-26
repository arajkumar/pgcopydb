import os

env = os.environ.copy()
LIVE_MIGRATION_DOCKER = env.get('LIVE_MIGRATION_DOCKER') == 'true'

def pgcopydb_init_env(args):
    global env

    for key in ["PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"]:
        if key not in env or env[key] == "" or env[key] is None:
            raise ValueError(f"${key} not found")

    if args.dir:
        env["PGCOPYDB_DIR"] = str(args.dir.absolute())