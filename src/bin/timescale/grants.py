import logging
import re

from environ import env
from exec import run_cmd
from utils import print_logs_with_error

logger = logging.getLogger(__name__)

def dump_grants(pg_uri: str, grants_file: str):
    dump_grants_cmd = [
        "pg_dump",
        "--section=pre-data",
        "--format=plain",
        "--snapshot",
        "$(cat $PGCOPYDB_DIR/snapshot)",
        "--file",
        grants_file,
        pg_uri,
    ]
    dump_grants_cmd = " ".join(dump_grants_cmd)

    logger.info(f"Dumping grants to {grants_file}")
    run_cmd(dump_grants_cmd, f"{env['PGCOPYDB_DIR']}/logs/dump_grants")


# This regex is used to filter out the grants from the dump file
_SELECTED_GRANTS = re.compile(r'(ALTER.*OWNER.*|GRANT|REVOKE)')

def filter_grants_only(grants_file: str, grants_file_filtered: str):
    logger.info(f"Filtering grants from {grants_file} to {grants_file_filtered}")

    with open(grants_file, 'r') as file_grants:
        with open(grants_file_filtered, 'w') as file_filtered:
            for line in file_grants:
                if _SELECTED_GRANTS.match(line):
                    file_filtered.write(line)

def restore_grants(pg_uri: str, grant_file: str):
    restore_grants_cmd = [
            "psql",
            "-X",
            "-d",
            pg_uri,
            "--echo-errors",
            "-v",
            "ON_ERROR_STOP=0",
            "-f",
            grant_file,
    ]
    restore_grants_cmd = " ".join(restore_grants_cmd)

    logger.info(f"Restoring grants from {grant_file}")

    log_path = f"{env['PGCOPYDB_DIR']}/logs/restore_grants"
    run_cmd(restore_grants_cmd, log_path)
    print_logs_with_error(log_path=f"{log_path}_stderr.log", after=3, tail=0)
