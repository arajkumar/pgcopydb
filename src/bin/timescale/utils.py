import os
import sys
import tempfile
import logging

from pathlib import Path
from time import perf_counter
from urllib.parse import urlparse
from enum import Enum

from version import SCRIPT_VERSION, DOCKER_IMAGE_NAME
from environ import LIVE_MIGRATION_DOCKER, env
from exec import run_cmd, psql

logger = logging.getLogger(__name__)

class timeit:
    def __init__(self, topic: str = None):
        self.topic = topic

    def __enter__(self):
        if self.topic:
            logger.info("Begin %s", self.topic)

        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.start

        logger.info("=> Completed %s in %s",
                    self.topic,
                    seconds_to_human(self.time))


class DBType(Enum):
    POSTGRES = 1
    TIMESCALEDB = 2


def get_dbtype(uri):
    result = run_cmd(psql(uri=uri, sql="select exists(select 1 from pg_extension where extname = 'timescaledb');"))
    if result == "t\n":
        return DBType.TIMESCALEDB
    return DBType.POSTGRES


def seconds_to_human(seconds):
    intervals = [("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)]
    strings = []
    for name, interval in intervals:
        value = int(seconds // interval)
        format = "d"
        if interval == 1 and seconds % 1 > 0:
            # Retain fractional component of seconds
            format = ".3g"
            value += seconds % 1
        if value > 0 or interval == 1:
            strings.append(f"{value:{format}} {name}{'s' if value > 1 or value < 1 else ''}")
        seconds -= (value * interval)
    return " ".join(strings)


def store_val(name: str, value):
    with open(f"{env['PGCOPYDB_DIR']}/run/{name}", 'w') as file:
        file.write(str(value))


def get_stored_val(name: str):
    try:
        with open(f"{env['PGCOPYDB_DIR']}/run/{name}", 'r') as file:
            value = file.read()
        return value
    except FileNotFoundError:
        return None


def bytes_to_human(bytes):
    intervals = [("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10), ("B", 2**0)]
    for name, size in intervals:
        value = bytes // size
        if value > 0:
            return f"{value}{name}"
    return "0B"


def create_dirs(work_dir: Path):
    (work_dir / "logs").mkdir(parents=True, exist_ok=True)
    # This is needed because cleanup doesn't support --dir
    pgcopydb_dir = Path(tempfile.gettempdir()) / "pgcopydb"
    pgcopydb_dir.mkdir(parents=True, exist_ok=True)

    # To store the state of the migration as marker files
    run_dir = work_dir / "run"
    run_dir.mkdir(parents=True, exist_ok=True)


def docker_command(name, *args):
    if LIVE_MIGRATION_DOCKER:
        return rf"""
            docker run -it --rm --name {name} \
            -e PGCOPYDB_SOURCE_PGURI=$SOURCE \
            -e PGCOPYDB_TARGET_PGURI=$TARGET \
            --pid=host \
            -v ~/live-migration:{env['PGCOPYDB_DIR']} \
            {DOCKER_IMAGE_NAME}:{SCRIPT_VERSION} \
            {" ".join(args)}""".rstrip()
    else:
        python = os.path.basename(sys.executable)
        script = sys.argv[0]
        script_args = " ".join(args)
        return rf"""
                {python } {script} {script_args}""".rstrip()


def dbname_from_uri(uri: str) -> str:
    if "dbname=" in uri:
        parameters = uri.split()
        for param in parameters:
            key, value = param.split("=")
            if key == "dbname":
                return value.replace("'", "")
    else:
        # Input => uri: 'postgres://tsdb:abcd@a.timescaledb.io:26479/rbac_test?sslmode=require'
        # result.path[1]: '/rbac_test'
        result = urlparse(uri)
        return result.path[1:]
