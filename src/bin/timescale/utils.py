import sys
import subprocess
import tempfile

from pathlib import Path
from time import perf_counter
from urllib.parse import urlparse

from version import SCRIPT_VERSION
from environ import LIVE_MIGRATION_DOCKER, env

class timeit:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.start
        print(f"=> Completed in {self.time:.1f}s")


def create_dirs(work_dir: Path):
    global env
    (work_dir / "logs").mkdir(parents=True, exist_ok=True)
    # This is needed because cleanup doesn't support --dir
    pgcopydb_dir = Path(tempfile.gettempdir()) / "pgcopydb"
    pgcopydb_dir.mkdir(parents=True, exist_ok=True)


def docker_command(name, *args):
    if LIVE_MIGRATION_DOCKER:
        return rf"""
            docker run -it --rm --name {name} \
            -e PGCOPYDB_SOURCE_PGURI=$SOURCE \
            -e PGCOPYDB_TARGET_PGURI=$TARGET \
            -v ~/live-migration:{env['PGCOPYDB_DIR']} \
            timescale/live-migration:v{SCRIPT_VERSION} \
            {" ".join(args)}""".rstrip()
    else:
        return rf"""
            {sys.argv[0]}
            {" ".join(args)}""".rstrip()


def dbname_from_uri(uri: str) -> str:
    if "dbname=" in uri:
        parameters = uri.split()
        for param in parameters:
            key, value = param.split("=")
            if key == "dbname":
                return value
    else:
        # Input => uri: 'postgres://tsdb:abcd@a.timescaledb.io:26479/rbac_test?sslmode=require'
        # result.path[1]: '/rbac_test'
        result = urlparse(uri)
        return result.path[1:]


def print_logs_with_error(log_path: str = "", before: int = 0, after: int = 0, tail: int = 50):
    """
    Print error logs in the provided log_path along with tail
    of given number of log lines at all levels.
    """
    proc = subprocess.run(f"cat {log_path} | grep -i 'error\|warn' -A{after} -B{before}",
                                    shell=True,
                                    env=env,
                                    stderr=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    text=True)
    r = str(proc.stdout)
    if r != "":
        print(f"\n\n---------LOGS WITH ERROR FROM '{log_path}'---------")
        print(r)
        print("------------------END------------------")

    if tail > 0:
        proc = subprocess.run(f"tail -n {tail} {log_path}",
                                        shell=True,
                                        env=env,
                                        stderr=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        text=True)
        r = str(proc.stdout)
        if r != "":
            print(f"\n---------LAST {tail} LOG LINES FROM '{log_path}'---------")
            print(r)
            print("------------------END------------------")