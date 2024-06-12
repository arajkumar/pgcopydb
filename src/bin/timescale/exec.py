import datetime
import signal
import subprocess
import logging
import os
import typing
import time

from pathlib import Path

from environ import env
from exception import RedactedException
from health_check import health_checker

logger = logging.getLogger(__name__)

def psql(uri: str, sql: str):
    return f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d "{uri}" -c " {sql} " """


class LogFile:
    def __init__(self, fname: str):
        time_suffix = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        log_dir = Path(env["PGCOPYDB_DIR"]) / "logs"
        self.stdout = log_dir / f"{fname}_stdout_{time_suffix}.log"
        self.stderr = log_dir / f"{fname}_stderr_{time_suffix}.log"


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
    if r:
        logger.error(f"---------LOGS WITH ERROR/WARN FROM '{log_path}'---------")
        for line in r.splitlines():
            # Let's keep this normal print as it is already wrapped b/w error log lines.
            # Normal print statements are more readable as the log lines here will themselves
            # contain the "error" substring.
            print(line)
        logger.error("------------------END------------------")

    if tail > 0:
        proc = subprocess.run(f"tail -n {tail} {log_path}",
                                        shell=True,
                                        env=env,
                                        stderr=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        text=True)
        r = str(proc.stdout)
        if r:
            logger.info(f"---------LAST {tail} LOG LINES FROM '{log_path}'---------")
            for line in r.splitlines():
                print(line)
            logger.info("------------------END------------------")


def start_process(args: list, log_file: LogFile, env=env):
    if log_file is None:
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE
    else:
        stdout = open(log_file.stdout, "w")
        stderr = open(log_file.stderr, "w")

    # shell must be True to use exec command
    process = subprocess.Popen(args, env=env,
                               stdout=stdout, stderr=stderr,
                               universal_newlines=True,
                               bufsize=1)
    return process


def run_cmd(cmd: str, log_file: LogFile = None, ignore_non_zero_code: bool = False) -> str:
    if log_file is not None:
        stdout = open(log_file.stdout, "w")
        stderr = open(log_file.stderr, "w")
    else:
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE

    with subprocess.Popen("exec " + cmd,
                           shell=True,
                           env=env,
                           stderr=stderr,
                           stdout=stdout,
                           text=True,
                           ) as process:

        try:
            out, err = process.communicate()
        except:
            os.killpg(os.getpgid(process.pid), signal.SIGINT)
            process.wait()
            raise
        else:
            retcode = process.wait()
            if retcode != 0 and not ignore_non_zero_code:
                if log_file is not None:
                    print_logs_with_error(log_path=log_file.stderr)
                cmd_name = cmd.split()[0]
                raise RedactedException(
                    f"""command '{cmd}' exited with {retcode} code.
                    stderr={err}. stdout={out}""",
                    f"{cmd_name} exited with code {retcode}")
            return str(out)
        finally:
            if log_file is not None:
                stdout.close()
                stderr.close()


def run_sql(execute_on_target: bool, sql: str) -> str:
    dest = "$PGCOPYDB_SOURCE_PGURI"
    if execute_on_target:
        dest = "$PGCOPYDB_TARGET_PGURI"
    return run_cmd(psql(dest, sql))


class Retry:
    def __init__(self, args: list[str], max_retries: int) -> None:
        self.args = args
        self.max_retries = max_retries
        self.count = 0

    def increment(self):
        self.count += 1

    def should_retry(self) -> bool:
        return self.count < self.max_retries


class Process:
    def __init__(self, args: list[str], name: str):
        self.args = args
        self.process: subprocess.Popen = None
        self.name = name
        self.store_logs = False

    def with_logging(self):
        self.store_logs = True
        return self

    def with_retry(self, retry_args: list[str], max_retries: int = 3):
        self.retry = Retry(retry_args, max_retries)
        return self

    def with_health_check(self, checker_func: typing.Callable):
        if not checker_func:
            raise Exception("checker_func cannot be None")
        self.health_check_func = checker_func

        # If health check is enabled, we should enable logging for health checker to work.
        self.with_logging()
        return self

    def wait(self) -> int:
        return self.process.wait()

    def current(self) -> subprocess.Popen:
        retcode = self.process.poll()
        if retcode is None:
            return self.process

        # The process has stopped running since retcode is not None,
        # hence we should stop the corresponding health_checker routine
        # if it exists.
        health_checker.stop(self.name)

        if retcode == 0:
            # Return early as we don't need to check the retry part as
            # everything is fine.
            return self.process

        # The retcode is not 0. Hence, if retry feature is enabled, let's retry.
        if self.retry and self.retry.should_retry():
            self.retry.increment()
            logger.warning(f"{self.name} failed. Retrying: {self.retry.count}/{self.retry.max_retries}")
            self.args = self.retry.args
            self.run()
        else:
            if self.store_logs:
                print_logs_with_error(self.log_file.stderr)
            out, err = self.process.communicate()
            cmd_name = self.args[0]
            logger.error(f"command '{cmd_name}' exited with {retcode} code.\n\nstderr={err}\n\nstdout={out}")
        return self.process

    def run(self):
        if self.store_logs:
            self.log_file = LogFile(self.name)
        self.process = start_process(args=self.args, log_file=self.log_file)
        if self.health_check_func and self.store_logs:
            health_checker.check_log_for_health(self.name, self.log_file.stderr, self.health_check_func)
        return self

    def alive(self) -> bool:
        return self.current().poll() is None

    def terminate(self):
        while self.process and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGINT)
                self.process.wait()
            except:
                # ignore all exceptions and wait for the process to terminate
                pass
