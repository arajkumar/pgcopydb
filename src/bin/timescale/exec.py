import datetime
import subprocess
import threading
import logging

from pathlib import Path

from environ import env
from exception import RedactedException

logger = logging.getLogger(__name__)

def psql(uri: str, sql: str):
    return f"""psql -X -A -t -v ON_ERROR_STOP=1 --echo-errors -d "{uri}" -c " {sql} " """


INIT_TIME = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

class LogFile:
    def __init__(self, fname: str):
        log_dir = Path(env["PGCOPYDB_DIR"]) / "logs"
        self.stdout = log_dir/f"{fname}_stdout_{INIT_TIME}.log"
        self.stderr = log_dir/f"{fname}_stderr_{INIT_TIME}.log"


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
        logger.error(f"---------LOGS WITH ERROR FROM '{log_path}'---------")
        for line in r.splitlines():
            logger.error(line)
        logger.error("------------------END------------------")

    if tail > 0:
        proc = subprocess.run(f"tail -n {tail} {log_path}",
                                        shell=True,
                                        env=env,
                                        stderr=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        text=True)
        r = str(proc.stdout)
        if r != "":
            logger.info(f"---------LAST {tail} LOG LINES FROM '{log_path}'---------")
            for line in r.splitlines():
                logger.info(line)
            logger.info("------------------END------------------")


class Command:
    def __init__(self, command: str = "", env=env, use_shell: bool = False, log_file: LogFile = None):
        self.command = command
        self.stderr_path = ""
        if log_file == None:
            self.process = subprocess.Popen("exec " + self.command, shell=use_shell, env=env)
        else:
            f_stdout = open(log_file.stdout, "w")

            self.stderr_path = log_file.stderr
            f_stderr = open(log_file.stderr, "w")

            self.process = subprocess.Popen("exec " + self.command, shell=use_shell, env=env,
                                            stdout=f_stdout, stderr=f_stderr,
                                            universal_newlines=True,
                                            bufsize=1)
        self.wait_thread = threading.Thread(target=self.process_wait)
        self.wait_thread.start()

    def process_wait(self):
        self.process.wait()
        code = self.process.returncode
        if code not in {-9, 0, 12}:
            # code -9 is for SIGKILL.
            # We ignore process execution that stops for SIGKILL since these are signals
            # given by self.terminate() and self.kill(). pgcopydb often requires these signals to stop.
            #
            # If we do not give these signals during the program execution (as and when required),
            # then pgcopydb hangs indefinitely (for some reason) even if it has completed its execution.
            #
            # code 12 is received when `pgcopydb follow`'s internal child processes exit due to an external signal.
            # We believe ignoring this code is safe.
            if self.stderr_path != "":
                print_logs_with_error(log_path=self.stderr_path)
            cmd_name = self.command.split()[0]
            raise RedactedException(
                f"command '{self.command}' exited with {self.process.returncode} code stderr={self.process.stderr}",
                f"{cmd_name} exited with code {self.process.returncode}")

    def wait(self):
        """
        wait waits for the process to complete with a zero code.
        """
        self.wait_thread.join()

    def terminate(self):
        self.process.terminate()

    def kill(self):
        self.process.kill()


def run_cmd(cmd: str, log_file: LogFile = None, ignore_non_zero_code: bool = False) -> str:
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    if log_file != None:
        stdout = open(log_file.stdout, "w")
        stderr = open(log_file.stderr, "w")
    result = subprocess.run(cmd, shell=True, env=env, stderr=stderr, stdout=stdout, text=True)
    if result.returncode != 0 and not ignore_non_zero_code:
        if log_file != None:
            print_logs_with_error(log_path=log_file.stderr)
        cmd_name = cmd.split()[0]
        raise RedactedException(
            f"command '{cmd}' exited with {result.returncode} code. stderr={result.stderr}. stdout={result.stdout}",
            f"{cmd_name} exited with code {result.returncode}")
    return str(result.stdout)


def run_sql(execute_on_target: bool, sql: str):
    dest = "$PGCOPYDB_SOURCE_PGURI"
    if execute_on_target:
        dest = "$PGCOPYDB_TARGET_PGURI"
    return run_cmd(psql(dest, sql))
