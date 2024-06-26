import threading
import typing
import logging

from pathlib import Path

logger = logging.getLogger(__name__)

class HealthCheck:
    process_logs = []
    stop_events = []
    active_threads = []
    health_check_interval = 5

    def get_filename(self, path):
        return path.split("/")[-1]

    def check_log_for_health(self, name: str, log_path: Path, is_error_func: typing.Callable):
        """
        Monitors log health by passing lines from the log file to `is_error_func`.
        - `name`: Name for check.
        - `log_path`: Path to log file.
        - `is_error_func`: Function that accepts a log line and determines whether
            it is an unhealthy/unexpected line. If the return is True, the line is
            treated as unexpected and subsequently logged.

        Optimization:
        - It monitors only the new logs that are added to the line. This is done by moving the read poiter
            to the end of the file as the file is opened. This prevents logging errors that were already
            logged in the previous session
        - Continuous scanning of file. The function opens a file and keeps scanning until a new line is found.
            This avoids reopening of files and skipping the lines that were previously scanned.
        """
        log_path = str(log_path)
        if log_path is None or log_path == "" or is_error_func is None:
            raise Exception("provide a valid log_path and health_func")
        stop_event = threading.Event()
        def checker():
            with open(log_path, "rb") as file:
                while not stop_event.is_set():
                    line = file.readline()
                    if line == b'':
                        # EOF reached. Wait for sometime and start scanning the
                        # next batch of lines.
                        stop_event.wait(self.health_check_interval)
                        continue
                    line_str = line.decode().strip()
                    if is_error_func(line_str):
                        logger.info(f"unexpected log from '<container-mount>/logs/{self.get_filename(log_path)}")
                        logger.info(f"\t{line_str}")

        thread = threading.Thread(target=checker)
        thread.start()
        self.active_threads.append(thread)
        self.stop_events.append(stop_event)

    def stop_all(self):
        logger.info("Stopping all health checkers ...")
        for e in self.stop_events:
            e.set()
        for t in self.active_threads:
            t.join()

health_checker = HealthCheck()
