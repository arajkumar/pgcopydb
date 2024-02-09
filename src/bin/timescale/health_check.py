import time
import threading
import typing

class HealthCheck:
    process_logs = []
    stop_events = []
    health_check_interval = 5

    def get_filename(self, path):
        return path.split("/")[-1]

    def check_log_for_health(self, name: str, log_path: str, is_error_func: typing.Callable):
        """
        Continuously monitors log health by passing lines from the log file to `is_error_func`.
        - `name`: Name for check.
        - `log_path`: Path to log file.
        - `is_error_func`: Function that accepts a log line and determines whether
            it is an unhealthy/unexpected line. If the return is True, the line is
            treated as unexpected and subsequently logged.

        Optimization: Tracks last scanned position, ensuring only new lines are read.
        """
        if log_path is None or log_path == "" or is_error_func is None:
            raise Exception("provide a valid log_path and health_func")
        stop_event = threading.Event()
        def checker():
            last_read_line_number = 0
            while not stop_event.is_set():
                with open(log_path, "r") as file:
                    for _ in range(last_read_line_number):
                        next(file)

                    error_lines = []
                    for line in file:
                        if is_error_func(line):
                            error_lines.append(line)
                        last_read_line_number += 1

                    if len(error_lines) > 0:
                        print(f"[health:{name}] unexpected logs")
                        for err in error_lines:
                            print("\t", err)
                        print(f"For more information please grep for errors from '<container-mount>/logs/{self.get_filename(log_path)}\n")
                time.sleep(self.health_check_interval)
            print(f"[health:{name}] Stopped")

        thread = threading.Thread(target=checker)
        print(f"[health:{name}] Starting ...")
        thread.start()
        self.stop_events.append(stop_event)
        return

    def stop_all(self):
        print("Stopping health checker ...")
        for e in self.stop_events:
            e.set()
        time.sleep(self.health_check_interval+2) # Sleep for 2 more seconds so that all threads can exit.

health_checker = HealthCheck()
