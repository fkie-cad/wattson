import logging
import threading
import time
from pathlib import Path
from threading import Thread, Event, Lock

import yaml
import zmq
import ujson

from wattson.analysis.statistics.common.constants import STATISTIC_SERVER_PORT
from wattson.analysis.statistics.server.scenario_analyzer import ScenarioAnalyzer
from wattson.util.log import get_logger

from wattson.analysis.statistics.common.statistic_message import StatisticMessage


class StatisticServer(Thread):
    """
    The statistic server collects (logged) statistics from all its clients
    and stores them in a consistent format at the given location.
    To limit the memory overhead, the statistic log can be split into several files after
    a pre-defined number of entries has been collected.

    """
    def __init__(self, ip: str, **kwargs):
        self.sock = None
        self.ip = ip
        self.port = kwargs.get("port", STATISTIC_SERVER_PORT)
        self.max_size = kwargs.get("max_size", None)
        self.target_folder = kwargs.get("target_folder", Path("auto_stats"))
        self.target_folder.mkdir(mode=0o755, parents=True, exist_ok=True)

        self.server_address = f"tcp://{self.ip}:{self.port}"
        self._log = []

        self._network = kwargs.get("network")
        self._power_net = kwargs.get("power_net")
        self._data_points = kwargs.get("data_points")

        self._lock = Lock()

        self._log_file_postfix = 0
        self._log_file_name = "wattson_statistics"
        self._log_file_format = "json"

        super().__init__()
        self._stop_requested = Event()
        self._logger = get_logger("Wattson", "StatisticServer")
        self._logger.setLevel(kwargs.get("log_level", logging.INFO))

    def start(self):
        t = threading.Thread(target=self._log_general_statistics)
        t.start()
        super().start()

    def stop(self):
        # When stopping, write out any left out messages
        self.write_log()
        self._stop_requested.set()

    def run(self) -> None:
        with zmq.Context() as context:
            with context.socket(zmq.REP) as sock:
                self._logger.info(f"Listening on {self.server_address}")
                self.sock = sock
                self.sock.bind(self.server_address)

                while not self._stop_requested.is_set():
                    if self.sock.poll(1000):
                        message: StatisticMessage = self.sock.recv_pyobj()
                        self._logger.debug(f"Got message: {message.to_dict()}")
                        self.sock.send_pyobj(True)
                        self.log(message)
                        self._handle_log_size()
                self._logger.debug(f"Terminating...")

    def _handle_log_size(self):
        if self.max_size is None:
            return
        with self._lock:
            size = self._log_size()
            if size > self.max_size:
                self._write_log()
                self._clear_log()

    def log_size(self):
        with self._lock:
            return self._log_size()

    def _log_size(self):
        return len(self._log)

    def log(self, message: StatisticMessage, locked: bool = False):
        with self._lock:
            self._log.append(message.to_dict())

    def write_log(self):
        with self._lock:
            self._write_log()
            self.target_folder.chmod(0o755)
            for file in self.target_folder.glob("*"):
                file.chmod(0o755)

    def _write_log(self):
        log_file = self.target_folder.joinpath(f"{self._log_file_name}_{self._log_file_postfix}.{self._log_file_format}")
        self._logger.info(f"Writing log to {log_file}")
        try:
            start_time = time.time()
            if self._log_file_format == "yml":
                with log_file.open("w") as f:
                    yaml.dump(self._log, f)
            elif self._log_file_format == "json":
                with log_file.open("w") as f:
                    ujson.dump(self._log, f, indent=4)
            end_time = time.time()
            self._logger.info(f"Log written to {log_file} in {round(end_time - start_time, 2)}s")
        except Exception as e:
            self._logger.error(f"Could not save file: {e=}")
            self._logger.warning("Could not log to file. Logging to stdout")
            print(ujson.dumps(self._log))
        self._log_file_postfix += 1

    def _clear_log(self):
        self._log = []

    def _log_general_statistics(self):
        analyzer = ScenarioAnalyzer(self, self._power_net, self._network, self._data_points)
        analyzer.analyze()
