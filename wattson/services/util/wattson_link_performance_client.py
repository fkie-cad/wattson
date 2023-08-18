import json
import socket
import threading
import time
from typing import Dict, Optional

from wattson.services.deployment import PythonDeployment
from wattson.util import get_logger


class WattsonLinkPerformanceClient(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._ip = self.config.get("ip", "127.0.0.1")
        self._port = self.config.get("port", 20001)
        self._buffer_size = self.config.get("buffer_size", 1024)
        self._payload_size = self.config.get("payload_size", 1024)
        self._interval_ms = self.config.get("interval_ms", 10)
        self._expected_delay = False
        self.logger = get_logger("WattsonLinkPerformanceClient", "WattsonLinkPerformanceClient")
        self._terminate_requested = threading.Event()
        self._probing_thread: Optional[threading.Thread] = None

    def start(self):
        self.logger.info("Starting measurement client thread")
        self._terminate_requested.clear()
        self._probing_thread = threading.Thread(target=self._send_probes_worker)
        self._probing_thread.start()
        self._probing_thread.join()

    def _send_probes_worker(self):
        self.logger.info(f"Connecting to {self._ip}:{self._port}")
        client_socket = socket.socket(socket.AF_INET, type=socket.SOCK_DGRAM)

        seq = 0
        wait_duration_ms = 0

        while not self._terminate_requested.wait(wait_duration_ms * 0.001):
            ts = time.time_ns()
            data = {
                "seq": seq,
                "timestamp": ts,
                "data": {
                    "expected": self._expected_delay
                }
            }
            seq += 1
            data_str = json.dumps(data).encode("utf-8")
            try:
                client_socket.sendto(data_str, (self._ip, self._port))
            except Exception as e:
                self.logger.error(f"Sending failed: {e=}")
            duration_ms = (time.time_ns() - ts) / 1_000_000
            wait_duration_ms = self._interval_ms - duration_ms
        self.logger.info(f"Attempted to sent {seq} probing packets")

    def stop(self):
        self.logger.info("Stopping measurement client...")
        super().stop()
        self._terminate_requested.set()
        if self._probing_thread is not None:
            self._probing_thread.join()
        self.logger.info("Stopped measurement client...")
