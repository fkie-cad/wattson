import threading
from threading import Thread
from typing import Dict, Optional

from wattson.services.deployment import PythonDeployment
from wattson.util import get_logger


class WattsonEcho(PythonDeployment):
    """
    Exemplary PythonDeployment repeatedly echoing a text and optional counter while running and a configurable interval.
    """
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._interval = self.config.get("interval_seconds", 1)
        self._text = self.config.get("text", "Echo")
        self._increment = self.config.get("increment", True)
        self._current_iteration = 0

        self.logger = get_logger("WattsonEchoService")

        self._stop_requested = threading.Event()

    def start(self):
        self.logger.info("Starting Echo Service")
        self._stop_requested.clear()
        self._echo()

    def stop(self):
        self.logger.info("Stopping Echo Service...")
        self._stop_requested.set()

    def _echo(self):
        while not self._stop_requested.is_set():
            self._current_iteration += 1
            text = self._text
            if text == "":
                text = "Echo"
            if self._increment:
                text = f"{text} {self._current_iteration}"
            print(text, flush=True)
            self._stop_requested.wait(self._interval)
        self.logger.info("Stopped Echo Service")
