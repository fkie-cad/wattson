import json
import queue
import threading
from pathlib import Path
from typing import Optional

import pyprctl
from wattson.time import WattsonTime, WattsonTimeType
from wattson.util import get_logger


class ExportThread(threading.Thread):
    def __init__(self, export_path: Path, enable: bool = False,
                 maximum_export_interval: float = 0.5):
        super().__init__()
        self._export_path = export_path
        self._enable: bool = enable
        self._queue = queue.Queue()
        self._termination_requested: threading.Event = threading.Event()
        self._cancel_export: threading.Event = threading.Event()
        self._maximum_export_interval: float = maximum_export_interval
        self._initial_configuration_applied_event: Optional[threading.Event] = None
        self.logger = get_logger("ExportThread", "ExportThread")

    def export(self, timestamp: float, values: dict):
        if not self._enable:
            return
        self._queue.put({
            "timestamp": timestamp,
            "values": values
        })

    def is_enabled(self) -> bool:
        return self._enable

    def enable_export(self):
        self._enable = self._ensure_export_path()

    def disable_export(self):
        self._enable = False

    def start(self, wait_for_event: Optional[threading.Event] = None):
        self._termination_requested.clear()
        self._cancel_export.clear()
        self._initial_configuration_applied_event = wait_for_event
        if self.is_enabled():
            self._ensure_export_path()
        super().start()

    def stop(self, discard_queue: bool = False):
        self.disable_export()
        self._termination_requested.set()
        if discard_queue:
            self._cancel_export.set()

    def _get_filename(self, timestamp: float) -> str:
        time = WattsonTime(timestamp)
        return time.file_name(WattsonTimeType.WALL, with_milliseconds=True, with_timestamp=True)

    def run(self) -> None:
        pyprctl.set_name("W/PG/Exp")
        last_timestamp = -1
        if self._initial_configuration_applied_event is not None:
            while not self._termination_requested.is_set():
                if self._initial_configuration_applied_event.wait(2):
                    break
                self.logger.info(f"Still waiting for initial configuration event...")

        self.logger.info(f"Export initialized")

        while not self._termination_requested.is_set():
            while not self._cancel_export.is_set() or self._queue.qsize() > 0:
                try:
                    export_entry = self._queue.get(block=True, timeout=1)
                    timestamp = export_entry.get("timestamp")
                    if timestamp - last_timestamp < self._maximum_export_interval:
                        # Skip
                        continue
                    filename = f"power_grid_{self._get_filename(timestamp)}.json"
                    last_timestamp = timestamp
                    export_file = self._export_path.joinpath(filename)
                    with export_file.open("w") as f:
                        json.dump(export_entry, f)
                except queue.Empty:
                    break
                if self._queue.qsize() > 10:
                    self.logger.warning(f"Can't keep up: Queue is potentially overflowing")

    def _ensure_export_path(self) -> bool:
        if self._export_path.exists() and self._export_path.is_dir():
            return True
        if self._export_path.exists() and self._export_path.is_file():
            self.logger.error("Export path already exists, but is a file")
            return False
        try:
            self._export_path.mkdir(parents=True)
            return True
        except Exception as e:
            self.logger.error(f"Cannot create export path: {e=}")
            return False

