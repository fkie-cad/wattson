import json
import logging
import queue
import threading
from pathlib import Path
from typing import List, Optional

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.util import get_logger


class NotificationExportThread(threading.Thread):
    def __init__(self, base_folder: Path, allowed_topics: List[str], enabled: bool = True, logger: Optional[logging.Logger] = None):
        super().__init__()
        self.base_folder = base_folder
        self.allowed_topics = allowed_topics
        self.export_enabled = enabled
        self._export_queue = queue.Queue()
        self._termination_requested = threading.Event()
        self._export_files = {}
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("NotificationExportThread")

    def queue(self, notification: WattsonNotification):
        if not self.export_enabled:
            return
        if self.base_folder is None:
            return
        if notification.notification_topic not in self.allowed_topics:
            return
        try:
            self._export_queue.put(notification, False)
        except queue.Full:
            self.logger.error("Could not queue for export: queue is full")

    def start(self):
        self._termination_requested.clear()
        if self.base_folder is not None:
            try:
                self.base_folder.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Could not create export folder ({self.base_folder}) : {e}")
                self.base_folder = None
                return
        else:
            return
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        try:
            self.join(timeout=timeout)
        except RuntimeError:
            pass

    def run(self):
        while not self._termination_requested.is_set():
            try:
                notification = self._export_queue.get(True, timeout=1)
            except queue.Empty:
                continue

            handle = self._get_export_file_handle(notification.notification_topic)
            if handle is None:
                return
            json.dump(notification.to_dict(), handle)
            handle.write("\n")
            handle.flush()
        self._close_file_handles()

    def _get_export_file_handle(self, topic: str):
        if topic not in self._export_files:
            try:
                file = self.base_folder.joinpath(f"{topic}.jsonl")
                handle = file.open("a")
                self._export_files[topic] = handle
            except Exception:
                self.logger.error(f"Could not create notification export file for topic {topic}")
                return None
        return self._export_files[topic]

    def _close_file_handles(self):
        for topic, handle in self._export_files.items():
            try:
                handle.close()
            except Exception:
                self.logger.error(f"Could not close {topic} export file")
