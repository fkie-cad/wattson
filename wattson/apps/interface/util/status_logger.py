import copy
import datetime
import threading
import time
from typing import Optional


class StatusLogger:
    def __init__(self, namespace: str = "status-logger", parent: Optional['StatusLogger'] = None):
        if "." in namespace:
            raise ValueError("Namespace may not contain a dot (.)")
        self.namespace = namespace
        self.parent = parent
        self._status_lock = None
        self._connection_status = None
        if self.parent is None:
            self._status_lock = threading.Lock()
            self._connection_status = {
                self.namespace: {}
            }
        else:
            self.namespace = f"{self.parent.namespace}.{self.namespace}"
            self._status_lock = self.parent._status_lock
            self._connection_status = self.parent._connection_status
            with self._status_lock:
                self._connection_status[self.namespace] = {}

    def get_child(self, namespace: str):
        return StatusLogger(namespace, parent=self)

    def get_linear_log(self):
        messages = []
        with self._status_lock:
            for message in self._connection_status.setdefault("_log", []):
                messages.append(self._linear_message(message))
        return messages

    def get_data(self, return_copy: bool = True):
        with self._status_lock:
            if return_copy:
                return copy.deepcopy(self._connection_status)
            return self._connection_status

    def set_connection_status(self, key, value):
        with self._status_lock:
            self._connection_status[self.namespace][key] = value

    def get_connection_status(self, key, default_value=None):
        with self._status_lock:
            return self._connection_status[self.namespace].get(key, default_value)

    def change_counter(self, key, change, start_value=1):
        value = self.get_connection_status(key, start_value)
        value = value + change
        self.set_connection_status(key, value)

    def increase_counter(self, key, step=1, start_value=0):
        self.change_counter(key, abs(step), start_value)

    def decrease_counter(self, key, step=1, start_value=0):
        self.change_counter(key, -abs(step), start_value)

    def log(self, message: str, m_type: str = "info"):
        with self._status_lock:
            self._connection_status.setdefault("_log", []).append({
                "namespace": self.namespace,
                "time": time.time(),
                "type": m_type.upper(),
                "message": message
            })

    def error(self, message: str):
        self.log(message, "error")

    def info(self, message: str):
        self.log(message, "info")

    def warning(self, message: str):
        self.log(message, "warning")

    def critical(self, message: str):
        self.log(message, "critical")

    def _linear_message(self, message: dict) -> str:
        dt = datetime.datetime.fromtimestamp(message["time"])
        return f"{dt.isoformat()} - {message['namespace']} - {message['type']} - {message['message']}"
