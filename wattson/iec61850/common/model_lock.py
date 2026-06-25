import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_model import IEC61850Model


class ModelLock:
    def __init__(self, model: 'IEC61850Model'):
        self._model = model
        self._access_lock = threading.Lock()
        self._lock_counter = 0

    def __enter__(self):
        with self._access_lock:
            if self._lock_counter == 0:
                self._model.lock_model()
            self._lock_counter += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._access_lock:
            self._lock_counter -= 1
            if self._lock_counter == 0:
                self._model.unlock_model()
            if self._lock_counter < 0:
                raise ValueError("Lock counter is negative")
