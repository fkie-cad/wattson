import threading
from typing import Optional

from wattson.cosimulation.exceptions.timeout_exception import TimeoutException


class TimeLimit:
    def __init__(self, timeout_seconds: Optional[float] = None):
        self._exception = None
        self._timeout = timeout_seconds
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, callback, *args, **kwargs):
        if self._timeout is None or self._timeout <= 0:
            return self._call(callback, args, kwargs)

        t = threading.Thread(target=self._call, args=(callback, args, kwargs), daemon=True)
        t.start()
        t.join(self._timeout)
        if t.is_alive():
            raise TimeoutException(f"Function timed out after {self._timeout} seconds")
        if self._exception is not None:
            raise self._exception
        return self._result

    def _call(self, callback, args, kwargs):
        try:
            self._result = callback(*args, **kwargs)
        except Exception as e:
            self._exception = e
        return self._result
