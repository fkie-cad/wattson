import threading
import time
from typing import Optional


class MultiEvent(threading.Event):
    def __init__(self, *wrapped_events: threading.Event):
        super().__init__()
        self._wrapped_events = wrapped_events

    def monitor(self, *events):
        self._wrapped_events = events

    def wait(self, timeout: Optional[float] = None):
        start_time = time.time()
        for event in self._wrapped_events:
            passed_time = time.time() - start_time
            remaining_time = None
            if timeout is not None:
                remaining_time = timeout - passed_time
            if not event.wait(remaining_time):
                return False
        return self.is_set()

    def is_set(self) -> bool:
        for event in self._wrapped_events:
            if not event.is_set():
                return False
        return True

    def isSet(self) -> bool:
        return self.is_set()

    def set(self) -> None:
        for event in self._wrapped_events:
            event.set()

    def clear(self) -> None:
        for event in self._wrapped_events:
            event.clear()
