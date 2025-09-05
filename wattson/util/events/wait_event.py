import threading
from typing import Optional


class WaitEvent(threading.Event):
    """
    Triggers the internal event after the given timeout (in seconds).
    Allows to manually set, wait and check the internal event.

    """
    def __init__(self, timeout: float = 0, event: Optional[threading.Event] = None):
        super().__init__()
        self._event = event if event is not None else threading.Event()
        self._timeout = timeout
        self._cancel = threading.Event()
        self._is_timed_out: bool = False
        self._restart = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def is_waiting(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self, timeout: Optional[float] = None):
        """
        Starts the timer.

        Args:
            timeout (Optional[float], optional):
                Optionally overrides the initially given timeout
                (Default value = None)
        """
        if self._thread is None or not self._thread.is_alive():
            self._cancel.clear()
            if timeout is not None:
                self._timeout = timeout
            self._thread = threading.Thread(target=self._thread_run)
            self._thread.start()
        else:
            if timeout is not None:
                self._timeout = timeout
            self._restart.set()
            self._cancel.set()

    def cancel(self):
        """Cancels the waiting immediately which does NOT fire the internal event"""
        self._restart.clear()
        self._cancel.set()

    def complete(self):
        """Cancels the waiting immediately and fires the internal event."""
        self._cancel.set()
        self._event.set()

    def _thread_run(self):
        while True:
            if not self._cancel.wait(self._timeout):
                self._is_timed_out = True
                self._event.set()
                break
            else:
                if not self._restart.is_set():
                    break
                self._restart.clear()
                self._cancel.clear()

    def set(self):
        self._cancel.set()
        self._event.set()

    def clear(self):
        self._cancel.clear()
        self._event.clear()

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._event.wait(timeout=timeout)

    def is_set(self):
        return self._event.is_set()

    def timed_out(self) -> bool:
        return self._is_timed_out
