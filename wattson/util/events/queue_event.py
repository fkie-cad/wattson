import threading
import time

from wattson.util.events.wait_event import WaitEvent


class QueueEvent(threading.Event):
    def __init__(self, max_queue_time_s: float = 5, max_wait_time_s: float = 0.2, max_queue_interval_s: float = 0):
        """
        A QueueEvent allows subscribers to queue setting this event and wait for other clients to set it as well.
        Thus, multiple queues get accumulated to a single trigger of this event
        @param max_queue_time_s: How long to wait at most before triggering this event (after at least one client requested triggering)
        @param max_wait_time_s: How long to wait for another client before triggering
        @param max_queue_interval_s: How many seconds to wait at least between clearing and triggering.
        """
        super().__init__()
        self._max_queue_time_s = max_queue_time_s
        self._max_wait_time_s = max_wait_time_s
        self._max_queue_interval_s = max_queue_interval_s
        self._last_clear: float = 0
        self._lock = threading.RLock()
        self._wait_started = 0
        self._wait_event = WaitEvent(event=self)

    def queue(self):
        if self.is_set():
            return
        with self._lock: 
            t = time.time()
            if not self._wait_event.is_waiting():
                self._wait_started = t
            time_passed = t - self._wait_started
            time_since_last_clear = t - self._last_clear
            minimum_wait = max(0.0, self._max_queue_interval_s - time_since_last_clear)
            total_time_remaining = max(0.0, self._max_queue_time_s - time_passed)
            wait_time = min(total_time_remaining, self._max_wait_time_s)
            wait_time = max(wait_time, minimum_wait)
            if wait_time <= 0:
                self.set()
            else:
                self._wait_event.start(wait_time)

    def set(self) -> None:
        with self._lock:
            if self._wait_event is not None:
                self._wait_event.cancel()
        super().set()

    def clear(self) -> None:
        with self._lock:
            if self._wait_event is not None:
                self._wait_event.cancel()
            if self.is_set():
                self._last_clear = time.time()
        super().clear()

    def is_set(self) -> bool:
        return super().is_set()

    def isSet(self) -> bool:
        return self.is_set()
