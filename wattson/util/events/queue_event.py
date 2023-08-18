import threading
import time

from wattson.util.events.wait_event import WaitEvent


class QueueEvent(threading.Event):
    def __init__(self, max_queue_time_s: float = 5, max_queue_interval_s: float = 0.2):
        """
        A QueueEvent allows subscribers to queue setting this event and wait for other clients to set it as well.
        Thus, multiple queues get accumulated to a single trigger of this event
        @param max_queue_time_s: How long to wait at most before triggering this event
        @param max_queue_interval_s: How long to wait for another client before triggering
        """
        super().__init__()
        self._max_queue_time_s = max_queue_time_s
        self._max_queue_interval_s = max_queue_interval_s
        self._lock = threading.Lock()
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
            total_time_remaining = max(0.0, self._max_queue_time_s - time_passed)
            wait_time = min(total_time_remaining, self._max_queue_interval_s)
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
        super().clear()

    def is_set(self) -> bool:
        return super().is_set()

    def isSet(self) -> bool:
        return self.is_set()
