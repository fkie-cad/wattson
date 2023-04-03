import queue
import threading
import time
from typing import Optional


class TimeLimitedConnect:
    """
    Allows multi-threaded blocking of tasks, whereby only one task should be active at once.
    As soon as a task exceeds the time limit, the next task is allowed to run.
    A potentially long-running task is not interrupted in this case.
    """
    def __init__(self, time_limit_s: float = 0):
        self._time_limit_s = time_limit_s
        self._time_task_started = -1
        self._current_task_id: Optional[str] = None
        self._lock = threading.Lock()
        self._task_queue = queue.Queue()
        self._last_task_id = -1
        self._task_done_event = threading.Event()

    @property
    def next_task_id(self) -> str:
        with self._lock:
            self._last_task_id += 1
            return str(self._last_task_id)

    def _task_timed_out(self):
        if self._current_task_id is not None:
            task_running_for = time.time() - self._time_task_started
            timeout_in = self._time_limit_s - task_running_for
            return timeout_in <= 0
        return False

    def _check_next_task(self):
        with self._lock:
            if self._current_task_id is None or self._task_timed_out():
                self._next_task()
                return
            else:
                # Wait for timeout
                task_running_for = time.time() - self._time_task_started
                timeout_in = self._time_limit_s - task_running_for
        if timeout_in > 0:
            self._task_done_event.wait(timeout_in)

    def _wait_for_task(self, task_id: str):
        while task_id != self._current_task_id:
            self._check_next_task()

    def _next_task(self):
        self._current_task_id = None
        self._task_done_event.clear()
        try:
            next_task = self._task_queue.get()
        except queue.Empty:
            return
        self._current_task_id = next_task
        self._time_task_started = time.time()

    def start(self) -> str:
        task_id = self.next_task_id
        self._task_queue.put(task_id)
        self._wait_for_task(task_id)
        return task_id

    def done(self, task_id: str):
        with self._lock:
            if task_id != self._current_task_id:
                return
            self._current_task_id = None
            self._task_done_event.set()

