import logging
import threading
from threading import RLock, Event
from queue import Empty, Queue
from typing import Optional, Any, Callable, Dict


class AsyncLogger(logging.Logger):
    def __init__(self, name: str, logger: logging.Logger, level: Optional[int] = None, **kwargs):
        if level is None:
            level = logger.level
        self.logger = logger
        super().__init__(name, level)
        self.logger.setLevel(level)
        self._queue = Queue()
        self._worker_thread = threading.Thread(target=self._handle_logs)
        self._terminate = Event()
        self._timeout = kwargs.get("timeout", 0.5)
        self._worker_thread.start()
        if "active_contexts" in kwargs:
            self.add_contexts(kwargs.get("active_contexts", []))

    def __del__(self):
        self._terminate.set()

    def add_contexts(self, contexts):
        from wattson.util import ContextLogger
        if isinstance(self.logger, ContextLogger):
            self.logger.add_contexts(contexts)

    @property
    def level(self):
        return self.logger.level

    @level.setter
    def level(self, val):
        self.logger.level = val

    def setLevel(self, level: int):
        self.logger.setLevel(level)

    def debug(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ..., stacklevel: int = ...,
              extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "debug",
            "message": msg
        }
        self._queue.put(task)

    def info(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ..., stacklevel: int = ...,
             extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "info",
            "message": msg
        }
        self._queue.put(task)

    def warning(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ..., stacklevel: int = ...,
                extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "warning",
            "message": msg
        }
        self._queue.put(task)

    def warn(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ..., stacklevel: int = ...,
             extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        self.warning(msg, **kwargs)

    def error(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ..., stacklevel: int = ...,
              extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "error",
            "message": msg
        }
        self._queue.put(task)

    def exception(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
                  stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "exception",
            "message": msg
        }
        self._queue.put(task)

    def critical(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
                 stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ..., **kwargs: Any) -> None:
        task = {
            "method": "critical",
            "message": msg
        }
        self._queue.put(task)

    def getChild(self, suffix: str) -> 'AsyncLogger':
        child = self.logger.getChild(suffix)
        return AsyncLogger(child.name, child)

    def _handle_logs(self):
        while not self._terminate.is_set() and threading.main_thread().is_alive():
            try:
                task = self._queue.get(True, timeout=self._timeout)
                method = getattr(self.logger, task["method"])
                method(task["message"])
            except Empty:
                continue
