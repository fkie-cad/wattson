import threading
import time
from typing import Callable, Any, Optional


class TimedCache:
    def __init__(self,
                 cache_refresh_callback: Callable,
                 cache_timeout_seconds: float,
                 set_initial_content: bool = False,
                 initial_content: Any = None,
                 async_refresh: bool = True):
        self._refresh = cache_refresh_callback
        self._timeout = cache_timeout_seconds
        self._last_update: Optional[float] = None
        self._content = None
        self._async_refresh = async_refresh
        self._async_lock = threading.Lock()
        if set_initial_content:
            self._last_update = time.time()
            self._content = initial_content

    def set_outdated(self):
        with self._async_lock:
            self._last_update = None

    def get_content(self, *args, **kwargs) -> Any:
        """
        Returns the cache's content. If the content is outdated, calls the refresh callback first.
        @return: The cache's (updated) content
        """
        try:
            if self._async_refresh:
                self._async_lock.acquire()
            if self.is_refresh_required():
                self._content = self._refresh()
                self._last_update = time.time()
        finally:
            if self._async_refresh:
                self._async_lock.release()

        return self._content

    def get_raw_content(self) -> Any:
        """
        Returns the cache's content. This does not update the content.
        @return: The cache's (potentially outdated) content
        """
        return self._content

    def is_refresh_required(self) -> bool:
        if self._last_update is None:
            return True
        return self._last_update + self._timeout < time.time()

    def is_up_to_date(self) -> bool:
        return not self.is_refresh_required()
