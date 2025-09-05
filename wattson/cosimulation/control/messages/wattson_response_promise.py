import threading
from typing import Optional, Callable, List

from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.exceptions import WattsonClientException


class WattsonResponsePromise(WattsonResponse):
    def __init__(self, query: WattsonQuery, resolve_event: threading.Event):
        super().__init__(True)
        self._query = query
        self._resolve_event = resolve_event
        self._on_resolve_callbacks: List[Optional[Callable[[WattsonResponse], None]]] = []
        self._watchdog_thread: Optional[threading.Thread] = None

    def is_promise(self) -> bool:
        return True

    def trigger_resolve(self):
        self._resolve_event.set()

    @property
    def query(self):
        return self._query

    @property
    def resolve_event(self):
        return self._resolve_event

    def is_resolved(self) -> bool:
        """
        Checks whether the associated query has already been resolved

        """
        return self._resolve_event.is_set()

    def resolve(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the query to resolve.
        If a timeout is given, the waiting is stopped after this timeout (in seconds).
        Returns True iff the query has been resolved.

        Args:
            timeout (Optional[float], optional):
                
                (Default value = None)
        """
        return self._resolve_event.wait(timeout=timeout)

    def get_response(self) -> Optional[WattsonResponse]:
        """
        If the associated query has been resolved, the respective WattsonResponse is returned.
        Otherwise, None is returned.

        """
        if not self.is_resolved():
            return None
        return self._query.response

    def on_resolve(self, callback: Callable[[WattsonResponse], None]):
        """
        Adds a callback to be called as soon as the promise resolves.

        Args:
            callback (Callable[[WattsonResponse], None]):
                The callback to call
        """
        self._on_resolve_callbacks.append(callback)
        self._start_watchdog()

    def raise_exception_on_fail(self):
        """
        As soon as the response resolves, it is checked for its success.
        In case it is not successful, an exception is raised.

        """
        def callback(response):
            if not response.is_successful():
                raise WattsonClientException(f"Query of type {self._query.query_type} failed")
        self.on_resolve(callback)

    def _watchdog(self):
        self._resolve_event.wait()
        for callback in self._on_resolve_callbacks:
            callback(self.get_response())

    def _start_watchdog(self):
        if self._watchdog_thread is None:
            self._watchdog_thread = threading.Thread(target=self._watchdog, daemon=True)
            self._watchdog_thread.start()

