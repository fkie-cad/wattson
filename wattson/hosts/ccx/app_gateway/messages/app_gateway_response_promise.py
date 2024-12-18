import threading
from typing import Optional, Callable, List, TYPE_CHECKING

from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery


class AppGatewayResponsePromise(AppGatewayResponse):
    def __init__(self, query: 'AppGatewayQuery', resolve_event: threading.Event):
        super().__init__(True)
        self._query = query
        self._resolve_event = resolve_event
        self._on_resolve_callbacks: List[Optional[Callable[[AppGatewayResponse], None]]] = []
        self._watchdog_thread: Optional[threading.Thread] = None

    def is_promise(self) -> bool:
        return True

    def trigger_resolve(self):
        self._resolve_event.set()

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
        """
        self._resolve_event.wait(timeout=timeout)
        return self.is_resolved()

    def get_response(self) -> Optional[AppGatewayResponse]:
        """
        If the associated query has been resolved, the respective WattsonResponse is returned.
        Otherwise, None is returned.
        """
        if not self.is_resolved():
            return None
        return self._query.response

    def on_resolve(self, callback: Callable[[AppGatewayResponse], None]):
        """
        Adds a callback to be called as soon as the promise resolves.
        @param callback: The callback to call
        @return:
        """
        self._on_resolve_callbacks.append(callback)
        self._start_watchdog()

    def raise_exception_on_fail(self):
        """
        As soon as the response resolves, it is checked for its success.
        In case it is not successful, an exception is raised.
        @return:
        """
        def callback(response):
            if not response.successful():
                raise Exception(f"Query of type {self._query.query_type} failed")
        self.on_resolve(callback)

    def _watchdog(self):
        self._resolve_event.wait()
        for callback in self._on_resolve_callbacks:
            callback(self.get_response())

    def _start_watchdog(self):
        if self._watchdog_thread is None:
            self._watchdog_thread = threading.Thread(target=self._watchdog)
            self._watchdog_thread.start()

