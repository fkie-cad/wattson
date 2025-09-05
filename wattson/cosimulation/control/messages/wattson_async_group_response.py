import threading
from typing import Any, Optional, TYPE_CHECKING, Callable, Dict

from wattson.cosimulation.control.messages.wattson_async_response import WattsonAsyncResponse
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_server import WattsonServer


class WattsonAsyncGroupResponse(WattsonAsyncResponse):
    """
    The WattsonAsyncGroupResponse is a WattsonAsyncResponse that can send identical data to multiple clients.
    This is used for static queries that contain a larger data model.
    In this case, a single notification is sent to indicate the resolved queries instead of several individual notifications.
    """
    def __init__(self, group_key: Any):
        super().__init__()
        self.group_key = group_key
        self.reference_map = {}
        self.wattson_server: Optional['WattsonServer'] = None
        self.resolvable: Optional[threading.Event] = threading.Event()
        self._modify_lock = threading.Lock()
        self.is_resolving = False
        self.group_wait_delay: Optional[float] = None

    def register_reference(self, client_id: str, reference_id: int) -> bool:
        """
        Warning: This is only safe to call if you have blocked the response previously!

        Args:
            client_id: The client ID of the client that should receive the response.
            reference_id: The reference ID of the response, linking to the query.

        Returns:
            True if the response could be registered, False otherwise.

        """
        self.reference_map[client_id] = reference_id
        return True

    def block(self, timeout: float = 1) -> bool:
        """
        Blocks this response from being resolved.

        Returns:
            bool: True if the response could be blocked, False otherwise.
        """
        return self._modify_lock.acquire(blocking=True, timeout=timeout)

    def unblock(self):
        """
        Remove the blocking of this response.
        """
        self._modify_lock.release()

    def has_response(self, client_id: str) -> bool:
        return client_id in self.reference_map

    def get_reference(self, client_id: str) -> Optional[int]:
        return self.reference_map.get(client_id)

    def get_reference_map(self) -> dict:
        return self.reference_map.copy()

    def is_successful(self) -> bool:
        return self._success

    def set_successful(self, success: bool = True):
        self._success = success

    def is_promise(self) -> bool:
        return True

    def resolve(self, response: WattsonResponse):
        self.resolvable.wait()
        with self._modify_lock:
            self.is_resolving = True
            self.wattson_server.resolve_async_response(self, response)

    def is_resolvable(self) -> bool:
        return self.resolvable.is_set()

    def resolve_with_task(self, resolve_task: Callable[['WattsonAsyncResponse', Dict], WattsonResponse], further_kwargs: Optional[Dict] = None):
        """
        Calls the given function (resolve_task) in a new thread to allow for asynchronous resolution.
        After the resolve_task finishes and returns a WattsonResponse, this WattsonAsyncResponse is automatically resolved.

        Args:
            resolve_task (Callable[['WattsonAsyncResponse', Dict], WattsonResponse]):
                The callable to call for deriving the full WattsonResponse
            further_kwargs (Optional[Dict], optional):
                A dict with further arguments to pass to the resolve_task function (as a dict!). If None is given, an empty dict is created.
                (Default value = None)
        """
        if further_kwargs is None:
            further_kwargs = {}

        def async_resolve(_resolve_task, custom_kwargs):
            self.resolvable.wait()
            response = _resolve_task(self, custom_kwargs)
            self.resolve(response)

        t = threading.Thread(target=async_resolve, args=(resolve_task, further_kwargs))
        t.start()

    def copy_for_sending(self, client_id: Optional[str] = None) -> 'WattsonAsyncResponse':
        """
        When sending the object, instances of complex classes (e.g., the WattsonServer or a threading.Event) are removed.
        """
        if client_id is None:
            raise ValueError("Client ID has to be specified before sending an async group response")

        reference_id = self.reference_map.get(client_id)
        response = WattsonAsyncResponse(reference_id)
        response.client_id = client_id
        response.data = self.data
        response._success = self._success
        response.wattson_server = None
        response.resolvable = None
        return response
