import threading
from typing import Any, Optional, TYPE_CHECKING, Callable, Dict

from wattson.cosimulation.control.messages.wattson_response import WattsonResponse

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_server import WattsonServer


class WattsonAsyncResponse(WattsonResponse):
    def __init__(self, reference_id: int = -1):
        super().__init__(True)
        self.reference_id = reference_id
        self.client_id: Optional[str] = None
        self.wattson_server: Optional['WattsonServer'] = None
        self.resolvable: Optional[threading.Event] = threading.Event()

    def register_reference(self, client_id: str, reference_id: int):
        self.client_id = client_id
        self.reference_id = reference_id

    def get_reference_map(self) -> dict:
        return {self.client_id: self.reference_id}

    def is_successful(self) -> bool:
        return self._success

    def set_successful(self, success: bool = True):
        self._success = success

    def is_promise(self) -> bool:
        return True

    def resolve(self, response: WattsonResponse):
        self.resolvable.wait()
        self.wattson_server.resolve_async_response(self, response)

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
        response = WattsonAsyncResponse(reference_id=self.reference_id)
        if client_id is not None:
            self.client_id = client_id
        response.client_id = self.client_id
        response.data = self.data
        response._success = self._success
        response.wattson_server = None
        response.resolvable = None
        return response

