import threading
from typing import Any, Optional, TYPE_CHECKING

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

    def is_successful(self) -> bool:
        return self._success

    def set_successful(self, success: bool = True):
        self._success = success

    def is_promise(self) -> bool:
        return True

    def resolve(self, response: WattsonResponse):
        self.resolvable.wait()
        self.wattson_server.resolve_async_response(self, response)

    def copy_for_sending(self) -> 'WattsonAsyncResponse':
        response = WattsonAsyncResponse(reference_id=self.reference_id)
        response.client_id = self.client_id
        response.data = self.data
        response._success = self._success
        response.wattson_server = None
        response.resolvable = None
        return response

