import threading
from typing import Any, Optional, TYPE_CHECKING

from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway import AppGatewayServer


class AppGatewayAsyncResponse(AppGatewayResponse):
    def __init__(self, reference_id: int = -1):
        super().__init__(True)
        self.reference_id = reference_id
        self.client_id: Optional[str] = None
        self.app_gateway: Optional['AppGatewayServer'] = None
        self.resolvable: Optional[threading.Event] = threading.Event()

    def is_successful(self) -> bool:
        return self.successful

    def set_successful(self, success: bool = True):
        self.successful = success

    def is_promise(self) -> bool:
        return True

    def resolve(self, response: AppGatewayResponse):
        self.resolvable.wait()
        self.app_gateway.resolve_async_response(self, response)

    def copy_for_sending(self) -> 'AppGatewayAsyncResponse':
        response = AppGatewayAsyncResponse(reference_id=self.reference_id)
        response.client_id = self.client_id
        response.data = self.data
        response.successful = self.successful
        response.app_gateway = None
        response.resolvable = None
        return response

