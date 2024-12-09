from typing import Optional, Any

from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse


class AppGatewayQuery:
    def __init__(self, query_type: AppGatewayMessageType, query_data: Any = None):
        self._handled: int = 0
        self.query_type: Optional[AppGatewayMessageType] = query_type
        self.query_data: Any = query_data
        if self.query_data is None:
            self.query_data = {}
        self.response: Optional[AppGatewayResponse] = None
        self.client_id: Optional[str] = None

    def has_response(self) -> bool:
        return self.response is not None

    def has_successful_response(self) -> bool:
        return self.has_response() and self.response.is_successful()

    def mark_as_handled(self):
        self._handled += 1

    def is_handled(self):
        return self._handled >= 1

    def respond(self, successful: bool, data: dict):
        self.response = AppGatewayResponse(successful=successful, data=data)

    def is_successful(self) -> bool:
        if not self.has_response():
            return False
        return self.response.is_successful()

    def add_response(self, response: 'AppGatewayResponse'):
        self.response = response
