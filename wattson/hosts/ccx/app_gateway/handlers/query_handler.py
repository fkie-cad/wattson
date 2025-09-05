import abc
from typing import TYPE_CHECKING, Optional

from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway import AppGatewayServer


class QueryHandler(abc.ABC):
    def __init__(self, app_gateway: 'AppGatewayServer'):
        self.app_gateway = app_gateway
        self._priority = 0
        self.logger = self.app_gateway.logger.getChild(self.get_name())

    def get_name(self):
        return self.__class__.__name__

    def set_priority(self, priority: int):
        self._priority = priority

    @property
    def priority(self):
        return self._priority

    def handle(self, query: AppGatewayQuery) -> Optional[AppGatewayResponse]:
        """
        Attempts to handle the given query.
        If it is handled, the AppGatewayResponse should be returned.
        If it is not handled, None should be returned.

        Args:
            query (AppGatewayQuery):
                The received AppGatewayQuery

        Returns:
            Optional[AppGatewayResponse]: The AppGatewayResponse or None
        """
        return None
