from typing import Optional

from wattson.hosts.ccx.app_gateway.handlers.query_handler import QueryHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse


class DefaultQueryHandler(QueryHandler):
    def handle(self, query: AppGatewayQuery) -> Optional[AppGatewayResponse]:
        """
        Attempts to handle the given query.
        If it is handled, the AppGatewayResponse should be returned.
        If it is not handled, None should be returned.
        @param query: The received AppGatewayQuery
        @return: The AppGatewayResponse or None
        """
        q_type = query.query_type
        q_data = query.query_data
        if q_type == AppGatewayMessageType.REGISTRATION:
            client_name = q_data.get("client_name")
            if client_name is None:
                client_name = "generic-client"
            client_id = f"{client_name}-{self.app_gateway.next_client_id}"
            return AppGatewayResponse(successful=True, data={"client_id": client_id})

        if q_type == AppGatewayMessageType.GET_NODE_STATUS:
            return AppGatewayResponse(successful=True, data={"connection_status": self.app_gateway.ccx.get_connection_status()})

        if q_type == AppGatewayMessageType.REQUEST_DATA_POINTS:
            return AppGatewayResponse(successful=True, data={"data_points": self.app_gateway.ccx.data_points})

        if q_type == AppGatewayMessageType.REQUEST_GRID_VALUE_MAPPING:
            return AppGatewayResponse(successful=True, data={"grid_value_mapping": self.app_gateway.ccx.grid_value_mapping})

        return None
