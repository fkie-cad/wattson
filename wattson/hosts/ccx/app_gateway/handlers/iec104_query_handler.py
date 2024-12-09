import logging
from typing import Optional, TYPE_CHECKING, Dict, Any

from wattson.hosts.ccx.app_gateway.handlers.query_handler import QueryHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_async_response import AppGatewayAsyncResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse
from wattson.hosts.ccx.clients.iec104 import Iec104CCXProtocolClient
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.iec104.interface.types import COT

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway import AppGatewayServer


class Iec104QueryHandler(QueryHandler):
    def __init__(self, app_gateway: 'AppGatewayServer'):
        super().__init__(app_gateway)
        # self.logger.setLevel(logging.INFO)
        self.client: Optional[Iec104CCXProtocolClient] = self.app_gateway.ccx.get_client(CCXProtocol.IEC104)
        if self.client is not None and not isinstance(self.client, Iec104CCXProtocolClient):
            raise RuntimeError(f"Invalid client class for {CCXProtocol.IEC104}: {type(self.client)}")
        if self.client is not None:
            self.client.on("receive_data_point", self._on_receive_data_point)
            self.client.on("data_point_command_reply", self._on_data_point_command_reply)

        self._active_data_point_commands = {}

    def _register_async_response(self, action_id: str, async_response: AppGatewayAsyncResponse):
        self._active_data_point_commands.setdefault(action_id, []).append(async_response)

    def _cancel_async_response(self, action_id: str, async_response: AppGatewayAsyncResponse):
        try:
            self._active_data_point_commands[action_id].remove(async_response)
        finally:
            return True

    def _on_receive_data_point(self, client: Iec104CCXProtocolClient, data_point_identifier: str, value: Any, protocol_data: Dict):
        action_id = f"read-{data_point_identifier}"

        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id}: {async_response.reference_id}")
            async_response.resolve(AppGatewayResponse(
                successful=True,
                data={
                    "data_point_identifier": data_point_identifier,
                    "value": value,
                    "protocol_data": protocol_data
                })
            )

    def _on_data_point_command_reply(self, client: Iec104CCXProtocolClient, data_point_identifier: str, successful: bool, value: Any, protocol_data: Dict):
        action_id = f"set-{data_point_identifier}"

        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id}: {async_response.reference_id} {successful=}")
            async_response.resolve(AppGatewayResponse(
                successful=successful,
                data={
                    "data_point_identifier": data_point_identifier,
                    "value": value,
                    "protocol_data": protocol_data
                })
            )

    def get_iec_104_data_point(self, data_point_identifier: str) -> Optional[dict]:
        data_point = self.app_gateway.ccx.get_data_point(data_point_identifier)
        if data_point is None:
            return None
        protocol = self.app_gateway.ccx.get_data_point_protocol(data_point)
        if protocol != CCXProtocol.IEC104:
            return None
        return data_point

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
        # Cannot handle queries without client
        if self.client is None:
            return None

        """
        Data Point Reading Commands
        """
        if q_type == AppGatewayMessageType.READ_DATA_POINT_COMMAND:
            data_point_identifier = q_data.get("data_point_identifier")
            data_point = self.get_iec_104_data_point(data_point_identifier)
            if data_point is None:
                return None
            action_id = f"read-{data_point_identifier}"
            protocol_data = data_point["protocol_data"]
            coa = protocol_data["coa"]
            ioa = protocol_data["ioa"]
            self.logger.info(f"Triggering read of {data_point_identifier}")
            iec104point = self.client.client.get_datapoint(coa, ioa, as_dict=False)
            async_response = AppGatewayAsyncResponse()
            self._register_async_response(action_id, async_response)
            if iec104point.read():
                return async_response
            else:
                self._cancel_async_response(action_id, async_response)
            self.logger.error(f"Read rejected by IEC104 client")
            return AppGatewayResponse(successful=False, data={"error": "Read rejected by IEC104 client"})

        """
        Data Point Setting Commands
        """
        if q_type == AppGatewayMessageType.SET_DATA_POINT_COMMAND:
            data_point_identifier = q_data.get("data_point_identifier")
            data_point = self.get_iec_104_data_point(data_point_identifier)
            value = q_data.get("value")
            if data_point is None:
                return None
            action_id = f"set-{data_point_identifier}"
            protocol_data = data_point["protocol_data"]
            coa = protocol_data["coa"]
            ioa = protocol_data["ioa"]
            iec104point = self.client.client.get_datapoint(coa, ioa, as_dict=False)
            self.logger.info(f"Triggering setting of {data_point_identifier} to {value} ({iec104point.info.__class__})")
            try:
                iec104point.value = value
            except Exception as e:
                return AppGatewayResponse(successful=False, data={"error": "Could not set value on data point"})

            async_response = AppGatewayAsyncResponse()
            self._register_async_response(action_id, async_response)
            if self.client.client.send(coa, ioa, COT.ACTIVATION):
                return async_response
            else:
                self._cancel_async_response(action_id, async_response)
            self.logger.error(f"Write rejected by IEC104 client")
            return AppGatewayResponse(successful=False, data={"error": "Write rejected by IEC104 client"})

        if q_type == AppGatewayMessageType.TRIGGER_INTERROGATION:
            server_key = q_data.get("server_key")
            if server_key is None:
                return AppGatewayResponse(successful=False, data={"error": f"Triggering CI failed - no server key given"})
            self.logger.info(f"Triggering general interrogation for {server_key}")
            coa = self.client.get_coa(server_key)
            if coa is None:
                self.logger.error(f"No COA found for server {server_key}")
                return AppGatewayResponse(successful=False, data={"error": f"Triggering CI failed for {server_key} (COA not found)"})
            if self.client.client.send_C_IC(coa=coa):
                return AppGatewayResponse(successful=True)
            return AppGatewayResponse(successful=False, data={"error": f"Triggering CI failed for {server_key} ({coa})"})

        return None
