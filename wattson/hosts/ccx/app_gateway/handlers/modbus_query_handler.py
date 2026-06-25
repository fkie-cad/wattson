from typing import Optional, TYPE_CHECKING, Dict, Any

from wattson.hosts.ccx.app_gateway.handlers.query_handler import QueryHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_async_response import AppGatewayAsyncResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse
from wattson.hosts.ccx.clients.modbus import ModbusCCXProtocolClient
from wattson.hosts.ccx.protocols import CCXProtocol

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway import AppGatewayServer


class ModbusQueryHandler(QueryHandler):
    def __init__(self, app_gateway: 'AppGatewayServer'):
        super().__init__(app_gateway)
        # self.logger.setLevel(logging.INFO)
        self.client: Optional[ModbusCCXProtocolClient] = self.app_gateway.ccx.get_client(CCXProtocol.MODBUS)
        if self.client is not None and not isinstance(self.client, ModbusCCXProtocolClient):
            raise RuntimeError(f"Invalid client class for {CCXProtocol.MODBUS}: {type(self.client)}")
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

    def _on_receive_data_point(self, client: ModbusCCXProtocolClient, data_point_identifier: str, value: Any, protocol_data: Dict):
        action_id = f"read-{data_point_identifier}"

        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id}: {async_response.reference_id}")
            async_response.resolve(AppGatewayResponse(
                successful=True,
                data={
                    "data_point_identifier": data_point_identifier,
                    "value": value,
                    "protocol": client.get_protocol_name(),
                    "protocol_data": protocol_data
                })
            )

    def _on_data_point_command_reply(self, client: ModbusCCXProtocolClient, data_point_identifier: str, successful: bool, value: Any, protocol_data: Dict):
        action_id = f"set-{data_point_identifier}"

        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id}: {async_response.reference_id} {successful=}")
            async_response.resolve(AppGatewayResponse(
                successful=successful,
                data={
                    "data_point_identifier": data_point_identifier,
                    "value": value,
                    "protocol": client.get_protocol_name(),
                    "protocol_data": protocol_data
                })
            )

    def get_modbus_data_point(self, data_point_identifier: str) -> Optional[dict]:
        data_point = self.app_gateway.ccx.get_data_point(data_point_identifier)
        if data_point is None:
            return None
        protocol = self.app_gateway.ccx.get_data_point_protocol(data_point)
        if protocol != CCXProtocol.MODBUS:
            return None
        return data_point

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
            data_point = self.get_modbus_data_point(data_point_identifier)
            if data_point is None:
                return None
            action_id = f"read-{data_point_identifier}"
            value_definition = self.client.get_value_definition(data_point_identifier)
            if value_definition is None:
                return None

            self.logger.info(f"Triggering read of {data_point_identifier}")

            async_response = AppGatewayAsyncResponse()
            self._register_async_response(action_id, async_response)

            if value_definition.async_read():
                return async_response
            else:
                self._cancel_async_response(action_id, async_response)
            self.logger.error(f"Read rejected by Modbus client")
            return AppGatewayResponse(successful=False, data={"error": "Read rejected by Modbus client"})

        """
        Data Point Setting Commands
        """
        if q_type in [AppGatewayMessageType.SET_DATA_POINT_COMMAND,
                      AppGatewayMessageType.WRITE_DATA_POINT_COMMAND,
                      AppGatewayMessageType.CONTROL_DATA_POINT_COMMAND]:
            # For Modbus, SET = WRITE = CONTROL (TODO?)
            data_point_identifier = q_data.get("data_point_identifier")
            data_point = self.get_modbus_data_point(data_point_identifier)
            value = q_data.get("value")
            if data_point is None:
                return None
            value_definition = self.client.get_value_definition(data_point_identifier)
            if value_definition is None:
                return None
            action_id = f"set-{data_point_identifier}"
            self.logger.info(f"Triggering setting of {data_point_identifier} to {value})")

            async_response = AppGatewayAsyncResponse()
            self._register_async_response(action_id, async_response)
            if value_definition.async_write(value):
                return async_response
            else:
                self._cancel_async_response(action_id, async_response)

            self.logger.error(f"Write rejected by Modbus client")
            return AppGatewayResponse(successful=False, data={"error": "Write rejected by Modbus client"})

        if q_type == AppGatewayMessageType.TRIGGER_INTERROGATION:
            server_key = q_data.get("server_key")
            if server_key is None:
                return AppGatewayResponse(successful=False, data={"error": f"Triggering CI failed - no server key given"})
            self.logger.info(f"Triggering general interrogation for {server_key}")
            device_client = self.client.client.get_device_client(server_key)
            if device_client is None:
                self.logger.error("Failed to find client for requested server")
                return AppGatewayResponse(successful=False, data={"error": f"Triggering Interrogation failed for {server_key} (Device not found)"})
            if self.client.client.general_interrogation(device_client.server_id):
                return AppGatewayResponse(successful=True)
            return AppGatewayResponse(successful=False, data={"error": f"Triggering Interrogation failed for {server_key}"})

        return None
