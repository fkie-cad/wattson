import logging
from typing import TYPE_CHECKING, Any, Optional, Dict, List

from pymodbus.pdu import ModbusPDU

from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.protocols.modbus.modbus_client import ModbusClient
from wattson.protocols.modbus.model.modbus_client_value_definition import ModbusClientValueDefinition
from wattson.protocols.modbus.model.modbus_device_client import ModbusDeviceClient
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.protocols.tls.tls_configuration import TlsConfiguration

if TYPE_CHECKING:
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class ModbusCCXProtocolClient(CCXProtocolClient):
    def __init__(self, ccx: 'ControlCenterExchangeGateway', tls_configurations: Optional[Dict[str, TlsConfiguration]] = None):
        super().__init__(ccx, tls_configurations=tls_configurations)
        self.data_points: dict = self.ccx.protocol_info[CCXProtocol.MODBUS]
        self._data_point_list: List[dict] = []
        server_data = {}

        self.logger = self.ccx.logger.getChild("Modbus")
        self.logger.setLevel(logging.INFO)
        # Add servers
        for dp_id, dp in self.data_points.items():
            self._data_point_list.append(dp)
            server_id = dp["protocol_server_id"]
            if server_id in server_data:
                continue
            server = self.get_server(server_id)
            if server is None:
                self.logger.warning(f"No server with ID/Key {server_id} found - cannot create client for {dp_id}")
                # raise KeyError(f"No server with ID/Key {server_id} found")
                continue
            server_data[server_id] = server
        self.known_servers = set(server_data.keys())
        # Create client
        self.client = ModbusClient(
            server_data=server_data,
            logger=self.logger.getChild("Client"),
        )
        self.client.set_on_connection_change_callback(self._on_connection_change)

        self.client.set_on_send_pdu_callback(self._on_send_pdu)
        self.client.set_on_receive_pdu_callback(self._on_receive_pdu)

        self.client.set_on_read_callback(self._on_read_request_done)
        self.client.set_on_write_callback(self._on_write_request_done)
        self.client.set_on_value_update_callback(self._on_data_point_update)

        self.client.set_data_points(self._data_point_list)

    def get_default_port(self):
        return 502

    def get_default_tls_port(self):
        return 802

    def get_protocol(self) -> CCXProtocol:
        return CCXProtocol.MODBUS

    def start(self):
        self.client.start(do_general_interrogation=True, enable_polling=True)

    def stop(self):
        self.client.stop()

    def get_value_definition(self, data_point_identifier: str) -> Optional[ModbusClientValueDefinition]:
        return self.client.get_value_definition(data_point_identifier)

    def send_data_point_command(self, data_point_identifier: str, value: Any, protocol_options: Optional[Dict] = None):
        pass

    def _on_receive_pdu(self, device_client: ModbusDeviceClient, pdu: ModbusPDU):
        server_key = device_client.server_id
        raw_data = {
            "pdu": pdu
        }
        raw_data_info = {
            "protocol": CCXProtocol.MODBUS
        }
        server_ip = device_client.server_address
        server_port = device_client.server_port
        self.trigger_on_receive_packet(server_key, server_ip, server_port, raw_data, raw_data_info)

    def _on_send_pdu(self, device_client: ModbusDeviceClient, pdu: ModbusPDU):
        server_key = device_client.server_id
        raw_data = {
            "pdu": pdu
        }
        raw_data_info = {
            "protocol": CCXProtocol.MODBUS,
            "device_id": device_client.server_id,
            "unit_id": pdu.dev_id,
            "function": pdu.function_code,
            "address": pdu.address,
            "count": pdu.count,
        }
        server_ip = device_client.server_address
        server_port = device_client.server_port
        self.trigger_on_send_packet(server_key, server_ip, server_port, raw_data, raw_data_info)

    def _on_read_request_done(self,
                              value_definition: ModbusClientValueDefinition, success: bool,
                              error_code: Optional[int], raw_value: List[int] | List[bool], value: ModbusValueType):
        protocol_data = {
            "protocol": CCXProtocol.MODBUS,
            "read_request": True,
            "write_request": False,
            "device_id": value_definition.unit_client.device_client.server_id,
            "unit_id": value_definition.unit_client.unit_id,
            "table": value_definition.modbus_table.name,
            "address": value_definition.start_address,
            "count": value_definition.register_width,
            "memory": raw_value,
            "error_code": error_code,
        }
        self.trigger_on_data_point_command_reply(
            data_point_identifier=value_definition.data_point_identifier,
            successful=success,
            value=value,
            protocol_data=protocol_data
        )

    def _on_write_request_done(self,
                               value_definition: ModbusClientValueDefinition, success: bool,
                               error_code: Optional[int], raw_value: List[int] | List[bool], value: ModbusValueType):
        protocol_data = {
            "protocol": CCXProtocol.MODBUS,
            "read_request": False,
            "write_request": True,
            "device_id": value_definition.unit_client.device_client.server_id,
            "unit_id": value_definition.unit_client.unit_id,
            "table": value_definition.modbus_table.name,
            "address": value_definition.start_address,
            "count": value_definition.register_width,
            "memory": raw_value,
            "error_code": error_code,
        }
        self.trigger_on_data_point_command_reply(
            data_point_identifier=value_definition.data_point_identifier,
            successful=success,
            value=value,
            protocol_data=protocol_data
        )

    def _on_data_point_update(self, value_definition: ModbusClientValueDefinition, old_value: ModbusValueType, new_value: ModbusValueType):
        protocol_data = {
            "protocol": CCXProtocol.MODBUS,
            "read_request": False,
            "write_request": False,
            "address": value_definition.start_address,
            "device_id": value_definition.unit_client.device_client.server_id,
            "unit_id": value_definition.unit_client.unit_id,
            "table": value_definition.modbus_table.name,
            "count": value_definition.register_width,
            "old_value": old_value
        }
        self.trigger_on_receive_data_point(value_definition.data_point_identifier, new_value, protocol_data)

    def _on_connection_change(self, device_client: ModbusDeviceClient, connected: bool, ip: str, port: int):
        server_id = device_client.server_id
        if server_id not in self.known_servers:
            self.logger.error(f"Cannot find server for Key {server_id}")
            return
        connection_status = CCXConnectionStatus.CONNECTED if connected else CCXConnectionStatus.DISCONNECTED
        self.trigger_on_connection_change(server_id, ip, port, connection_status)
