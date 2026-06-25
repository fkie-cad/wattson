from typing import Optional, TYPE_CHECKING, List

import numpy as np
from pymodbus.constants import ExcCodes

from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName
from wattson.protocols.modbus.modbus_server import ModbusServer
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType

if TYPE_CHECKING:
    from wattson.hosts.rtu import RTU


class RtuModbus:
    def __init__(self, rtu: 'RTU', **kwargs):
        self.rtu = rtu
        self.port = kwargs.get("port", 502)
        self.logger = self.rtu.logger.getChild("Modbus")

        self.allowed_mtu_ips = kwargs.get("allowed_mtu_ips", True)
        self.block_control_commands = kwargs.get("block_control_commands", False)
        self.server: Optional[ModbusServer] = None

        self.logger.info("Initialized RtuModbus")

    def setup_socket(self):
        self.logger.info(f"Adding Server Socket: {self.rtu.ip}:{self.port}")
        self.server = ModbusServer(
            device_id=self.rtu.node_id,
            bind_ip=self.rtu.ip,
            bind_port=self.port,
            zero_based=True,
            logger=self.logger.getChild("Server")
        )
        self.server.set_on_client_connect(self._on_client_connect)
        self.server.set_on_client_disconnect(self._on_client_disconnect)
        self.server.set_on_before_read(self._on_before_read)
        self.server.set_on_before_write(self._on_before_write)
        self.server.set_on_value_write(self._on_write)
        self.server.set_data_points(self._get_data_points())

    def start(self):
        self.server.start()

    def stop(self):
        self.server.stop()

    def _get_data_points(self):
        data_points = []
        for identifier, dp in self.rtu.data_point_dict.items():
            if dp["protocol"] == ProtocolName.MODBUS_TCP.value:
                data_points.append(dp)
        return data_points

    def _on_client_connect(self, ip: str, port: int) -> bool:
        self.logger.info(f"Client {ip}:{port} connecting")
        if not self.allowed_mtu_ips:
            return False
        if isinstance(self.allowed_mtu_ips, list) and ip not in self.allowed_mtu_ips:
            return False
        self.logger.info(f"Client {ip}:{port} connected")
        return True

    def _on_client_disconnect(self, ip: str, port: int):
        self.logger.info(f"Client {ip}:{port} disconnected")

    def _on_before_read(self, value_definition: ModbusValueDefinition) -> bool | ExcCodes:
        try:
            if value_definition.is_step():
                # Step values are kept in local register memory only
                return True
            val = self.rtu.get_value(value_definition.data_point_identifier)
            if val is None:
                val = 0
            try:
                if np.isnan(val):
                    val = 0
            except Exception:
                pass
            value_definition.set_value(val)
            return True
        except Exception as e:
            self.logger.error(f"Could not update data point value {value_definition.data_point_identifier}")
            self.logger.error(f"{e}")
            return ExcCodes.DEVICE_FAILURE

    def _on_before_write(self, value_definition: ModbusValueDefinition, registers_or_coils: List[int] | List[bool]) -> bool | ExcCodes:
        # Validate if writing is possible
        self.logger.info(f"On before write {value_definition.data_point_identifier} with {registers_or_coils}")
        if not value_definition.is_writable():
            return ExcCodes.ILLEGAL_FUNCTION
        return True

    def _on_write(self, value_definition: ModbusValueDefinition, registers_or_coils: List[int] | List[bool], value: ModbusValueType) -> bool | ExcCodes:
        # Set value in DataPointManager
        if value_definition.is_step():
            if not isinstance(value, bool):
                return ExcCodes.ILLEGAL_VALUE
            point_info = self.rtu.get_data_point_info(value_definition.data_point_identifier)
            if "related" in point_info and len(point_info["related"]) == 1:
                # Try to read value from related data point
                current_value = self.rtu.get_value(point_info["related"][0])
            else:
                self.logger.warning(f"No related DP for step {value_definition.data_point_identifier}")
                current_value = self.rtu.get_value(value_definition.data_point_identifier)
            if current_value is None or np.isnan(current_value):
                current_value = 0
            if value:
                current_value += 1
            else:
                current_value -= 1
            if self.rtu.set_value(value_definition.data_point_identifier, current_value):
                return True
            return ExcCodes.DEVICE_FAILURE
        # Write value directly
        self.logger.info(f"Setting {value_definition.data_point_identifier} to {value}")
        success = self.rtu.set_value(value_definition.data_point_identifier, value)
        self.logger.info(f"  Success: {success}")
        return success
