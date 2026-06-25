from typing import Dict, List, Optional, TYPE_CHECKING

from wattson.protocols.modbus.model.modbus_unit_client import ModbusUnitClient
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition

if TYPE_CHECKING:
    from wattson.protocols.modbus.modbus_client import ModbusClient


class ModbusDeviceClient:
    """
    The ModbusDeviceClient class represents the client-perspective of a Modbus server.
    It stores connection details and wraps individual units.
    """
    def __init__(self, server_id: str, server_address: str, server_port: int):
        self.server_id = server_id
        self.server_address = server_address
        self.server_port = server_port
        self.units: Dict[int, ModbusUnitClient] = {}
        self.modbus_client: Optional['ModbusClient'] = None

    def add_unit(self, modbus_unit: ModbusUnitClient):
        self.units[modbus_unit.unit_id] = modbus_unit
        modbus_unit.device_client = self

    def has_unit(self, unit_id: int) -> bool:
        return unit_id in self.units

    def get_unit(self, unit_id: int) -> ModbusUnitClient:
        return self.units[unit_id]

    def get_value_definitions(self) -> List[ModbusValueDefinition]:
        values = []
        for unit in self.units.values():
            values.extend(unit.get_value_definition_list())
        return values
