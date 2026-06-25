from typing import Dict, List, Optional, TYPE_CHECKING

from wattson.protocols.modbus.model.modbus_client_value_definition import ModbusClientValueDefinition
from wattson.protocols.modbus.model.modbus_table import ModbusTable


if TYPE_CHECKING:
    from wattson.protocols.modbus.model.modbus_device_client import ModbusDeviceClient


class ModbusUnitClient:
    """
    The ModbusDeviceClient class represents the client-perspective of a Modbus server.
    It stores connection details and wraps individual units.
    """
    def __init__(self,
                 unit_id: int,
                 device_client: 'ModbusDeviceClient',
                 zero_based_addressing: bool = True,
                 table_offset_coil: Optional[int] = None,
                 table_offset_discrete_input: Optional[int] = None,
                 table_offset_input_register: Optional[int] = None,
                 table_offset_holding_register: Optional[int] = None,
                 ):

        self.unit_id = unit_id
        self.device_client = device_client
        self.zero_based_addressing: bool = zero_based_addressing
        self.table_offsets = {
            ModbusTable.COIL: table_offset_coil,
            ModbusTable.DISCRETE_INPUT: table_offset_discrete_input,
            ModbusTable.HOLDING_REGISTER: table_offset_holding_register,
            ModbusTable.INPUT_REGISTER: table_offset_input_register,
        }
        # Keys are DataPoint Identifiers
        self._value_definitions: Dict[str, ModbusClientValueDefinition] = {}

    def register_value_definition(self, value_definition: ModbusClientValueDefinition):
        self._value_definitions[value_definition.data_point_identifier] = value_definition

    def get_value_definition_list(self) -> List[ModbusClientValueDefinition]:
        return list(self._value_definitions.values())

    def register_table_offset(self, modbus_table: ModbusTable, table_offset: int, raise_on_mismatch: bool = True):
        current_offset = self.table_offsets[modbus_table]
        if current_offset is None:
            current_offset = table_offset
        if current_offset != table_offset and raise_on_mismatch:
            raise ValueError(f"Table Offset {table_offset} does not match current table offset {current_offset} for {modbus_table.name}")
        self.table_offsets[modbus_table] = table_offset

    def get_relative_address(self, value_definition: ModbusClientValueDefinition) -> int:
        offset = self.table_offsets[value_definition.modbus_table]
        if offset is None:
            raise KeyError(f"No offset specified for {value_definition.modbus_table}")
        if self.zero_based_addressing:
            return value_definition.get_zero_based_address() - offset
        else:
            return value_definition.get_one_based_address() - offset
