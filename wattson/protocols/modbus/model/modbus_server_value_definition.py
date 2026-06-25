from typing import Optional, List, TYPE_CHECKING

from wattson.protocols.modbus.model.callbacks import ModbusOnValueWriteCallback, ModbusOnValueReadCallback
from wattson.protocols.modbus.model.modbus_endian import ModbusEndian
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType

if TYPE_CHECKING:
    from wattson.protocols.modbus.model.modbus_unit_memory import ModbusUnitMemory


class ModbusServerValueDefinition(ModbusValueDefinition):
    def __init__(self,
                 data_point_identifier: str,
                 unit_id: int,
                 modbus_table: ModbusTable,
                 type_id: str,
                 register_width: int,
                 start_address: int,
                 endian: ModbusEndian,
                 unit_memory: Optional['ModbusUnitMemory'],
                 on_read_callback: Optional[ModbusOnValueReadCallback] = None,
                 on_write_callback: Optional[ModbusOnValueWriteCallback] = None):
        super(ModbusServerValueDefinition, self).__init__(
            data_point_identifier=data_point_identifier,
            unit_id=unit_id,
            modbus_table=modbus_table,
            type_id=type_id,
            register_width=register_width,
            start_address=start_address,
            endian=endian,
        )
        self.on_read_callback = on_read_callback
        self.on_write_callback = on_write_callback
        self._unit_memory = unit_memory

    def clone(self) -> 'ModbusServerValueDefinition':
        return ModbusServerValueDefinition(
            data_point_identifier=self.data_point_identifier,
            unit_id=self.unit_id,
            modbus_table=self.modbus_table,
            type_id=self.type_id,
            register_width=self.register_width,
            start_address=self.start_address,
            endian=self.endian,
            unit_memory=self._unit_memory
        )

    def set_unit_memory(self, unit_memory: 'ModbusUnitMemory'):
        self._unit_memory = unit_memory

    @staticmethod
    def from_data_point_dict(data_point_dict: dict) -> 'ModbusServerValueDefinition':
        identifier = data_point_dict['identifier']
        protocol_data = data_point_dict.get("protocol_data", {})
        unit_id = protocol_data.get("unit_id")
        modbus_table = ModbusTable(protocol_data.get("table"))
        return ModbusServerValueDefinition(
            identifier,
            unit_id,
            modbus_table=modbus_table,
            type_id=protocol_data.get("type_id"),
            register_width=protocol_data.get("width"),
            start_address=protocol_data.get("address"),
            endian=ModbusEndian(protocol_data.get("endian")),
            unit_memory=None
        )

    def get_value(self) -> ModbusValueType:
        """
        Reads the value from the respective registers and parses it into a native value format (int, float, bool, str)
        Returns: The current value stored in the Modbus register(s) as a native data type
        """
        values = self.get_raw_value()

        # Bool / Coil
        if self.modbus_table.is_bool():
            if len(values) != 1:
                raise RuntimeError(f"Unexpected values returned for {self.data_point_identifier}: {values}")
            if not isinstance(values[0], bool):
                raise RuntimeError(f"Unexpected value type returned for {self.data_point_identifier}: {values[0]}")
            return values[0]
        # Register
        if len(values) != self.register_width:
            raise RuntimeError(f"Unexpected values returned for {self.data_point_identifier}: {values}")
        # TODO: Validate for PyModbus 4
        return self.decode_value(values)

    def set_value(self, value: ModbusValueType, attempt_cast: bool = True) -> bool:
        """
        Sets the register value(s) to the representation of the given value.
        The given value should be a native value type (int, float, bool, str) and must correspond to the configured type_id.
        Args:
            value: The value to set.
            attempt_cast: Whether to attempt to cast the given value to the expected value type. Only casts floats to int.

        Returns:
            Whether the value was successfully set.
        """
        if not self._matches_value_type(type(value)):
            if attempt_cast and type(value) is float and self._matches_value_type(int):
                value = int(value)
            else:
                raise RuntimeError(f"Value {value} ({type(value)}) does not match {self.data_point_identifier} type {self.type_id}")
        if self.modbus_table.is_bool():
            if not isinstance(value, bool):
                raise RuntimeError(f"Can only write boolean values for {self.data_point_identifier} ({self.modbus_table.value})")
            self.set_raw_value(value)
            return True
        encoded_values = self.encode_value(value)
        self.set_raw_value(encoded_values)
        return True

    def get_raw_value(self) -> List[int]:
        """
        Returns the raw values of the registers forming this value.
        This is a list of 16-bit integers, where list entry 0 corresponds to the register at this values start_address.
        Returns:
            A list of 16-bit integers representing the raw register values.
        """
        return self._unit_memory.read_raw_values(self.start_address, self.register_width, self.modbus_table)

    def set_raw_value(self, values: List[int] | List[bool] | bool):
        """
        Sets the raw values of the registers forming this value.
        Args:
            values: The raw values to set.
        """
        if isinstance(values, bool):
            values = [values]
        if len(values) != self.register_width:
            raise ValueError(f"Expected {len(values)} values, got {len(values)} for ModbusValueDefinition {self.data_point_identifier}")
        self._unit_memory.write_raw_values(start_address=self.start_address, values=values, register_type=self.modbus_table)
