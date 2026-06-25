import abc
from abc import ABC, abstractmethod
from typing import Optional, Any, List, Tuple, TYPE_CHECKING, Type

import pymodbus.client

from wattson.protocols.modbus.model.callbacks import ModbusOnValueWriteCallback, ModbusOnValueReadCallback
from wattson.protocols.modbus.model.modbus_endian import ModbusEndian
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType


class ModbusValueDefinition(ABC):
    def __init__(self,
                 data_point_identifier: str,
                 unit_id: int,
                 modbus_table: ModbusTable,
                 type_id: str,
                 register_width: int,
                 start_address: int,
                 endian: ModbusEndian,
                 on_read_callback: Optional[ModbusOnValueReadCallback] = None,
                 on_write_callback: Optional[ModbusOnValueWriteCallback] = None):
        self.data_point_identifier = data_point_identifier
        self.unit_id = unit_id
        self.modbus_table = modbus_table
        self.type_id = type_id
        self.register_width = register_width
        # This is the absolute start address (register)
        self.start_address = start_address
        self.endian = endian
        self.on_read_callback = on_read_callback
        self.on_write_callback = on_write_callback

    @abstractmethod
    def clone(self) -> 'ModbusValueDefinition':
        ...

    @staticmethod
    def from_data_point_dict(data_point_dict: dict) -> 'ModbusValueDefinition':
        raise NotImplementedError()

    def value(self, value_to_set: Optional[ModbusValueType] = None) -> Any:
        if value_to_set is None:
            return self.get_value()
        return self.set_value(value_to_set)

    @abstractmethod
    def get_value(self) -> ModbusValueType:
        """
        Reads the locally cached value and parses it into a native value format (int, float, bool, str)
        """
        ...

    @abstractmethod
    def set_value(self, value_to_set: ModbusValueType) -> bool:
        """
        Sets the local value(s) to the representation of the given value
        Args:
            value_to_set: The value to set.

        Returns:
            Whether the value was successfully set.
        """
        ...

    @abstractmethod
    def get_raw_value(self) -> List[int] | List[bool]:
        """
        Returns the raw values of the (local) registers forming this value.
        This is a list of 16-bit integers, where list entry 0 corresponds to the register at this values start_address.
        Returns:
            A list of 16-bit integers representing the raw register values or a list of booleans representing the raw register values.
        """
        ...

    @abstractmethod
    def set_raw_value(self, values: List[int] | List[bool] | bool):
        """
        Sets the raw values of the registers forming this value.
        Args:
            values: The raw values to set.
        """
        ...

    @property
    def end_address(self) -> int:
        """
        The (absolute) end address of the register.
        """
        return self.start_address + self.register_width - 1

    def get_addresses(self) -> List[int]:
        """
        Returns all (absolute) addresses of the registers forming this value.
        """
        return list(range(self.start_address, self.end_address + 1))

    def overlaps(self, start_address: int, end_address: int) -> bool:
        """
        Returns whether the given (absolute) address range overlaps with the absolute address range of this value.
        """
        return not (self.end_address < start_address or self.start_address > end_address)

    def overlaps_count(self, start_address: int, count: int) -> bool:
        return self.overlaps(start_address, start_address + count - 1)

    def get_overlap(self, start_address: int, end_address: int) -> Optional[Tuple[int, int]]:
        """
        Returns this values overlap with the given address range
        Args:
            start_address: The start address of the memory segment
            end_address: The end address of the memory segment

        Returns:
            None if there is no overlap.
            Tuple with overlap start address and overlap length otherwise
        """
        if not self.overlaps(start_address, end_address):
            return None
        overlap_start = max(self.start_address, start_address)
        overlap_end = min(self.end_address, end_address)
        overlap_length = overlap_end - overlap_start + 1
        return overlap_start, overlap_length

    def is_register(self) -> bool:
        return self.modbus_table in [ModbusTable.INPUT_REGISTER, ModbusTable.HOLDING_REGISTER]

    def is_bool(self) -> bool:
        return not self.is_register()

    def is_float(self) -> bool:
        return self.is_register() and self.type_id in ["float32", "float64", "double"]

    def is_int(self) -> bool:
        return self.is_register() and self.type_id in ["int16", "int32", "int64", "uint16", "uint32", "uint64"]

    def is_writable(self) -> bool:
        return self.modbus_table.is_writable()

    def is_step(self) -> bool:
        return self.type_id == "step"

    def _matches_value_type(self, value_type: Type[ModbusValueType]) -> bool:
        if value_type == float:
            return self.type_id in ["float32", "float64", "double"]
        if value_type == int:
            return self.type_id in ["int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64", "double"]
        if value_type == bool:
            return self.type_id in ["bool", "step"]
        if value_type == str:
            return self.type_id in ["str"]
        return False

    def decode_value(self, values: List[int] | List[bool]) -> ModbusValueType:
        data_type = self.get_pymodbus_type()
        if not self.endian.is_default():
            raise ValueError("Only BIG_ENDIAN and LITTLE_ENDIAN supported")
        if data_type is bool:
            if len(values) != self.register_width:
                raise ValueError("Boolean values require exactly one register / coil")
            if not isinstance(values[0], bool):
                raise ValueError("Values must be boolean values")
            return values[0]

        decoded_values = pymodbus.client.ModbusTcpClient.convert_from_registers(
            registers=values,
            data_type=data_type,
            word_order=self.endian.to_literal()
        )
        if isinstance(decoded_values, list):
            raise ValueError(f"Got multiple decoded values: {decoded_values}")
        return decoded_values

    def encode_value(self, value: ModbusValueType) -> List[int] | List[bool]:
        data_type = self.get_pymodbus_type()
        if not self.endian.is_default():
            raise ValueError("Only BIG_ENDIAN and LITTLE_ENDIAN supported")
        if isinstance(value, bool):
            encoded_values = [value]
        else:
            encoded_values = pymodbus.client.ModbusTcpClient.convert_to_registers(value, data_type=data_type, word_order=self.endian.to_literal())
        if len(encoded_values) != self.register_width:
            raise ValueError(f"Returned values only cover {len(encoded_values)} registers instead of {self.register_width} registers")
        return encoded_values

    def get_pymodbus_type(self) -> pymodbus.client.ModbusTcpClient.DATATYPE | Type[bool]:
        match self.type_id:
            case "uint16":
                return pymodbus.client.ModbusTcpClient.DATATYPE.UINT16
            case "uint32":
                return pymodbus.client.ModbusTcpClient.DATATYPE.UINT32
            case "uint64":
                return pymodbus.client.ModbusTcpClient.DATATYPE.UINT64
            case "int16":
                return pymodbus.client.ModbusTcpClient.DATATYPE.INT16
            case "int32":
                return pymodbus.client.ModbusTcpClient.DATATYPE.INT32
            case "int64":
                return pymodbus.client.ModbusTcpClient.DATATYPE.INT64
            case "string":
                return pymodbus.client.ModbusTcpClient.DATATYPE.STRING
            case "float32":
                return pymodbus.client.ModbusTcpClient.DATATYPE.FLOAT32
            case "float64":
                return pymodbus.client.ModbusTcpClient.DATATYPE.FLOAT64
            case "bool":
                return bool
            case "step":
                return bool
        raise ValueError(f"Cannot match type_id {self.type_id} to Pymodbus type")
