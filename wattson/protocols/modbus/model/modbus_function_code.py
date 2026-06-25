import enum
from typing import Optional

from wattson.protocols.modbus.model.modbus_table import ModbusTable


class ModbusFunctionCode(int, enum.Enum):
    RESERVED = 0
    NONE = -1
    UNKNOWN = -2

    # READING
    READ_COIL = 1
    READ_DISCRETE_INPUT = 2
    READ_HOLDING_REGISTER = 3
    READ_INPUT_REGISTER = 4
    # WRITING
    WRITE_COIL = 5
    WRITE_HOLDING_REGISTER = 6
    WRITE_MULTIPLE_COILS = 15
    WRITE_HOLDING_REGISTERS = 16
    # ADDITIONAL
    MASK_WRITE_REGISTER = 22
    READ_WRITE_MULTIPLE_REGISTERS = 23
    READ_DEVICE_IDENTIFICATION = 43

    def is_reading(self) -> bool:
        return self in [ModbusFunctionCode.READ_COIL, ModbusFunctionCode.READ_DISCRETE_INPUT,
                        ModbusFunctionCode.READ_HOLDING_REGISTER, ModbusFunctionCode.READ_INPUT_REGISTER]

    def is_writing(self) -> bool:
        return self in [ModbusFunctionCode.WRITE_COIL, ModbusFunctionCode.WRITE_MULTIPLE_COILS,
                        ModbusFunctionCode.WRITE_HOLDING_REGISTER, ModbusFunctionCode.WRITE_HOLDING_REGISTER]

    def get_affected_table(self) -> Optional[ModbusTable]:
        if self in [ModbusFunctionCode.READ_COIL, ModbusFunctionCode.WRITE_COIL, ModbusFunctionCode.WRITE_MULTIPLE_COILS]:
            return ModbusTable.COIL
        if self in [ModbusFunctionCode.READ_DISCRETE_INPUT]:
            return ModbusTable.DISCRETE_INPUT
        if self in [ModbusFunctionCode.READ_HOLDING_REGISTER, ModbusFunctionCode.WRITE_HOLDING_REGISTER, ModbusFunctionCode.WRITE_HOLDING_REGISTERS]:
            return ModbusTable.HOLDING_REGISTER
        if self in [ModbusFunctionCode.READ_INPUT_REGISTER]:
            return ModbusTable.INPUT_REGISTER
        return None
