import enum
from typing import Literal, cast


class ModbusEndian(str, enum.Enum):
    BIG_ENDIAN = "big-endian"
    LITTLE_ENDIAN = "little-endian"
    BIG_SWAP_ENDIAN = "big-swap-endian"
    LITTLE_SWAP_ENDIAN = "little-swap-endian"

    @property
    def short_value(self) -> str:
        match self:
            case ModbusEndian.BIG_ENDIAN:
                return ">"
            case ModbusEndian.LITTLE_ENDIAN:
                return "<"
            case ModbusEndian.BIG_SWAP_ENDIAN:
                return "swapped_big"
            case ModbusEndian.LITTLE_SWAP_ENDIAN:
                return "swapped_little"
        raise ValueError("Unknown modbus endian")

    def is_default(self) -> bool:
        return self in [ModbusEndian.BIG_ENDIAN, ModbusEndian.LITTLE_ENDIAN]

    def to_literal(self) -> str:
        return "-".join(self.value.split("-")[:-1])
