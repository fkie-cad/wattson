import enum


class ModbusTable(str, enum.Enum):
    DISCRETE_INPUT = "discrete input"
    COIL = "coil"
    INPUT_REGISTER = "input register"
    HOLDING_REGISTER = "holding register"

    @property
    def short_value(self) -> str:
        return self.value.split(" ")[0]

    def is_register(self):
        return self in [ModbusTable.HOLDING_REGISTER, ModbusTable.INPUT_REGISTER]

    def is_bool(self) -> bool:
        return self in [ModbusTable.COIL, ModbusTable.DISCRETE_INPUT]

    def is_read_only(self) -> bool:
        return self in [ModbusTable.DISCRETE_INPUT, ModbusTable.INPUT_REGISTER]

    def is_writable(self) -> bool:
        return self in [ModbusTable.COIL, ModbusTable.HOLDING_REGISTER]
