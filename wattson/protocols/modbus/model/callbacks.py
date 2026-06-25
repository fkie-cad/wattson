from typing import Any, Callable, List

from pymodbus.constants import ExcCodes

from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType

ModbusOnValueWriteCallback = Callable[['ModbusValueDefinition', list, ModbusValueType], bool | ExcCodes]
ModbusOnBeforeValueWriteCallback = Callable[['ModbusValueDefinition', List[int] | List[bool]], bool | ExcCodes]
ModbusOnValueReadCallback = Callable[['ModbusValueDefinition'], bool | ExcCodes]
ModbusOnUnmappedReadCallback = Callable[[int, int, bool], bool | ExcCodes]
ModbusOnUnmappedWriteCallback = Callable[[int, list, bool], bool | ExcCodes]
