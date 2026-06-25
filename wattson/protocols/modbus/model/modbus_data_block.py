from pymodbus.constants import ExcCodes
from pymodbus.datastore.store import BaseModbusDataBlock

from wattson.protocols.modbus.model.modbus_response_error import ModbusResponseError
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_unit_memory import ModbusUnitMemory
from wattson.util import get_logger


class ModbusDataBlock(BaseModbusDataBlock):
    def __init__(self, modbus_unit_memory: ModbusUnitMemory, modbus_table: ModbusTable, use_zero_based_addressing: bool = True, address_offset: int = 0):
        self.memory = modbus_unit_memory
        self.table = modbus_table
        self._zero_based = use_zero_based_addressing
        self.address_offset = address_offset
        self.logger = get_logger(f"ModbusDataBlock.{self.table.name}")

    def _pdu_address_to_absolute_address(self, address: int) -> int:
        if self._zero_based:
            return address + self.address_offset
        else:
            return address + self.address_offset + 1

    def getValues(self, address, count=1):
        """
        PyModbus Interface
        """
        # Undo addition made by ModbusDeviceContext
        address -= 1
        absolute_address = self._pdu_address_to_absolute_address(address)
        self.logger.debug(f"getValues for PDU address {address} (-> {absolute_address} >>> {self.address_offset}) with count {count} (ZB: {self._zero_based})")
        try:
            if self.table.is_register():
                return self.memory.read_registers(absolute_address, count)
            else:
                return self.memory.read_coils(absolute_address, count)
        except ModbusResponseError as e:
            return e.modbus_exception_code
        except Exception as e:
            self.logger.error(f"Exception while reading memory: {e=}")
            return ExcCodes.DEVICE_FAILURE

    def setValues(self, address, values):
        """
        PyModbus Interface
        """
        # Undo addition made by ModbusDeviceContext
        address -= 1
        absolute_address = self._pdu_address_to_absolute_address(address)
        if not self.table.is_writable():
            return ExcCodes.ILLEGAL_FUNCTION
        try:
            if self.table.is_register():
                self.memory.write_registers(absolute_address, list(values))
            else:
                self.memory.write_coils(absolute_address, list(values))
        except ModbusResponseError as e:
            return e.modbus_exception_code
        except Exception as e:
            self.logger.error(f"Exception while writing memory: {e=}")
            return ExcCodes.DEVICE_FAILURE
