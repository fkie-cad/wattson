from pymodbus.constants import ExcCodes


class ModbusResponseError(Exception):
    def __init__(self, modbus_exception_code: ExcCodes, *args):
        self.modbus_exception_code = modbus_exception_code
        super(ModbusResponseError, self).__init__(*args)
