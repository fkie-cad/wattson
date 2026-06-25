import enum


class ModbusServerCallbacks(str, enum.Enum):
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    READ_COIL = "read_coil"
    WRITE_COIL = "write_coil"
    READ_REGISTER = "read_register"
    WRITE_REGISTER = "write_register"
    READ = "read"
    WRITE = "write"
