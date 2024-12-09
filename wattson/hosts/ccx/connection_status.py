import enum


class CCXConnectionStatus(str, enum.Enum):
    UNINITIALIZED = "uninitialized"
    DISCONNECTED = "disconnected"
    ESTABLISHING = "establishing"
    CONNECTED = "connected"
    ESTABLISHED = "established"
    LOST = "lost"

