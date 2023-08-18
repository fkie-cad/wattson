from enum import Enum


class SystemMessageType(Enum):
    PING = 0
    CONNECT = 1
    DISCONNECT = 2
    ENABLE_PROMPT = 3
    DISABLE_PROMPT = 4
    OK = 5
    CANCEL = 6
