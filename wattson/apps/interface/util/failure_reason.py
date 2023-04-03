from enum import Enum


class FailReason(Enum):
    RTUSide = "RTU-Side Failure"
    IOA = "Bad IOA"
    COA = "Bad COA"
    NETWORK = "Network-Connection"
    NEGATIVE = "Negative Bit"
    QUALITY = "Bad Quality"
    COLLISION = "Collision"
    INTERRO_UNFINISHED = "Interro Unfinished"
    TYPE_UNSUPPORTED = "TypeID unsupported"
    SERVER = "MTU/Server Error"

