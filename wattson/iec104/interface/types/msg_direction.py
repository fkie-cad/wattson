from enum import unique, IntEnum


@unique
class MsgDirection(IntEnum):
    MONITORING = 0
    CONTROL = 1
    UNKNOWN = 2
    APPLICATION_REPLY = 3
    BOTH_POSSIBLE = 4