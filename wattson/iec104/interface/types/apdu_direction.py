from enum import unique, IntEnum


@unique
class APDUDirection(IntEnum):
    MONITORING = 0
    CONTROL = 1
    BOTH_POSSIBLE = 2