from enum import unique, IntEnum


@unique
class MsgStatus(IntEnum):
    WAITING_FOR_SEND = 0
    SEND_NO_ACK = 1
    RECEIVED_ACK = 2
    RECEIVED_TERM = 3  # kinda unecessary
    IN_RTU_PROGRESS = 4
    RECEIVED_NEG_ACK = 5
    # once the RTU starts sending intermediate cmds during gen-interro