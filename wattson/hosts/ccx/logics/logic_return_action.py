import enum


class LogicReturnAction(str, enum.Enum):
    NONE = "none"
    CONTINUE = "continue"
    STOP_NOTIFICATION = "stop-notification"
    STOP_LOGICS = "stop-logics"
    STOP_ALL = "stop-all"
