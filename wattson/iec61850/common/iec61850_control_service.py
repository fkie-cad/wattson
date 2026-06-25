import enum


class IEC61850ControlService(str, enum.Enum):
    # DIN EN 61850-7-2
    CANCEL = "Cancel"
    SELECT = "Sel"
    SELECT_WITH_VALUE = "SelVal"
    OPERATE = "Oper"
    TIME_ACTIVATED_OPERATE = "TimOper"
    TIME_ACTIVATED_OPERATE_TERMINATION = "TimOperTermination"
    COMMAND_TERMINATION = "CmdTerm"
