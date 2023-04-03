from typing import Union


class UnsupportedError(NotImplementedError):
    """ Occurs for any not-yet implmeneted case. """
    pass


class InvalidL5Error(ValueError):
    pass


class InvalidIEC104Error(InvalidL5Error):
    pass


class InvalidModbusError(InvalidL5Error):
    pass


class UnexpectedAPDUError(RuntimeError):
    pass


IEC104Exceptions = Union[UnsupportedError, InvalidIEC104Error]


class MissingArgumentError(ValueError):
    pass


class BadArgumentError(ValueError):
    pass


class ArgumentIsNoneError(BadArgumentError):
    pass


class SetupError(RuntimeError):
    pass


class CLIError(RuntimeError):
    pass


class NormNotTransferredError(NotImplementedError):
    """
    Where the detected norm is not implemented for the specifric method yet
    """
    pass
