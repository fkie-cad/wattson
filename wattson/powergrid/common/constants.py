import typing

# TODO: Remove this file

ERROR_PREFIX = b"Error when answering the NetQuery: "
QUERY_INVALID = ERROR_PREFIX + b"Query format was invalid"
COMM_SIM_FAILED = ERROR_PREFIX + b"Communication with pandapower failed"
VALUE_UNKNW = ERROR_PREFIX + b"Requested value does not exist"

TEST_PREFIX = b"Test: "

DEFAULT_COORD_IP_ADDR = "127.0.0.1"
DEFAULT_COORD_POWER_PORT = 5555
DEFAULT_COORD_GLOBAL_EVENT_PORT = 5556


T = typing.TypeVar('T')
ListOrSingle = typing.Union[T, typing.List[T]]
