from typing import Union

from wattson.iec104.interface.apdus.i_format import I_FORMAT
from wattson.iec104.interface.apdus.s_format import S_FORMAT
from wattson.iec104.interface.apdus.u_format import U_FORMAT

APDU = Union[I_FORMAT, U_FORMAT, S_FORMAT]
