from dataclasses import dataclass
from typing import Union, Tuple

IEC_SINGLE_VALUE = Union[bool, float, int]
IECValue = Union[IEC_SINGLE_VALUE, Tuple[IEC_SINGLE_VALUE, int]]
IEC_PARAMETER_SINGLE_VALUE = float

# TODO: maybe add 'normalised', 'scaled' Value as new types/ classes

@dataclass
class CustomIECValue:
    val: IEC_SINGLE_VALUE
    ts: float = -1


IECValues = Union[IEC_SINGLE_VALUE, IECValue, CustomIECValue]
