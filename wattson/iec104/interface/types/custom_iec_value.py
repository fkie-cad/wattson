from dataclasses import dataclass
from typing import Union, Tuple
import c104

#IEC_SINGLE_VALUE = Union[bool, float, int]
IEC_SINGLE_VALUE = Union[None,bool,c104.Double,c104.Step,c104.Int7,c104.Int16,int,c104.Byte32,c104.NormalizedFloat,float,c104.EventState,c104.StartEvents,c104.OutputCircuits,c104.PackedSingle]
IECValue = Union[IEC_SINGLE_VALUE, Tuple[IEC_SINGLE_VALUE, int]]
IEC_PARAMETER_SINGLE_VALUE = float

# TODO: maybe add 'normalised', 'scaled' Value as new types/ classes

@dataclass
class CustomIECValue:
    val: IEC_SINGLE_VALUE
    ts: float = -1


IECValues = Union[IEC_SINGLE_VALUE, IECValue, CustomIECValue]
