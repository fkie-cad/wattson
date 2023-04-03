from abc import ABC, abstractmethod
from dataclasses import dataclass

from wattson.iec104.interface.types import TypeID, IECValue, QualityByte, QualityBit


@dataclass
class IEC104Point(ABC):
    coa: int
    ioa: int
    type: int
    report_ms: int = 0
    related_ioa: int = 0
    quality: QualityByte = QualityByte({QualityBit.INVALID})
    _value: IECValue = None
    reported_at_ms: int = 0
    updated_at_ms: int = 0

    def __post_init__(self):
        self.type = TypeID(self.type)
        self.val_type = TypeID.type_converter(self.type)
        if self._value is not None:
            self._value = self.val_type(self._value)

    def __str__(self):
        return str(self.translate())

    def coa_ioa_str(self):
        return f"{self.coa}.{self.ioa}"

    def __hash__(self):
        hash_tuple = (
            self.coa,
            self.ioa,
            self.type,
        )
        return hash(hash_tuple)

    def translate(self) -> dict:
        val = "UNSET" if self.value_is_unset else self.value
        return {
            "coa": self.coa,
            "ioa": self.ioa,
            "type": self.type,
            "value": val,
            "quality": self.quality,
            "reported_at_ms": self.reported_at_ms,
            "updated_at_ms": self.updated_at_ms
        }

    @abstractmethod
    def read(self) -> bool:
        ...

    @abstractmethod
    def transmit(self, cause: int) -> bool:
        ...

    @property
    def _is_c104_point(self):
        # check without import
        return "Boost" in str(self.__class__) or "c104.Point" in str(self.__class__)

    @property
    def value(self):
        if self._value is None:
            raise RuntimeError(f"Read before dp-value has been set for dp with coa/ioa: {self.coa}/{self.ioa}")
        return self._value

    @value.setter
    def value(self, v):
        self._value = TypeID.convert_val_by_type(self.type, v)#v

    @property
    def value_is_unset(self) -> bool:
        return self._value is None

# DISCUSS: Is this still used?
def patchDataPoint(c104):
    if not hasattr(c104.Point, "FCSpatch"):
        c104.Point.FCSpatch = True
        c104.Point.value = property(lambda self: self.rtu.get_point_value(self), lambda self, value: self.setPointValue(value))
        c104.Point.getPointValue = lambda self: self._value

        def sp(self, val):
            self._value = val
        c104.Point.setPointValue = sp
