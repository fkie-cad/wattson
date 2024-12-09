# necessary for wrapping the c104 upwards to the MTU
from dataclasses import dataclass
from math import isnan
from typing import Optional

import c104
from c104 import StepCmd

from wattson.iec104.interface.types import QualityBit, QualityByte, TypeID
from wattson.iec104.common.datapoint import IEC104Point


@dataclass
class C104Point(IEC104Point):
    def __init__(self, c104_point: c104.Point):
        self.c104_point = c104_point
        fcs_quality_descriptor = self._translate_c104_quality(c104_point.quality)
        super().__init__(c104_point.station.common_address,
                         c104_point.io_address,
                         c104_point.type,
                         c104_point.report_ms,
                         c104_point.related_io_address,
                         fcs_quality_descriptor,
                         c104_point.value,
                         c104_point.processed_at,
                         c104_point.recorded_at
                         )

    @staticmethod
    def parse_to_previous_point(previous_info: c104.Information, cur_point: c104.Point) -> 'C104Point':
        new_v = cur_point.value
        p = C104Point(cur_point)
        p.c104_point = None
        p._value = previous_info.value
        s = f"{p} {previous_info} {cur_point.value} {new_v} {type(new_v)} {type(cur_point.value)} {cur_point.value == new_v} {type(new_v) == type(cur_point.value)}"
        #s = str(p) + str(previous_state) + str(new_v) + str(cur_point.value)
        """
        if new_v != float(cur_point.value) and not (isnan(new_v) and isnan(cur_point.value)):
            raise RuntimeError(f"Bad value translation {s}")
        if not (new_v == cur_point.value or (isnan(new_v) and isnan(cur_point.value))):
            raise RuntimeError("2")
        elif not (p.value == previous_info.value or (isnan(p.value) and isnan(previous_info.value))):
            raise RuntimeError("3")
        """

        p.updated_at_ms = previous_info.recorded_at
        p.quality = C104Point._translate_c104_quality(previous_info.quality)
        return p

    def read(self) -> bool:
        res = self.c104_point.read()
        self._value = TypeID.convert_val_by_type(self.type, self.c104_point.value)
        self.reported_at_ms = self.c104_point.processed_at
        self.updated_at_ms = self.c104_point.recorded_at
        self.report_ms = self.c104_point.report_ms
        self.quality = self._translate_c104_quality(self.c104_point.quality)
        return res

    @property
    def value(self):
        return super().value

    @property
    def info(self):
        return self.c104_point.info

    @value.setter
    def value(self, v):
        v = self.translate_c104_value(v)
        self._value = v
        self.c104_point.value = v

    def transmit(self, cause: int) -> bool:
        # c104-cast is necessary due to the C++ signature
        cot = c104.Cot(cause)
        res = self.c104_point.transmit(cot)

        # not entirely sure if these values change in control-direction
        self.reported_at_ms = self.c104_point.processed_at
        self.updated_at_ms = self.c104_point.recorded_at
        self.report_ms = self.c104_point.report_ms
        self.quality = self._translate_c104_quality(self.c104_point.info.quality)
        return res

    @staticmethod
    def _translate_c104_quality(c104_quality: Optional[c104.Quality]) -> Optional[QualityByte]:
        if c104_quality is None:
            return None
        c104_map = {
            QualityBit.INVALID: c104.Quality.Invalid,
            QualityBit.NON_TOPICAL: c104.Quality.NonTopical,
            QualityBit.SUBSTITUTED: c104.Quality.Substituted,
            QualityBit.BLOCKED: c104.Quality.Blocked,
            QualityBit.ELAPSED_TIME_INVALID: c104.Quality.ElapsedTimeInvalid,
            QualityBit.NONE: c104.Quality(),
            QualityBit.OVERFLOW: c104.Quality.Overflow
        }

        fcs_bits = {fcs_bit for (fcs_bit, c104_bit) in c104_map.items() if c104_bit in c104_quality}
        return QualityByte(fcs_bits)

    def translate_c104_value(self, value):
        if isinstance(self.c104_point.info, c104.StepCmd):
            if not isinstance(value, c104.Step):
                return c104.Step(value)
            return value
        if isinstance(self.c104_point.info, c104.StepInfo):
            if not isinstance(value, c104.Int7):
                return c104.Int7(value)
            return value
        if isinstance(self.c104_point.info, (c104.DoubleCmd, c104.DoubleInfo)):
            if not isinstance(value, c104.Double):
                return c104.Double(value)
            return value
        return self.c104_point.value.__class__(value)
