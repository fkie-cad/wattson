# necessary for wrapping the c104 upwards to the MTU
from dataclasses import dataclass
from math import isnan

import c104

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
                         c104_point.processed_at.microsecond / 1000,
                         0 if c104_point.recorded_at is None else c104_point.recorded_at.microsecond / 1000
                         )

    @staticmethod
    def parse_to_previous_point(previous_state: c104.Information, cur_point: c104.Point) -> 'C104Point':
        new_v = cur_point.value
        p = C104Point(cur_point)
        p.c104_point = None
        p._value = previous_state.value
        s = f"{p} {previous_state} {cur_point.value} {new_v} {type(new_v)} {type(cur_point.value)} {cur_point.value == new_v} {type(new_v) == type(cur_point.value)}"
        #s = str(p) + str(previous_state) + str(new_v) + str(cur_point.value)
        if new_v != float(cur_point.value) and not (isnan(new_v) and isnan(cur_point.value)):
            raise RuntimeError(f"Bad value translation {s}")
        if not (new_v == cur_point.value or (isnan(new_v) and isnan(cur_point.value))):
            raise RuntimeError("2")
        elif not (p.value == previous_state.value or (isnan(p.value) and isnan(previous_state.value))):
            raise RuntimeError("3")

        p.updated_at_ms = 0 if previous_state.recorded_at is None else previous_state.recorded_at.microsecond / 1000
        p.quality = C104Point._translate_c104_quality({previous_state.quality})
        return p

    def read(self) -> bool:
        res = self.c104_point.read()
        self._value = TypeID.convert_val_by_type(self.type, self.c104_point.value)
        self.reported_at_ms = self.c104_point.processed_at.microsecond / 1000
        self.updated_at_ms = 0 if self.c104_point.recorded_at is None else self.c104_point.recorded_at.microsecond / 1000
        self.report_ms = self.c104_point.report_ms
        self.quality = self._translate_c104_quality(self.c104_point.quality)
        return res

    @property
    def value(self):
        return super().value

    @value.setter
    def value(self, v):
        self._value = v
        self.c104_point.value = v

    def transmit(self, cause: int) -> bool:
        # c104-cast is necessary due to the C++ signature
        cot = c104.Cot(cause)
        res = self.c104_point.transmit(cot)

        # not entirely sure if these values change in control-direction
        self.reported_at_ms = self.c104_point.processed_at.microsecond / 1000
        self.updated_at_ms = 0 if self.c104_point.recorded_at is None else self.c104_point.recorded_at.microsecond / 1000
        self.report_ms = self.c104_point.report_ms
        self.quality = self._translate_c104_quality(self.c104_point.quality)
        return res

    @staticmethod
    def _translate_c104_quality(c104_qualilty: c104.Quality) -> QualityByte:
        #print(c104_qualilty, type(c104_qualilty))
        c104_map = {
            QualityBit.INVALID: c104.Quality.Invalid,
            QualityBit.NON_TOPICAL: c104.Quality.NonTopical,
            QualityBit.SUBSTITUTED: c104.Quality.Substituted,
            QualityBit.BLOCKED: c104.Quality.Blocked,
            QualityBit.ELAPSED_TIME_INVALID: c104.Quality.ElapsedTimeInvalid,
            QualityBit.OVERFLOW: c104.Quality.Overflow
        }
        if c104_qualilty is None:
            return QualityByte(None)
        else:
            fcs_bits = {fcs_bit for (fcs_bit, c104_bit) in c104_map.items() if c104_bit in c104_qualilty}
            return QualityByte(fcs_bits)
