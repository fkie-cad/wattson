from dataclasses import dataclass, field
from typing import Union, List, Iterable

from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.interface.types import TypeID, COT


@dataclass
class I_FORMAT:
    type: Union[int, TypeID]
    cot: Union[int, COT]
    coa: int
    positive: bool
    ioas: List[int] = field(default_factory=list)

    def __post_init__(self):
        self.cot = COT(int(self.cot))
        self.type = TypeID(int(self.type))
        if not self.ioas:
            self.ioas = [0]
        else:
            self.ioas = [int(ioa) for ioa in self.ioas]

    def __str__(self):
        return f"I-Format({self.type.name} = {self.type}, {self.cot.name} = {self.cot}, " \
               f"coa = {self.coa}, pos = {self.positive}, ioas = {self.ioas})"

    @property
    def send_from_MTU(self):
        if self.cot in {COT.ACTIVATION, COT.DEACTIVATION, COT.INTERROGATION}:
            return True
        return False

    @staticmethod
    def from_dp(dp: IEC104Point):
        cot = COT.ACTIVATION if dp.type >= 45 else COT.INTERROGATION
        return I_FORMAT(dp.type, cot, dp.coa, True, [dp.ioa])

    @staticmethod
    def from_many_dps(datapoints: Iterable[IEC104Point]):
        assert len([dp.coa for dp in datapoints]) == 1
        assert len([dp.type for dp in datapoints]) == 1
        _dp = next(iter(datapoints))
        cot = COT.ACTIVATION if _dp.type >= 45 else COT.INTERROGATION
        return I_FORMAT(_dp.type, cot, _dp.coa, True, [dp.ioa for dp in datapoints])

    def verify(self) -> bool:
        from wattson.iec104.common.APDU_verifier import verify_I_FORMAT
        e_type, cause = verify_I_FORMAT(self)
        return e_type is None