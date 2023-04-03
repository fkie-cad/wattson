from dataclasses import dataclass

from wattson.iec104.interface.types import COT, TypeID, QualityByte, IECValue, QualityBit


@dataclass
class IEC104Message:
    type: TypeID
    coa: int
    cot: COT
    # ip:port
    connection_string: str
    ioa: int
    quality: QualityByte = QualityByte({QualityBit.INVALID})
    value: IECValue = float('NaN')
    is_test: bool = False
    is_sequence: bool = False
    is_negative: bool = False

    def __post_init__(self):
        self.type = TypeID(self.type)
        self.cot = COT(self.cot)


    def __str__(self):
        return f"msg w/ type:cot {self.type}:{self.cot} for dp coa:ioa {self.coa}:{self.ioa} " \
               f"from connection {self.connection_string}"
