from enum import Enum

from wattson.iec104.common import SUPPORTED_ASDU_TYPES, SUPPORTED_COTS
from wattson.iec104.interface.types import COT, TypeID


class UnexpectedAPDUCause(Enum):
    TYPE_UNKNOWN = "TYPE_UNKNOWN"
    CAUSE_UNKNOWN = "CAUSE_UNKNOWN"
    COA_UNKNOWN = "COA_UNKNOWN"
    IOA_UNKNOWN = "IOA_UNKNOWN"
    UNSUPPORTED_COT = "UNSUPPORTED_COT"
    UNSUPPORTED_TYPE = "UNSUPPORTED_TYPE"

    @staticmethod
    def from_cot_and_type(cot: COT, type_ID: TypeID):
        # division into 'unexpected by IEC104 protocols' and
        # 'unexpected for wattson-usage'
        if cot == COT.UNKNOWN_TYPE:
            return UnexpectedAPDUCause.TYPE_UNKNOWN
        if cot == COT.UNKNOWN_CAUSE:
            return UnexpectedAPDUCause.CAUSE_UNKNOWN
        if cot == COT.UNKNOWN_COA:
            return UnexpectedAPDUCause.COA_UNKNOWN
        if cot == COT.UNKNOWN_IOA:
            return UnexpectedAPDUCause.IOA_UNKNOWN
        if type_ID not in SUPPORTED_ASDU_TYPES:
            return UnexpectedAPDUCause.UNSUPPORTED_TYPE
        if cot not in SUPPORTED_COTS:
            return UnexpectedAPDUCause.UNSUPPORTED_COT
        raise ValueError("Not an unsupported APDU or bad cot/type_ID")

    @staticmethod
    def is_unexpected(cot: COT, type_ID: TypeID):
        if cot in range(44, 48):
            return True
        if cot in SUPPORTED_COTS and type_ID in SUPPORTED_ASDU_TYPES:
            return False
        return True
