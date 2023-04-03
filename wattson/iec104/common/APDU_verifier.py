from typing import Optional, Tuple

from wattson.util import UnsupportedError, IEC104Exceptions, InvalidIEC104Error
from wattson.apps.interface.util.messages import MsgID
from wattson.iec104.interface.apdus import *
from wattson.iec104.interface.types import COT
from wattson.iec104.common.config import SUPPORTED_COTS, SUPPORTED_ASDU_TYPES
from wattson.iec104.common import GLOBAL_COA


def verify_send_I_FORMAT(apdu: I_FORMAT) -> Tuple[Optional[IEC104Exceptions], str]:
    msg_id = MsgID.from_type(apdu.type, apdu.cot)
    e_type, cause = verify_I_FORMAT(apdu)
    if bad_COT_for_type_and_direction(msg_id, apdu, False):
        cause = "COT"
        e_type = UnsupportedError
    return e_type, cause


def verify_rcvd_I_FORMAT(apdu: I_FORMAT) -> Tuple[Optional[IEC104Exceptions], str]:
    msg_id = MsgID.from_type(apdu.type, apdu.cot)
    e_type, cause = verify_I_FORMAT(apdu)

    if bad_COT_for_type_and_direction(msg_id, apdu, True):
        cause = "COT"
        e_type = UnsupportedError

    #elif msg_id in (MsgID.PROCESS_INFO_CONTROL, MsgID.READ_DATAPOINT):
    #    if not apdu.positive:
    #        cause = "NegAck"
    #        e_type = UnexpectedAPDUError

    return e_type, cause


def verify_I_FORMAT(apdu: I_FORMAT) -> Tuple[Optional[IEC104Exceptions], str]:
    e_type = None
    cause = ""
    if apdu.type.invalidated_for_IEC104:
        cause = "TypeID"
        e_type = InvalidIEC104Error
    elif apdu.type not in SUPPORTED_ASDU_TYPES:
        cause = "TypeID"
        e_type = UnsupportedError
    elif apdu.cot not in SUPPORTED_COTS:
        cause = "COT"
        e_type = UnsupportedError
    elif apdu.type.expects_IOA_as_0 and apdu.ioas != [0]:
        cause = "IOA"
        e_type = InvalidIEC104Error
    elif apdu.type.expects_single_IO and len(apdu.ioas) != 1:
        cause = "NumIOs"
        e_type = InvalidIEC104Error
    elif apdu.coa == GLOBAL_COA and not apdu.type.global_coa_compatible:
        cause = "COA"
        e_type = InvalidIEC104Error
    return e_type, cause


def bad_COT_for_type_and_direction(msg_id: MsgID, apdu: I_FORMAT, from_server: bool) -> bool:
    """ only checks for supported Types & COTs """
    if from_server:
        if msg_id == MsgID.PROCESS_INFO_CONTROL:
            return (apdu.cot.is_known_COT
                    and apdu.cot not in (COT.ACTIVATION_CONFIRMATION, COT.ACTIVATION_TERMINATION))
        if msg_id == MsgID.READ_DATAPOINT:
            return apdu.cot.is_known_COT
        elif msg_id == MsgID.PARAMETER_ACTIVATE:
            return (apdu.cot.is_known_COT
                    and apdu.cot not in (COT.ACTIVATION_CONFIRMATION, COT.DEACTIVATION_CONFIRMATION))
        elif msg_id == MsgID.SYS_INFO_CONTROL:
            return apdu.cot not in (COT.ACTIVATION_CONFIRMATION, COT.ACTIVATION_TERMINATION,
                                    COT.UNKNOWN_COA)
    else:
        if msg_id in (MsgID.PROCESS_INFO_CONTROL, MsgID.SYS_INFO_CONTROL):
            return apdu.cot != COT.ACTIVATION
        elif msg_id == MsgID.READ_DATAPOINT:
            return apdu.cot != COT.INTERROGATION
        elif msg_id == MsgID.PARAMETER_ACTIVATE:
            return apdu.cot not in (COT.ACTIVATION, COT.DEACTIVATION)

    return False
