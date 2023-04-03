from wattson.iec104.interface.types import COT
from wattson.iec104.interface.apdus import I_FORMAT
from wattson.apps.interface.util import ConfirmationStatus, FailReason
from wattson.apps.interface.util.messages import Confirmation, IECMsg


def generate_confirmation_received_handler(apdu: I_FORMAT, orig_msg: IECMsg) -> Confirmation:
    assert not apdu.send_from_MTU
    if apdu.positive:
        if apdu.cot in (COT.ACTIVATION_CONFIRMATION, COT.DEACTIVATION_CONFIRMATION):
            success = ConfirmationStatus.POSITIVE_CONFIRMATION
        else:
            success = ConfirmationStatus.SUCCESSFUL_TERM
    else:
        success = ConfirmationStatus.FAIL
    result = {"status": success}
    if not apdu.positive:
        result["reason"] = FailReason.RTUSide

    return Confirmation(result, orig_msg.reference_nr, orig_msg.max_tries)
