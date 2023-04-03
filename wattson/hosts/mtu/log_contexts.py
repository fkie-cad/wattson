from wattson.util import log_contexts

from wattson.iec104.interface.types import COT
from wattson.iec104.interface.apdus import APDU, U_FORMAT, S_FORMAT


def define_context_on_received_apdu(apdu: APDU) -> log_contexts:
    if isinstance(apdu, U_FORMAT):
        context = log_contexts.ON_RCVD_U_FORMAT
    elif isinstance(apdu, S_FORMAT):
        context = log_contexts.ON_RCVD_S_FORMAT
    elif apdu.cot == COT.PERIODIC:
        context = log_contexts.PERIODIC
    else:
        context = log_contexts.ON_RECEIVE
    return context


def define_context_on_send_adpu(apdu: APDU) -> log_contexts:
    if isinstance(apdu, U_FORMAT):
        context = log_contexts.ON_SEND_U_FORMAT
    elif isinstance(apdu, S_FORMAT):
        context = log_contexts.ON_SEND_S_FORMAT
    elif apdu.cot == COT.PERIODIC:
        context = log_contexts.PERIODIC
    else:
        context = log_contexts.ON_SEND
    return context
