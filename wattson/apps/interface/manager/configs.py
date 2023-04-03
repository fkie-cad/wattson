from dataclasses import dataclass
from typing import Dict

from wattson.iec104.interface.types import COT
from wattson.iec104.interface.apdus import APDU, U_FORMAT, S_FORMAT

POLICY = Dict[str, bool]


@dataclass(frozen=True)
class SubPolicy:
    S_Frames: bool
    U_Frames: bool
    acks: bool
    combine_IOs: bool
    combine_periodic_IOs: bool
    independent_clock_sync: bool

    extract_val_from_raw: bool
    handle_cot_5_raw: bool
    handle_cot_20_raw: bool
    remove_actterm_raw: bool
    handle_monitoring_initiated_raw: bool

    ignore_unknown_cot_dp_callbacks: bool
    ignore_quality: bool

    def need_to_handle_apdu(self, apdu: APDU, raw_callback: bool) -> bool:
        """ Checks if this APDU needs to be handled by an IECMsgHandler

        Args:
            apdu: data send/ received
            raw_callback: whether pipe was executed upon a raw-msg callback

        Returns:
            Bool if expected to somehow handle the data, False if the data can be discarded
        """
        if isinstance(apdu, S_FORMAT):
            return self.S_Frames
        if isinstance(apdu, U_FORMAT):
            return self.U_Frames

        if not raw_callback:
            return True
        if apdu.cot == COT.INTERROGATED_BY_STATION:
            return self.handle_cot_20_raw
        if apdu.cot == COT.INTERROGATION:
            return self.handle_cot_5_raw
        if apdu.cot in (COT.PERIODIC, COT.SPONTANEOUS):
            return self.handle_monitoring_initiated_raw
        return True


DEFAULT_SUB_POLICY = SubPolicy(
    S_Frames=False,
    U_Frames=False,
    acks=True,
    combine_IOs=False,
    combine_periodic_IOs=True,
    # only forward clock-synch if a synch-command was send by subscribers, not by the general system itself
    independent_clock_sync=False,

    extract_val_from_raw=False,
    handle_cot_5_raw=False,
    remove_actterm_raw=False,
    handle_cot_20_raw=False,
    handle_monitoring_initiated_raw=False,

    ignore_unknown_cot_dp_callbacks=True,
    ignore_quality=True,
)


'''
DEPRECATED?
MAX_SUB_POLICY = {
    "S-Frames": True,
    "U-Frames": True,
    "acks": True,
    "combine_IOs": False,
    "combine_periodic_IOs": True,
    "extract_val_from_raw": False,
    "independent clock-synch": True,
    "handle_cot_5_raw": True,
    "remove_actterm_raw": False,
    "handle_cot_20_dps_raw": False,
    "handle_monitoring_initiated_raw": False,
    "ignore_unknown_cot_dp_callbacks": False,
    "ignore_quality": True,
}
'''
