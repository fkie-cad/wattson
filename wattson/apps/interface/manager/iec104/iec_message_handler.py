import logging
import threading as th
from typing import TYPE_CHECKING

from wattson.util import UnsupportedError, UnexpectedAPDUError, C_RD

from wattson.iec104.common import GLOBAL_COA
from wattson.iec104.common.iec104message import IEC104Message
from wattson.iec104.common.datapoint import IEC104Point

from wattson.apps.interface.util import UNEXPECTED_MSG_REFERENCE_NR
from wattson.apps.interface.util.messages import *
from wattson.apps.interface.util.msg_status import MsgStatus
from wattson.apps.interface.manager.MTU_cache import CacheEntry, MessageCache
from wattson.apps.interface.manager.message_generators import generate_confirmation_received_handler
from wattson.apps.interface.manager.unexpected_apdu_handler import UnexpectedApduHandler
from wattson.apps.interface.manager.configs import DEFAULT_SUB_POLICY, SubPolicy
from wattson.apps.interface.manager.iec104.on_send_apdu_handler import OnSendAPDUHandler
from wattson.apps.interface.manager.iec104.on_receive_apdu_handler import OnReceiveAPDUHandler
from wattson.apps.interface.manager.iec104.on_dp_handler import OnDPHandler

if TYPE_CHECKING:
    from wattson.apps.interface.manager import SubscriptionManager


class IEC104Handler:
    """
    Generates IECMessages based on IEC104-APDUs representing the incoming & outgoing traffic.
    Handles changes to the corresponding caches etc.
    """
    def __init__(self, subscription_manager: 'SubscriptionManager', mtu_node_id: str,
                 policy: Optional[SubPolicy] = None):
        """

        Args:
            subscription_manager: Overlaying mgr which requests this handler
            mtu_node_id: MTU-ID required for the msg-prefixing
            policy: policy deciding when to send updates/ignore or cache msgs
        """
        self._manager = subscription_manager
        self.logger = self._manager.logger.getChild("iecH")
        self.logger.setLevel(logging.INFO)

        self.unexpected_APDU_handler = UnexpectedApduHandler(self.logger)

        self.mtu_prefix = f"MTU_{mtu_node_id}"
        self.__mtu_msg_cnt = 0
        self._msg_cnt_lock = th.Lock()
        self.subscription_policy = policy if policy else DEFAULT_SUB_POLICY
        self.on_send_handler = OnSendAPDUHandler(self)
        self.on_rcv_handler = OnReceiveAPDUHandler(self)
        self.on_dp_handler = OnDPHandler(self)

    def on_send_apdu(self, apdu: APDU, rtu_coa: int) -> Optional[IECMsg]:
        """
        Intermediate handler to be called on every outgoing APDU, expected on RAW msgs

        Args:
            apdu: data to be send
            rtu_coa: RTU the apdu is to be send to (required since the APDU might have COA=65535 for global cmds)

        Returns:
            None if no msg needs to be send, else the respective update
        """
        self.unexpected_APDU_handler.verify_send_apdu(apdu)
        if not self.subscription_policy.need_to_handle_apdu(apdu, raw_callback=True):
            return

        msg = self.on_send_handler.generate_msg_and_update_cache(apdu, rtu_coa, self.mtu_msg_cnt)
        #self._generate_msg_and_update_cache_on_send_apdu(apdu, rtu_coa, self.mtu_msg_cnt)
        if msg and msg.reference_nr.startswith(self.mtu_prefix):
            # APDU was initialised by MTU and not by a subscriber
            self.mtu_msg_cnt += 1

        return msg

    def on_receive_apdu(self, apdu: APDU, raw_callback: bool) -> Optional[IECMsg]:
        """
        Handler to be called for every incoming APDU

        Args:
            apdu: data received
            raw_callback: whether the callback was executed through the *RAW* pipeline.
                TODO: The RAW pipeline is to be deprecated due to speed & python-lock-issues.
                This is necessary if data about incoming/outgoing S-Frames/U-Frames and pure
                    ACT_CON/DEACT_CON/ACT_TERM/DEACT_TERM messages are to be forwarded/ need to be kept track of

        Raises:
            RuntimeError if cannot generate a msg for this APDU

        Returns:
            None if no update needs to be send, else the rsp. update
        """
        self.unexpected_APDU_handler.verify_recvd_apdu(apdu)
        msg = None
        if self.subscription_policy.need_to_handle_apdu(apdu, raw_callback):
            #msg = self._generate_msg_and_update_cache_on_receive_apdu(apdu)
            msg = self.on_rcv_handler.generate_msg_and_update_cache(apdu)
            if msg and msg.reference_nr.startswith(self.mtu_prefix):
                self.mtu_msg_cnt += 1

            elif not msg:
                raise RuntimeError(f"Cannot handle apdu {apdu}")

        return msg

    def on_datapoint(self, p: IEC104Point, prev_point: IEC104Point, incoming_message: IEC104Message) \
            -> Optional[IECMsg]:
        """
        TODO
        Args:
            p: new dp data received
            prev_point: previous state of that dp
            incoming_message: remaining IEC-104 data (header etc.)

        Returns:
            IECMsg containing the dp-data if already necessary to be published, else None
        """
        return self.on_dp_handler.on_dp(p, prev_point, incoming_message)

    def raise_on_overwriting_nonterminated_global(self, apdu: I_FORMAT, entry: CacheEntry):
        """
        TODO
        Args:
            apdu:
            entry:

        Returns:
            None
        """
        if (
                entry.status == MsgStatus.RECEIVED_ACK
                and apdu.cot == COT.ACTIVATION_CONFIRMATION
                and apdu.type != TypeID.C_CS_NA_1
        ):
            raise NotImplementedError(
                "Currently assumes global requests are always ended with Termination"
                "(Optional in the IEC)"
            )

    def _log_global_cache_before_error(self):
        self.logger.info(self.msg_cache)

    @property
    def combine_IOs(self) -> bool:
        """ if to combine multiple Information Objects into a singular message """
        return self.subscription_policy.combine_IOs

    @property
    def next_mtu_reference_number(self) -> str:
        """
        Generates and returns next reference number for msgs published without an incoming cmd from an application
        """
        with self._msg_cnt_lock:
            self.__mtu_msg_cnt += 1
            return f"{self.mtu_prefix}_{self.__mtu_msg_cnt}"

    @property
    def msg_cache(self) -> MessageCache:
        return self._manager.msg_cache

    @property
    def combine_periodic_IOs(self) -> bool:
        """ If to combine trickling periodicly updated Information Objects into a single message"""
        return self.subscription_policy.combine_periodic_IOs

    @property
    def mtu_msg_cnt(self):
        with self._msg_cnt_lock:
            return self.__mtu_msg_cnt

    @mtu_msg_cnt.setter
    def mtu_msg_cnt(self, val):
        with self._msg_cnt_lock:
            self.__mtu_msg_cnt = val
