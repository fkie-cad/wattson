from typing import TYPE_CHECKING

from wattson.iec104.common.datapoint import IEC104Point
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

from wattson.apps.interface.util.messages import *
from wattson.apps.interface.util.msg_status import MsgStatus
from wattson.apps.interface.manager.message_generators import generate_confirmation_received_handler
from wattson.apps.interface.manager.MTU_cache import MessageCache
from wattson.apps.interface.manager.configs import SubPolicy


if TYPE_CHECKING:
    from wattson.apps.interface.manager.iec104.iec_message_handler import IEC104Handler


class OnDPHandler:
    def __init__(self, main_handler: 'IEC104Handler'):
        self.main_handler = main_handler
        self.logger = main_handler.logger

    def on_dp(self, p: IEC104Point, prev_point: IEC104Point, incoming_message: IEC104Message) \
            -> Optional[IECMsg]:
        """

        Args:
            p: newly incoming datapoint
            prev_point: Status of dp with same COA:IOA before the update is applied
            incoming_message: Remaining parts of the msg. (Header, is_test, etc.) of the new dp

        Returns:
            an IECMsg to be published if necessary
        """
        cot = COT(int(incoming_message.cot)) if incoming_message else COT.UNKNOWN_CAUSE

        sufficient_quality = self._has_sufficient_quality(p)
        if not sufficient_quality:
            self.logger.info(f"Bad Quality for incoming datapoint {p},"
                             f" oldpoint: {prev_point} and msg {incoming_message}")
            return

        if cot == COT.INTERROGATION:
            # specifically needs to reply to the requesting application
            return self._handle_on_dp_from_read(cot, p)

        if cot in (COT.INTERROGATED_BY_STATION, COT.SPONTANEOUS):
            # publish to all
            if cot == COT.INTERROGATED_BY_STATION:
                with self.cache.global_cache_lock:
                    if not self.cache.global_send_and_active(p.coa, TypeID.C_IC_NA_1):
                        self.main_handler._log_global_cache_before_error()
                        raise UnexpectedAPDUError(f"with point {p}; msg {incoming_message}")
                    entry = self.cache.get_interro_entry(p.coa)
                    entry.IO_cache[p.ioa] = p.value
                msg_nr = entry.msg.reference_nr
            else:
                msg_nr = self.main_handler.next_mtu_reference_number
            return ProcessInfoMonitoring(
                p.coa,
                {p.ioa: p.value}, {p.ioa: p.updated_at_ms},
                p.type, cot,
                msg_nr
            )

        if cot == COT.PERIODIC:
            msg = self._gen_msg_for_periodic_update_if_necessary(p)
            return msg

        self._check_for_unhandled_on_dp_cases(p, prev_point, incoming_message)

    def _handle_on_dp_from_read(self, cot: int, p: IEC104Point) -> Optional[ProcessInfoMonitoring]:
        """ Handles a new datapoint that was explicitly read

        Args:
            cot: COT of reply
            p: datapoint contained in reply

        Returns:
            None if dp should be combined with others, else respective update-msg
        """
        self.logger.debug("[START]", context=C_RD)
        with self.cache.dp_cache_lock:
            val_map = {p.ioa: p.value}
            ts_map = {p.ioa: p.updated_at_ms}

            if not self.cache.is_dp_active(p.coa, p.ioa):
                self.logger.warning('Received Read-answer without having send Read!')
                # How should we handle this error?
                return ProcessInfoMonitoring(p.coa, val_map, ts_map, p.type, cot,
                                             UNEXPECTED_MSG_REFERENCE_NR)

            entry = self.cache.pop_active_entry(p.coa, p.ioa)
            if self.combine_IOs:
                entry.IO_cache[p.ioa] = p.value
                if not entry.is_cache_filled():
                    return
                assert not self.cache.is_dp_active(p.coa, p.ioa)
                val_map = entry.IO_cache
                # TODO
                ts_map = {}

            self.logger.debug("[END]", C_RD)
            return ProcessInfoMonitoring(p.coa, val_map, ts_map, p.type, cot,
                                         entry.msg.reference_nr)

    def _check_for_unhandled_on_dp_cases(self, p: IEC104Point, prev_point: IEC104Point,
                                         incoming_message: IEC104Message):
        """ Checks if the reply given by the current IEC-104 Station matches with the app's implementation status

        Args:
            p: newly transmitted dp
            prev_point: the dp's status prior to the update
            incoming_message: remaining IEC104 data contained in the APDU carrying the new dp

        Raises:
            NotImplementedError if the subscription-policy & incoming data require an unsupported feature

        Returns:
            Nothing
        """
        sufficient_quality = self._has_sufficient_quality(p)
        if (self.policy.ignore_quality and
                ((self.combine_IOs and COT != COT.PERIODIC)
                     or (not self.combine_periodic_IOs and incoming_message.cot == COT.PERIODIC))):
            s = f"Cannot handle datapoint {p} oldpoint: {prev_point} and msg {incoming_message}"
            self.logger.warning(s)

        elif incoming_message.cot == COT.UNKNOWN_CAUSE \
                and not self.policy.ignore_unknown_cot_dp_callbacks:
            raise NotImplementedError()
        else:
            raise NotImplementedError(f"missing handler for point {p} with cot {incoming_message.cot}"
                                      f" - suff. quality: {sufficient_quality}; "
                                      f"combine:periodic IOs: "
                                      f"{self.combine_IOs}:{self.combine_periodic_IOs}")

    def _has_sufficient_quality(self, p: IEC104Point) -> bool:
        """ Checks if a newly transmitted dp has a quality sufficient in resp. to the subscription policy

        Args:
            p: newly incoming dp

        Returns:
            True if the dp is considered of sufficient quality *For this interface*, False otherwise
        """
        # TODO: adapt to new wattson-quality format
        if self.policy.ignore_quality:
            sufficient_quality = True
        else:
            sufficient_quality = p.quality.is_good
        return sufficient_quality

    def _gen_msg_for_periodic_update_if_necessary(self, p: IEC104Point) -> Optional[PeriodicUpdate]:
        if not self.combine_periodic_IOs:
            return PeriodicUpdate(
                p.coa,
                {p.ioa: p.value},
                {p.ioa: p.updated_at_ms},
                p.type,
                self.main_handler.next_mtu_reference_number,
            )

    @property
    def cache(self) -> MessageCache:
        return self.main_handler.msg_cache

    @property
    def policy(self) -> SubPolicy:
        return self.main_handler.subscription_policy

    @property
    def combine_periodic_IOs(self) -> bool:
        """ If to combine trickling periodicly updated Information Objects into a single message"""
        return self.policy.combine_periodic_IOs

    @property
    def combine_IOs(self) -> bool:
        """ if to combine multiple Information Objects into a singular message """
        return self.policy.combine_IOs
