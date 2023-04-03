from typing import TYPE_CHECKING

from wattson.iec104.common.datapoint import IEC104Point

from wattson.apps.interface.util.messages import *
from wattson.apps.interface.util.msg_status import MsgStatus
from wattson.apps.interface.manager.message_generators import generate_confirmation_received_handler
from wattson.apps.interface.manager.MTU_cache import MessageCache
from wattson.apps.interface.manager.configs import SubPolicy

if TYPE_CHECKING:
    from wattson.apps.interface.manager.iec104.iec_message_handler import IEC104Handler


class OnReceiveAPDUHandler:
    def __init__(self, main_handler: 'IEC104Handler'):
        self.main_handler = main_handler
        self.logger = self.main_handler.logger

    def generate_msg_and_update_cache(self, apdu: APDU) -> Optional[IECMsg]:
        """ General handler for generating an IECMsg on incoming data

        Args:
            apdu: incoming data

        Raises:
            NotImplementedError on msg-ids not yet supported (ATM File-transfer and such) or if non-I-Format APDU
            RuntimeError if expected to generate msg. but for some reason did not in the sub-routines

        Returns:
            None if nothing needs to be send, else some IECMsg
        """
        # return Confirmation if response to command/request, otherwise return Msg-Direction msg
        result = {}

        if not isinstance(apdu, I_FORMAT):
            raise NotImplementedError(f"Non-I format apdu {apdu} not supported for updating cache-pipe yet.")

        # most complex case due to many different payloads
        msg_id = MsgID.from_type(apdu.type, apdu.cot)

        if msg_id in (MsgID.PROCESS_INFO_MONITORING, MsgID.PERIODIC_UPDATE,
                      MsgID.SYS_INFO_MONITORING):
            msg = self._gen_msg_update_cache_for_IOs(msg_id, apdu)

        elif msg_id in (MsgID.PROCESS_INFO_CONTROL, MsgID.READ_DATAPOINT,
                        MsgID.SYS_INFO_CONTROL, MsgID.PARAMETER_ACTIVATE):
            msg = self._gen_msg_update_cache_for_expected_acks(msg_id, apdu)

        else:
            raise NotImplementedError(f"msg_id: {str(msg_id)}, {apdu}")

        if apdu.cot in (COT.ACTIVATION_CONFIRMATION, COT.ACTIVATION_TERMINATION) \
                and not self.policy.acks:
            # only needed to update cache, don't forward acks to subscribers
            return

        if msg_id in (MsgID.PROCESS_INFO_CONTROL, MsgID.READ_DATAPOINT, MsgID.SYS_INFO_CONTROL,
                      MsgID.PARAMETER_ACTIVATE):
            return generate_confirmation_received_handler(apdu, msg)

        if result and msg != "":
            return Confirmation(result, max_tries=msg.max_tries, reference_nr=msg.reference_nr)
        if msg:
            return msg

        raise RuntimeError(
            f"Receive-Generator was supposed to handle apdu {apdu} "
            f"but did not generate any message"
        )

    def _update_cache(self, ioa: int, apdu: I_FORMAT) -> IECMsg:
        if apdu.type.global_coa_compatible:
            # return self._update_global_cache_on_receive(apdu)
            return self._update_global_cache(apdu)

        entry = self.cache.get_entry_if_send_and_nonterminated(apdu.coa, ioa)

        if apdu.type in range(45, 70):
            assert entry
            if not apdu.positive and apdu.cot == COT.ACTIVATION_CONFIRMATION:
                msg = self.cache.pop_active_msg(apdu.coa, ioa)
                new_msg = Confirmation(
                    {'result': ConfirmationStatus.FAIL,
                     'reason': FailReason.NEGATIVE},
                    msg.reference_nr,
                    msg.max_tries
                )
                return new_msg
            elif apdu.cot == COT.ACTIVATION_CONFIRMATION:
                self.cache.archive_as_confirmed(apdu.coa, ioa)
                return entry.msg
            elif apdu.cot == COT.ACTIVATION_TERMINATION:
                self.cache.remove_archived_entry(apdu.coa, ioa)
                return entry.msg

        elif apdu.type in range(1, 45):
            if entry:
                raise NotImplementedError("Currently not supporting handling this in raw.")
            elif apdu.cot == COT.INTERROGATED_BY_STATION:
                raise NotImplementedError("Currently not supporting handling this in raw.")
            # RTU-initiated
            assert not entry
            raise ValueError("No reason to not handle it b4")

        if apdu.type == TypeID.C_RD_NA_1:
            raise RuntimeError(
                f"Not sure if negative ACTCONs are allowed to be send for this"
                f"See -104 7.2."
                "Should also be handled before it"
            )

    def _gen_msg_update_cache_for_IOs(self, msg_id: MsgID, apdu: I_FORMAT) -> Optional[IECMsg]:
        """ Updates general message cache and builds msg

        Args:
            msg_id: Type of Msg to be send
            apdu: newly received APDU in I-Format

        Returns:
            Msg if necessary, else None
        """
        if msg_id == MsgID.PROCESS_INFO_MONITORING:
            return self._gen_msg_for_received_process_info_apdu(apdu)
        elif msg_id == MsgID.PERIODIC_UPDATE:
            return self._optional_msg(apdu)
        elif msg_id == MsgID.SYS_INFO_MONITORING:
            # how hand over COI etc?
            return SysInfoMonitoring(apdu.coa)

    def _gen_msg_update_cache_for_expected_acks(self, msg_id: MsgID, apdu: I_FORMAT) -> Optional[IECMsg]:
        """ Genereates a msg & updates cache for incoming *expected* Acknowledgements (ACT_CON/ ACT_TERM)
            DEACT_CON/ DEACT_TERM not supporte yet by IEC104 and thereby not yet impplemented here.

        Args:
            msg_id: Msg the ack refers to
            apdu: incoming ACK data

        Returns:

        """
        msg = None

        def update_status_for_all_dps() -> None:
            for ioa in apdu.ioas:
                nonlocal msg
                msg = self._update_cache(ioa, apdu)

        if msg_id == MsgID.PROCESS_INFO_CONTROL:
            with self.cache.dp_cache_lock:
                assert self.cache.is_dp_active(apdu.coa, apdu.ioas[0])
                update_status_for_all_dps()

        elif msg_id == MsgID.READ_DATAPOINT:
            with self.cache.dp_cache_lock:
                update_status_for_all_dps()

        elif msg_id == MsgID.SYS_INFO_CONTROL:
            with self.cache.global_cache_lock:
                if apdu.type.global_coa_compatible:
                    # TODO: Need to be deleted once the it's fixed on the connector side
                    #if apdu.cot == COT.ACTIVATION_CONFIRMATION and apdu.type == TypeID.C_CS_NA_1:
                    #    apdu.coa = 163
                    if not self.cache.global_send_and_active(apdu.coa, apdu.type):
                        self.logger.critical(f'coa-type combi not set as send and active before reply: '
                                             f'{apdu.coa=} {apdu.type=} {apdu.cot=}')
                        self.logger.critical(self.cache._active_global_coa_cache)
                        self.logger.critical(self.cache._active_global_per_rtu_cache)

                    assert self.cache.global_send_and_active(apdu.coa, apdu.type)
                msg = self._update_cache(0, apdu)

        elif msg_id == MsgID.PARAMETER_ACTIVATE:
            with self.cache.param_cache_lock:
                msg = self.cache.pop_param_mesage(apdu.coa, apdu.ioas[0])

        return msg

    def _update_global_cache(self, apdu: I_FORMAT) -> IECMsg:
        """ Checks what msg compatible with global-COAs this apdu is a reply to and fixes the resp. cache

        Args:
            apdu: incoming data

        Returns:
            msg indicating the success- & ACK-status effectively ending the global command for this RTU
        """
        if apdu.type == TypeID.C_RP_NA_1:
            raise NotImplementedError("Want to make it forward the IOs reset")
        entry = self.cache.get_global_entry_if_is_active(apdu.coa, apdu.type)
        assert entry
        self.main_handler.raise_on_overwriting_nonterminated_global(apdu, entry)
        assert (
            (apdu.cot == COT.ACTIVATION_TERMINATION and entry.status == MsgStatus.RECEIVED_ACK)
            or (apdu.cot == COT.ACTIVATION_CONFIRMATION and entry.status == MsgStatus.SEND_NO_ACK)
            # TODO: Fix once I figured out why double-calls
            or (apdu.type == TypeID.C_CS_NA_1)
        )

        if apdu.cot == COT.ACTIVATION_CONFIRMATION and apdu.positive:
            # global cache is separated into a space of "just confirmed" and "fully removed"
            # since we ACT_TERM is not strictly necessary to be send
            self.cache.confirm_global_entry(apdu.coa, apdu.type)
            return entry.msg
        elif apdu.cot == COT.ACTIVATION_CONFIRMATION:
            # TODO: is that valid? only really if max_tries <= 1, or IDK ?
            self.cache.remove_global_entry(apdu.coa, apdu.type)
            return entry.msg
        elif apdu.cot == COT.ACTIVATION_TERMINATION:
            self.cache.remove_global_entry(apdu.coa, apdu.type, remove_global_coa_if_done=True)
            return entry.msg

        raise NotImplementedError()

    def _optional_msg(self, apdu: APDU) -> Optional[IECMsg]:
        """
        Handles APDUs which may not need be transmitted to subscriber yet/ not at all.

        Args:
            apdu: inc data

        Returns:
            Msg if to be send, else None
        """
        result = {}
        msg = None

        if isinstance(apdu, I_FORMAT):
            # TODO: why was this already w/e the raw-callback part? was it never called??
            # We should not use raw callbacks anymore anyway, were a hack which are to be
            # fixed by feature requests.
            if not self.policy.need_to_handle_apdu(apdu, raw_callback=False):
                return None

            if apdu.cot == COT.INTERROGATED_BY_STATION:
                msg, result = self._on_interro_by_station(apdu)

            elif apdu.cot == COT.INTERROGATION:
                msg, result = self._on_read_dp_reply(apdu)

            elif apdu.cot == COT.PERIODIC:
                if self.combine_periodic_IOs:
                    raise NotImplementedError("Cannot combine IOs yet")
                else:
                    return PeriodicUpdate(
                        apdu.coa, {ioa: -1 for ioa in apdu.ioas}, {ioa: -1 for ioa in apdu.ioas},
                        apdu.type, self.main_handler.next_mtu_reference_number
                    )

            elif apdu.cot == COT.SPONTANEOUS:
                if self.combine_IOs:
                    raise NotImplementedError("Cannot combine IOs yet")
                else:
                    return ProcessInfoMonitoring(
                        apdu.coa, {ioa: -1 for ioa in apdu.ioas}, {ioa: -1 for ioa in apdu.ioas},
                        apdu.type, apdu.cot, self.main_handler.next_mtu_reference_number
                    )

        if result and msg is not None:
            return Confirmation(result=result, max_tries=msg.max_tries, reference_nr=msg.reference_nr)
        elif msg:
            return msg
        else:
            return None

    def _gen_msg_for_received_process_info_apdu(self, apdu: I_FORMAT) -> IECMsg:
        """

        Args:
            apdu:

        Returns:

        """
        if apdu.cot == COT.INTERROGATED_BY_STATION:
            with self.cache.global_cache_lock:
                assert self.cache.is_interro_send_and_active(apdu.coa)
                return self._optional_msg(apdu)

        if apdu.cot == COT.INTERROGATION:
            with self.cache.dp_cache_lock:
                return self._optional_msg(apdu)

        if apdu.cot in (COT.PERIODIC, COT.SPONTANEOUS):
            return self._optional_msg(apdu)
        raise RuntimeError(f"Invalid handler for process_info_monitoring apdu: {apdu}")

    def _on_read_dp_reply(self, apdu: I_FORMAT) -> Tuple[IECMsg, dict]:
        """ Removes existing cache-entry & parses apdu to msg & result

        Args:
            apdu: incoming read-reply (COT=5, typeID < 45)

        Returns:
            Msg (populated with read-reply is successful, else read-cmd msg),
             result (populated only if read failed, i.e., apdu.negative )
        """
        result = {}
        active_entry = self.cache.get_entry_if_dp_is_active(apdu.coa, apdu.ioas[0])
        assert active_entry
        assert active_entry.status == MsgStatus.SEND_NO_ACK
        # If read-messages are changed to support > 1 IOA, need to change this part
        self.cache.remove_active_entry(apdu.coa, apdu.ioas[0])
        if apdu.positive:
            msg = ProcessInfoMonitoring(
                apdu.coa,
                {ioa: -1 for ioa in apdu.ioas},
                {ioa: -1 for ioa in apdu.ioas},
                apdu.type,
                apdu.cot,
                active_entry.msg.reference_nr,
            )
        else:
            msg = active_entry.msg
            result["status"] = ConfirmationStatus.FAIL
            result["reason"] = FailReason.NEGATIVE
            result["affected ioas"] = apdu.ioas
        return msg, result

    def _on_interro_by_station(self, apdu: I_FORMAT) -> Tuple[ProcessInfoMonitoring, dict]:
        """ Parses (general) interrogation mass-replies (typeID 20 - 36)

        Args:
            apdu: rcvd Monitoring data

        Returns:
           Msg (monitoring-data if successful/ original send interro msg otherwise),
           result (populated only if apdu is negative)
        """
        result = {}
        entry = self.cache.get_interro_entry(apdu.coa)
        reference_nr = entry.msg.reference_nr
        # could ignore since we have per-datapoint msgs right now from other handler.
        if apdu.positive:
            if not entry.status == MsgStatus.RECEIVED_ACK:
                # Sending ACTCON for interro is optional
                entry.set_confirmed()

            msg = ProcessInfoMonitoring(
                apdu.coa, {ioa: -1 for ioa in apdu.ioas}, {ioa: -1 for ioa in apdu.ioas},
                apdu.type, apdu.cot, reference_nr
            )
        else:
            msg = entry.msg
            result["status"] = ConfirmationStatus.FAIL
            result["reason"] = FailReason.NEGATIVE
            result["affected ioas"] = apdu.ioas
        return msg, result

    @property
    def cache(self) -> MessageCache:
        return self.main_handler.msg_cache

    @property
    def mtu_prefix(self) -> str:
        return self.main_handler.mtu_prefix

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
