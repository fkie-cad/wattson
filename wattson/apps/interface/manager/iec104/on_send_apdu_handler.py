import logging
from typing import TYPE_CHECKING

from wattson.iec104.common import GLOBAL_COA

from wattson.apps.interface.util.messages import *
from wattson.apps.interface.util.msg_status import MsgStatus
from wattson.apps.interface.manager.MTU_cache import CacheEntry, MessageCache

if TYPE_CHECKING:
    from wattson.apps.interface.manager.iec104.iec_message_handler import IEC104Handler


class OnSendAPDUHandler:
    """
    Handles all APDUs that *were just send out*
    """
    def __init__(self, main_handler: 'IEC104Handler'):
        self.main_handler = main_handler
        self.logger = main_handler.logger

    def generate_msg_and_update_cache(self, apdu: I_FORMAT, rtu_coa: int, next_msg_cnt: int) -> IECMsg:
        """ Main function which generates msg/ updates it if entry in cache is already existing

        Args:
            apdu: data send out just now
            rtu_coa: COA of the RTU it was just send to (important for apdu.coa == 65535 )
            next_msg_cnt: ID of next msg if initiated by the MTU (e.g., for gen-interro)

        Returns:
            Msg that identifies the content of the apdu and references the application's cmd if existing
        """
        if not isinstance(apdu, I_FORMAT):
            raise NotImplementedError(
                f"Received handler called with APDU expected to be handled: {apdu}"
            )

        assert apdu.send_from_MTU
        assert apdu.coa != GLOBAL_COA or rtu_coa != -1

        msg_id = MsgID.from_type(apdu.type, apdu.cot)
        reference_nr = f"{self.mtu_prefix}_{next_msg_cnt}"

        if apdu.type.global_coa_compatible:
            # on types that are global-coa compatible, the cache still needs to be blocked
            # bc. a global-update requested before this is finished would collide on the resp. COA
            msg = self._gen_msg_and_update_for_global_compatible_apdus(apdu, reference_nr, rtu_coa)
        else:
            # see 60870-5-5 6.1.4; wont get any further reply from these IOAs
            msg = self._gen_msg_and_update_for_non_global_compatible_apdus(apdu, msg_id, reference_nr)

        if not msg:
            raise NotImplementedError(f"Missing msg handler for apdu:\n{apdu}\nid: {msg_id}")

        if not msg.mtu_initiated:
            return Confirmation.from_result_and_previous_msg({'status': ConfirmationStatus.SUCCESSFUL_SEND}, msg)
        return msg

    def _gen_msg_and_update_for_global_compatible_apdus(self, apdu: I_FORMAT, reference_nr: str, rtu_coa: int)\
            -> IECMsg:
        """
        Handles just send APDUs whose typeIDs are compatible with setting a global-COA

        Args:
            apdu: send apdu with type for which it is allowed to send it with global COA
            reference_nr: ID msg will refer to
            rtu_coa: COA for RTU the apdu will be send to, explicitly necessary for apdu.coa == global

        Returns:
            Msg to be published
        """
        with self.cache.global_cache_lock:
            new_msg = IECMsg.from_apdu(apdu, reference_nr, send=True)
            msg = self._update_cache(0, apdu, rtu_coa, new_msg)
        return msg

    def _gen_msg_and_update_for_non_global_compatible_apdus(self, apdu: I_FORMAT, msg_id: MsgID, reference_nr: str)\
            -> Optional[IECMsg]:
        """
        Handles all APDUs whose types can *never* be combined with a global-coa

        Args:
            apdu: just send data
            msg_id: type of msg that will be published
            reference_nr: ID (new vs. old cmd-ID) which the msg will refer to

        Returns:
            Msg updating subscribers of the APDU-content (if necessary), else None
        """
        msg = None

        if msg_id == MsgID.SYS_INFO_MONITORING:
            self._handle_send_sys_info_monitoring(apdu)

        elif msg_id == MsgID.PARAMETER_ACTIVATE:
            # only has 1 IOA
            with self.cache.param_cache_lock:
                entry = self.cache.get_param_entry_if_active(apdu.coa, apdu.ioas[0])
                entry.status = MsgStatus.SEND_NO_ACK
                msg = entry.msg
        else:
            # various other formats that, e.g., where the reply can be ACT_TERM
            #   instead of just ACT_CON/ a read-reply
            # TODO: Could to making the new_msg to update_cache ..; they would just need the ref.nr
            new_msg = IECMsg.from_apdu(apdu, reference_nr, send=True)
            with self.cache.dp_cache_lock:
                msg = self._update_msg_and_cache_for_all_ioas(new_msg, apdu)
                if not msg.mtu_initiated and not isinstance(msg, ProcessInfoControl):
                    # due to > 1 callbacks when setting mutliple dps
                    # for ProcInfoControl
                    msg.max_tries -= 1
        return msg

    def _update_cache(self, ioa: int, apdu: I_FORMAT, rtu_coa: int, new_msg: Optional[IECMsg] = None) -> IECMsg:
        """ general cache-update handler, no entirely new message will be generated from here on.

        Args:
            ioa: IOA of object to be updated
            apdu: data just send the IOA belongs to
            rtu_coa: RTU the apdu is send to
            new_msg: new-msg if so far not yet existing in cache

        Returns:
            Msg to publish
        """
        assert ioa in apdu.ioas

        if apdu.type.global_coa_compatible:
            return self._update_global_cache(apdu, rtu_coa, new_msg)

        elif apdu.type.carries_parameter_modification:
            return self._update_param_cache(apdu)

        else:
            entry = self.cache.get_queued_dp_entry_if_exists(apdu.coa, ioa)
            if entry is None:
                assert new_msg is not None

                entry = CacheEntry(new_msg, MsgStatus.SEND_NO_ACK)
                self.cache.store_new_active_dp(apdu.coa, ioa, entry)
            else:
                entry.status = MsgStatus.SEND_NO_ACK

            return entry.msg

    def _update_global_cache(self, apdu: I_FORMAT, rtu_coa: int, new_msg: Optional[IECMsg]) -> IECMsg:
        """
        handles cache for all outgoing APDUs that are compatible with global-COAs

        Args:
            apdu: send data
            rtu_coa: RTU the apdu is send to
            new_msg: msg generated for apdu if no related entry from a cmd already exists

        Returns:
            Msg to be published; references command if it already existed
        """
        assert apdu.type.global_coa_compatible
        if apdu.coa == GLOBAL_COA:
            # deactivation-COTs not yet implemented
            self.logger.debug('handles APDU in update global cache with global coa')
            assert apdu.cot == COT.ACTIVATION

            entry = self.cache.get_global_coa_entry_if_is_queued(apdu.type)
            if entry is not None:
                assert entry.status == MsgStatus.WAITING_FOR_SEND

                entry.decrement_msg_max_tries()
                self.cache.set_global_command_active(apdu.type, rtu_coa)
            else:
                assert new_msg is not None and rtu_coa != -1
                entry = CacheEntry(new_msg, MsgStatus.SEND_NO_ACK)
                self.cache.store_global(rtu_coa, apdu.type, entry, True)
        else:
            entry = self.cache.pop_queued_global_if_is_active(apdu.coa, apdu.type)
            if entry is None:
                assert new_msg is not None
                entry = CacheEntry(new_msg, MsgStatus.SEND_NO_ACK)
            else:
                entry.status = MsgStatus.SEND_NO_ACK

            self.cache.store_global(apdu.coa, apdu.type, entry)

        return entry.msg

    def _update_param_cache(self, apdu: I_FORMAT) -> IECMsg:
        """
        Sets a parameter-changing entry to active, waiting for ACK

        Args:
            apdu: data send, assumes only 1 IO is contained

        Returns: Msg saying that the APDU was send

        """
        entry = self.cache.get_param_entry_if_active(apdu.coa, apdu.ioas[0])
        entry.status = MsgStatus.SEND_NO_ACK
        return entry.msg

    def _update_msg_and_cache_for_all_ioas(self, msg: IECMsg, apdu: I_FORMAT) -> IECMsg:
        """
        Sends an update to the cache for all IOAs contained in the IOA; also overwrites msg for each IO

        Args:
            msg: base-msg from APDU
            apdu: send I-Format APDU

        Returns:
            Msg with all necessary APDU-infos added
        """
        for ioa in apdu.ioas:
            msg = self._update_cache(ioa, apdu, apdu.coa, msg)
        return msg

    @property
    def cache(self) -> MessageCache:
        return self.main_handler.msg_cache

    @property
    def mtu_prefix(self) -> str:
        return self.main_handler.mtu_prefix
