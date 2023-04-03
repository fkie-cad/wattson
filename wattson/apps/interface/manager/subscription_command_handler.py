import logging
from typing import TYPE_CHECKING

from wattson.util import log_contexts

from wattson.apps.interface.util.messages import *
from wattson.apps.interface.util.constants import STILL_SEND_KEY
from wattson.apps.interface.manager.MTU_cache import CacheEntry, MessageCache
from wattson.apps.interface.util.msg_status import MsgStatus
from wattson.iec104.interface.client import IECClientInterface
from wattson.iec104.interface.types import TypeID, COT

if TYPE_CHECKING:
    from wattson.apps.interface.manager import SubscriptionManager


class SubscriptionCommandHandler:
    def __init__(self, subscription_manager: 'SubscriptionManager'):
        self.manager = subscription_manager
        self.logger = self.manager.logger.getChild('cmdH')
        self.logger.setLevel(logging.INFO)
        add_contexts = {log_contexts.C_RD}
        #self.logger.add_contexts(add_contexts)

    def handle(self, msg: IECMsg) -> Confirmation:
        if msg.id in (MsgID.READ_DATAPOINT, MsgID.SYS_INFO_CONTROL, MsgID.PROCESS_INFO_CONTROL,
                      MsgID.FILE_TRANSFER_REQ, MsgID.PARAMETER_ACTIVATE):
            if not self.iec_client.has_server(msg.coa):
                self.logger.critical("Unknwon server")
                return Confirmation({"status": ConfirmationStatus.FAIL,
                                     "reason": FailReason.COA},
                                    msg.reference_nr, msg.max_tries)

        sub_handlers = {
            MsgID.PROCESS_INFO_CONTROL: self._on_process_info_control,
            MsgID.READ_DATAPOINT: self._on_read_datapoint,
            MsgID.TOTAL_INTERRO_REQ: self._on_total_interro_request,
            MsgID.RTU_STATUS_REQ: self._on_rtu_status_request,
            MsgID.PARAMETER_ACTIVATE: self._on_parameter_activation,
            MsgID.SYS_INFO_CONTROL: self._on_sys_info_control,
            MsgID.MTU_CACHE_REQ: self._on_mtu_cache_request
        }
        if msg.id not in sub_handlers:
            self.logger.warning(f"Failed to handle subscription command msg {msg}")
            raise NotImplementedError(f"Failed to handle subscription command msg {msg}")
        res = sub_handlers[msg.id](msg)
        if res is not None and isinstance(res, Confirmation) and res.result["status"] == ConfirmationStatus.FAIL:
            self._clear_msg_active(msg)
        return res

    def check_collision(self, msg: Union[ProcessInfoControl, ReadDatapoint]) -> Optional[Confirmation]:
        coa = msg.coa
        result = {}
        if isinstance(msg, ProcessInfoControl):
            ioas = set(msg.val_map.keys())
        else:
            ioas = {msg.ioa}
        for ioa in ioas:
            with self.msg_cache.dp_cache_lock:
                entry = self.msg_cache.get_entry_if_dp_is_active(coa, ioa)
            if entry and msg.queue_on_collision:
                #self.add_cmd_to_queue(entry.msg, coa, ioa)
                result = {
                    "status": ConfirmationStatus.QUEUED,
                    "reason": FailReason.COLLISION,
                    "collision_dp": f"{coa}:{ioa}",
                    "collision_reference": entry.msg.reference_nr,
                    "coa": coa,
                    "ioa": ioa,
                }
            elif entry:
                result = {
                    "status": ConfirmationStatus.FAIL,
                    "reason": FailReason.COLLISION,
                    "collision_dp": f"{coa}:{ioa}",
                    "collision_reference": entry.msg.reference_nr,
                    "coa": coa,
                    "ioa": ioa
                }

            if result:
                return Confirmation(result, msg.reference_nr, msg.max_tries)
        return None

    def _on_process_info_control(self, msg: ProcessInfoControl) -> Confirmation:
        result = {}
        success = False
        dps_send_successful = set()
        available_fails = msg.max_tries
        for ioa, val in msg.val_map.items():
            point = self.iec_client.get_datapoint(msg.coa, ioa, False)
            if not point:
                result = {"status": ConfirmationStatus.FAIL,
                          "reason": FailReason.IOA,
                          "Requested ioas": [_ioa for _ioa in msg.val_map],
                          "error-ioa": ioa,
                          }
                break
            val = TypeID(point.type).convert_val_by_type(val)
            point.value = val
            with self.msg_cache.dp_cache_lock:
                if self.msg_cache.is_dp_active(msg.coa, ioa):
                    result = {
                        "status": ConfirmationStatus.FAIL,
                        "reason": FailReason.COLLISION,
                        "error-ioa": ioa,
                    }
                    break
                entry = CacheEntry(msg, MsgStatus.WAITING_FOR_SEND)
                self.msg_cache.store_new_active_dp(msg.coa, ioa, entry)

            self.iec_client.update_datapoint(msg.coa, ioa, val)
            success = False
            while not success and available_fails > 0:
                self.logger.info(f"Sending {msg.coa}.{ioa} = {val}...")
                success = self.iec_client.send(msg.coa, ioa, COT.ACTIVATION)
                if not success:
                    self.logger.warning(f"Sending {msg.coa}.{ioa} = {val} failed")
                    available_fails -= 1

            if success:
                dps_send_successful.add(ioa)
            else:
                with self.msg_cache.dp_cache_lock:
                    self.msg_cache.remove_active_entry(msg.coa, ioa)
                break

        if not success:
            result = {"status": ConfirmationStatus.FAIL,
                      "reason": FailReason.NETWORK,
                      STILL_SEND_KEY: dps_send_successful}
        else:
            result = {"status": ConfirmationStatus.WAITING_FOR_SEND} if not result else result
        return Confirmation(result, msg.reference_nr, msg.max_tries)

    def _on_read_datapoint(self, msg: ReadDatapoint) -> Confirmation:
        result = {"status": ConfirmationStatus.WAITING_FOR_SEND}
        if not self.iec_client.has_datapoint(msg.coa, msg.ioa):
            result = {"status": ConfirmationStatus.FAIL,
                      "reason": FailReason.IOA}
        else:
            with self.msg_cache.dp_cache_lock:
                entry = CacheEntry(msg, MsgStatus.WAITING_FOR_SEND)
                self.msg_cache.store_new_active_dp(msg.coa, msg.ioa, entry)

            dp = self.iec_client.get_datapoint(msg.coa, msg.ioa, as_dict=False)
            if dp is None:
                raise RuntimeError("Should be caught by has_datapoint")

            success = False
            while not success and entry.msg.max_tries > 0:
                self.logger.debug("[Try read()]")
                success = dp.read()
                entry.msg.max_tries -= 1

            if success:
                result["status"] = ConfirmationStatus.WAITING_FOR_SEND
                # enable if read-msg allows for multi-dp-read
                # if self.subscription_policy["combine_IOs"]:
                # entry.IO_cache[dp.io_address] = dp.value
            else:
                #with self.msg_cache.dp_cache_lock:
                #    self.msg_cache.remove_active_entry(msg.coa, msg.ioa)
                result["status"] = ConfirmationStatus.FAIL
                result["reason"] = FailReason.NETWORK

        return Confirmation(result, msg.reference_nr, msg.max_tries)

    def _clear_msg_active(self, msg):
        if isinstance(msg, (ProcessInfoControl, ReadDatapoint)):
            if isinstance(msg, ProcessInfoControl):
                ioas = set(msg.val_map.keys())
            else:
                ioas = {msg.ioa}
            with self.msg_cache.dp_cache_lock:
                for ioa in ioas:
                    self.logger.debug(f"Clearing active Entry: {msg.coa}.{ioa}")
                    self.msg_cache.archive_as_confirmed(msg.coa, ioa)

    def _on_total_interro_request(self, msg: TotalInterroReq) -> TotalInterroReply:
        res = TotalInterroReply(self.manager.get_RTU_status(),
                                self.manager.get_MTU_datapoints(),
                                msg.reference_nr)
        return res

    def _on_rtu_status_request(self, msg: RTUStatusReq) -> RTUStatusReply:
        res = RTUStatusReply(self.manager.get_RTU_status(),
                             msg.reference_nr)
        return res

    def _on_mtu_cache_request(self, msg: MtuCacheReq) -> MtuCacheReply:
        res = MtuCacheReply(self.manager.get_MTU_cache(), msg.reference_nr)
        return res

    def _on_parameter_activation(self, msg: ParameterActivate) -> Confirmation:
        cot = COT.ACTIVATION if msg.activate else COT.DEACTIVATION
        result = self._check_dp_and_generate_result_if_invalid(msg)
        if result != {}:
            return result

        with self.msg_cache.param_cache_lock:
            if self.msg_cache.is_param_active(msg.coa, msg.ioa):
                raise RuntimeError("Would ask cache to overwrite param; should have been tested before.")
            entry = CacheEntry(msg, MsgStatus.WAITING_FOR_SEND)
            self.msg_cache.store_new_active_param(msg.coa, msg.ioa, entry)

        success = False
        while not success and entry.msg.max_tries > 0:
            try:
                success = self.iec_client.send_P_AC(msg.coa, msg.ioa, cot)
            except NotImplementedError:
                entry.msg.max_tries = 0
                break
            entry.msg.max_tries -= 1

        if not success:
            # failed after all retries
            result = {
                "status": ConfirmationStatus.FAIL,
                "reason": FailReason.NETWORK
            }
            with self.msg_cache.param_cache_lock:
                self.msg_cache.remove_param_entry(msg.coa, msg.ioa)
        else:
            result = {"status": ConfirmationStatus.WAITING_FOR_SEND}

        return Confirmation(result, msg.reference_nr, msg.max_tries)

    def _on_sys_info_control(self, msg: SysInfoControl) -> Confirmation:
        if msg.type_ID not in (TypeID.C_CS_NA_1, TypeID.C_IC_NA_1):
            result = {
                "status": ConfirmationStatus.FAIL,
                "reason": FailReason.TYPE_UNSUPPORTED
            }
        else:
            success = self.iec_client.send_sys_info_non_read(msg.type_ID, msg.coa)
            if success:
                result = {"status": ConfirmationStatus.WAITING_FOR_SEND}
            else:
                result = {
                    "status": ConfirmationStatus.FAIL,
                    "reason": FailReason.NETWORK
                }
        return Confirmation(result, msg.reference_nr, msg.max_tries)

    def _check_dp_and_generate_result_if_invalid(self, msg: ParameterActivate) -> Dict:
        if not self.iec_client.has_server(msg.coa):
            return {"result": ConfirmationStatus.FAIL,
                    "reason": FailReason.COA}
        elif not self.iec_client.has_datapoint(msg.coa, msg.ioa):
            self.logger.warning("unknown IOA")
            self.logger.warning(f"dps of that RTU: {sorted(self.iec_client.iec_masters[msg.coa].datapoints.keys())}")
            raise RuntimeError
            return {"result": ConfirmationStatus.FAIL,
                    "reason": FailReason.IOA}
        else:
            return {}

    @property
    def iec_client(self) -> IECClientInterface:
        return self.manager.iec_client

    @property
    def msg_cache(self) -> MessageCache:
        return self.manager.msg_cache
