import logging
import queue
import time
import threading as th
from typing import Iterable, TYPE_CHECKING, List

from wattson.util import log_contexts
from wattson.apps.interface.util.messages import *

from wattson.apps.interface.manager.MTU_cache import MessageCache
from wattson.apps.interface.manager.configs import DEFAULT_SUB_POLICY, SubPolicy
from wattson.apps.interface.manager.subscription_command_handler import SubscriptionCommandHandler
from wattson.apps.interface.manager.iec104.iec_message_handler import IEC104Handler

from wattson.iec104.interface.client import IECClientInterface
from wattson.iec104.interface.types import COT
from wattson.iec104.common import ConnectionState, MTU_UPDATE_INTERVAL_S
from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.common.iec104message import IEC104Message

if TYPE_CHECKING:
    from wattson.hosts.mtu.mtu import MTU
    from wattson.apps.interface.servers import PublishingServer
    from wattson.apps.interface.servers import CommandServer


class SubscriptionManager(th.Thread):
    """
    The subscription manager handles all IEC104 APDUs received/send by the client to Subscription-Message cases.
    The internal function-order usually consists of:
        Public callback -> private callback that reduces changes between C104 and PYIEC104
            -> generate message to be published -> tell publish-server to send it off
    While the messages are generated, the MTU-Cache that holds the Message-Status for all those which a
        future IEC104-APDU might reference to. For instance, ACTCON/ACTTERM & datapoint updates for C_IC, C_RD
    Depending on the subscription-policy chosen by the MTU, not all messages will be published; however,
        the internal message-cache needs to be updated anyway to prevent corruption.
    """

    def __init__(self, mtu: 'MTU', rtus: Iterable[int], **kwargs):
        """

        Args:
            mtu: Which MTU the mgr is based on
            rtus: COAs of all RTUs the MTU is connected to
            **kwargs:
                - periodic_update_timeout (def. 20) ms to wait to bundle periodic updates before publishing
                - policy: Subscription policy of what, updates to automatically publish
        """
        super().__init__()
        # generate the message-caches in here?
        self._terminate = th.Event()
        self.received_new_periodic_updates = th.Event()
        # ms
        self.periodic_update_timeout = kwargs.get('periodic_update_timeout', 20) / 1000
        self._mtu = mtu
        additional_context = {log_contexts.ON_RECEIVE, log_contexts.C_IC}
        self.logger = self._mtu.logger.getChild("SubMgr")
        self.subscription_policy: SubPolicy = kwargs.get('policy', DEFAULT_SUB_POLICY)

        self.msg_cache = MessageCache(rtus, 5)
        self.periodic_queue = queue.Queue()
        self.periodic_cache = {}

        self._command_handler = SubscriptionCommandHandler(self)
        self._iec_handler = IEC104Handler(self, self._mtu.node_id, policy=self.subscription_policy)

        self.publishing_server = None
        self.command_server = None

        # If implementing deletion, adding to that queue
        self._confirmed_nonTerminated_entrys = queue.Queue()

    def check_is_executable(self, msg: Union[ProcessInfoControl, ReadDatapoint]) -> Optional[Confirmation]:
        """
        Checks for a read/write cmd if it is executable based on RTU-status & if a dp contained is already "busy"

        Args:
            msg: incoming cmd

        Returns:
            Confirmation stating either a negative (not-executable) or positive (cmd queued to be send)
        """
        state = self._mtu.get_single_RTU_conn_status(msg.coa)
        if state in (ConnectionState.CLOSED, ConnectionState.UNATTEMPTED_TO_CONNECT):
            self.logger.warning('Marking as network-fail before checking collision')
            return Confirmation(
                {'status': ConfirmationStatus.FAIL, 'reason': FailReason.NETWORK},
                reference_nr=msg.reference_nr, max_tries=msg.max_tries
            )
        return self._command_handler.check_collision(msg)

    def check_collision(self, msg: Union[ProcessInfoControl, ReadDatapoint]) -> Optional[Confirmation]:
        """
        Checks for a read/write cmd if this dp is already "busy" from a different cmd

        Args:
            msg: incoming cmd

        Returns:
            Confirmation stating either a collision occurred or that the new cmd will be executed.
        """
        return self._command_handler.check_collision(msg)

    def add_subscription_servers(self, publisher: 'PublishingServer', command_server: 'CommandServer'):
        """
        Sets servers through which to interact with external application

        Raises:
            RuntimeError if servers are already set and would be overwritten.

        Args:
            publisher: Server solely publishing info to all subscribers
            command_server: receives cmds from external applications

        Returns:
            None
        """
        if self.publishing_server is not None and self.command_server is not None:
            raise RuntimeError("Don't replace any subscription server.")
        self.publishing_server = publisher
        self.command_server = command_server

    def on_send_apdu(self, apdu: APDU, rtu_coa: int, raw_callback: bool = False):
        """
        Callback for both c104 and PYiec104 when a general apdu is send out.
        :param apdu: Bytes for the C104-format (not mandatory) or wattson.APDU
        :param rtu_coa: RTU-ID the APDU was send to. Resolves GLOBAL-COA APDUs
        """
        msg = self._iec_handler.on_send_apdu(apdu, rtu_coa)
        if msg:
            self.publishing_server.send_msg(msg)

    def on_receive_apdu(self, apdu: Union[bytes, APDU], raw_callback: bool = False, coa: int = -1):
        """
        Callback for both c104 and PYiec104 when a general apdu is send out.
        :param apdu: wattson.APDU
        :param raw_callback: if this was called from via any kind of raw_callback:
                            Necessary to know not corrupt the storage through
                            multi-handlings by different callbacks
        """
        if type(apdu) == I_FORMAT and coa != -1:
            # for fixing an c104-error TODO: remove once error fixed
            apdu.coa = coa

        msg = self._iec_handler.on_receive_apdu(apdu, raw_callback)
        if msg:
            self.publishing_server.send_msg(msg)

        self._send_next_command_if_free(apdu)

    def on_connection_change(self, coa: int, connected: bool, ip: str, port: int):
        """
        Builds a message when the connection status (conn/ unconn) of any RTU changes
        If disconnected, publishes a msg mentioning every cmd-id that was cancelled by the disconnect.

        Args:
            coa: COA of RTU with conn-change
            connected: new connection-status
            ip: IP of RTU
            port: port of RTU

        Returns:
            None
        """
        msg = ConnectionStatusChange(coa, connected, ip, port,
                                     self._iec_handler.next_mtu_reference_number)
        self.logger.debug(f"conn change {msg}")
        try:
            self.publishing_server.send_msg(msg)
        except Exception as e:
            self.logger.error(f"{e=}")
        if not connected:
            cancelled_ref_nrs = self.msg_cache.clean_cache_for_rtu(coa)
            cancel_msg = DisconnectCancelMsgsChange(
                coa, ip, port, self._iec_handler.next_mtu_reference_number, list(cancelled_ref_nrs)
            )
            self.publishing_server.send_msg(cancel_msg)

    def _send_next_command_if_free(self, apdu: APDU):
        """
        Checks for every IOA of the APDU if a cmd is queued for it,
        executing the cmd if the dp is no-longer active

        Args:
            apdu: incoming APDU

        Returns:
            None
        """
        if isinstance(apdu, I_FORMAT) and apdu.cot in (COT.ACTIVATION_CONFIRMATION,
                                                       COT.ACTIVATION_TERMINATION):
            with self.msg_cache.dp_cache_lock:
                for ioa in apdu.ioas:
                    if not self.msg_cache.is_dp_active(apdu.coa, ioa):
                        self.command_server.send_next_command(apdu.coa, ioa)

    def on_datapoint(self, p: IEC104Point, old_point: IEC104Point,
                     incoming_message: IEC104Message):
        """
        Handler executed on every new incoming dp.
        Publishes new dp-status to all,
        update internal cache based on it and execute next cmd for this dp if existing

        Args:
            p: new dp-status
            old_point: prior dp-status
            incoming_message: msg including header-etc. fields of the ASDU

        Returns:
            None
        """
        msg = self._iec_handler.on_datapoint(p, old_point, incoming_message)
        if msg:
            self.publishing_server.send_msg(msg)
        elif incoming_message.cot == COT.PERIODIC:
            # only called when combining periodic IOs
            self.periodic_queue.put(p.translate())
            self.received_new_periodic_updates.set()

        if incoming_message.cot == COT.INTERROGATION:
            if (isinstance(msg, Confirmation)
                    and msg.max_tries == 0 and msg.status == ConfirmationStatus.FAIL
                    or isinstance(msg, ProcessInfoMonitoring)):
                with self.msg_cache.dp_cache_lock:
                    if not self.msg_cache.is_dp_active(p.coa, p.ioa):
                        self.command_server.send_next_command(p.coa, p.ioa)
            else:
                self.logger.critical(
                    f"Don't know how to handle on_dp for msg {msg} and incoming_msg {incoming_message}."
                )

    def on_subscription_command(self, msg: IECMsg) -> Confirmation:
        """
        Handler for cmds from other applications

        Args:
            msg: rcvd cmd

        Returns:
            Confirmation-msg
        """
        return self._command_handler.handle(msg)

    def on_explicit_control_exit(self, coa: int, point: IEC104Point, success: bool, orig_cot: int):
        """
        Callback for when a set-cmd was send to the point.
        Publishes the pos/neg Confirmation

        Args:
            coa: COA of RTU the point belongs to
            point: point aimed at/ changed
            success: Success of cmd
            orig_cot: original cmd's COT

        Returns:
            None
        """
        with self.msg_cache.dp_cache_lock:
            entry = self.msg_cache.get_entry_if_dp_is_active(coa, point.ioa)
            if not entry:
                entry = self.msg_cache.get_entry_if_dp_is_confirmed(coa, point.ioa)
                if not entry:
                    # negative-bit set; handled already by on_receive_apdu
                    return

        if success:
            result = {"status": ConfirmationStatus.POSITIVE_CONFIRMATION}
        else:
            if entry.msg.max_tries > 0:
                self.logger.warning(f"Retry Sending {coa}.{point.ioa} -- COT = {COT(orig_cot)}")
                self.iec_client.send(coa, point.ioa, COT(orig_cot))
                result = {"status": "Retry", "reason": FailReason.RTUSide}
            else:
                result = {"status": ConfirmationStatus.FAIL, "reason": FailReason.RTUSide}
        new_msg = Confirmation(result, entry.msg.reference_nr, entry.msg.max_tries)
        self.publishing_server.send_msg(new_msg)

    def start(self) -> None:
        super().start()

    def run(self):
        """
        Periodically checks for new periodic updates and build queue, msgs from that.
        """
        POLL_TIME = 0.2
        self.logger.info("Starting Subscription Manager")
        while not self._terminate.is_set():
            if self.received_new_periodic_updates.wait(POLL_TIME):
                time.sleep(self.periodic_update_timeout / 1000)

                self._update_periodic_cache_from_queue()

                periodic_updates = self.build_periodic_update_msgs()
                for msg in periodic_updates:
                    self.logger.debug(f"send msg {msg}", context=log_contexts.SUB_MGR_OUT)
                    self.publishing_server.send_msg(msg)
                self.received_new_periodic_updates.clear()

    def _update_periodic_cache_from_queue(self):
        """
        Attempts to get qsize new periodic-cache items and parses them to the cache

        Returns:
            None
        """
        # to ensure updating the cache is non-blocking for the sending process
        for _ in range(self.periodic_queue.qsize()):
            try:
                p = self.periodic_queue.get(False)
            except queue.Empty:
                break

            if p['coa'] in self.periodic_cache:
                self.periodic_cache[p['coa']].append(p)
            else:
                self.periodic_cache[p['coa']] = [p]

    def build_periodic_update_msgs(self) -> List[PeriodicUpdate]:
        """
        Builds periodic update msg from cache and clears it afterwards
        Only applied if caching is allowed

        Returns:
            list of updates
        """
        msgs = []
        for rtu_coa, points in self.periodic_cache.items():
            val_maps = {}
            ts_maps = {}
            for p in points:
                if p['type'] not in val_maps:
                    val_maps[p['type']] = {p['ioa']: p['value']}
                    ts_maps[p['type']] = {p['ioa']: p['reported_at_ms']}
                else:
                    val_maps[p['type']][p['ioa']] = p['value']
                    ts_maps[p['type']][p['ioa']] = p['reported_at_ms']

            for type_ID, val_map in val_maps.items():
                ts_map = ts_maps[type_ID]
                msg = PeriodicUpdate(
                    rtu_coa, val_map, ts_map, type_ID, self._iec_handler.next_mtu_reference_number
                )
                msgs.append(msg)
        self.periodic_cache.clear()
        return msgs

    def stop(self):
        self._terminate.set()

    def get_RTU_status(self) -> Dict[int, Tuple[str, int, ConnectionState]]:
        return self._mtu.get_RTU_status()

    def get_MTU_cache(self):
        return self._mtu.get_cache()

    def get_MTU_datapoints(self) -> dict:
        return self._mtu.cast_datapoints()

    @property
    def iec_client(self) -> IECClientInterface:
        # necessary to handle new commands
        return self._mtu.iec_client

    @property
    def combine_periodic_IOs(self) -> bool:
        return self.subscription_policy.combine_periodic_IOs
