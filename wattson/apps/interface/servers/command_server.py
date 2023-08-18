import logging
import queue
import threading as th
import time
from typing import List, Dict, Union, TYPE_CHECKING, Optional, Any
from collections import Counter

import zmq

from wattson.apps.interface.util.constants import *
from wattson.apps.interface.util import ConfirmationStatus
from wattson.apps.interface.util.types import RECV_SEND_TYPES, ALL_MSGS
from wattson.apps.interface.util import messages as msgs

if TYPE_CHECKING:
    from wattson.apps.interface.manager import SubscriptionManager


class CommandServer(th.Thread):
    """ minimal example for receiving commands from other apps for the MTU """

    def __init__( self, sub_manager: 'SubscriptionManager', **kwargs: Any):
        """
        Args:
            sub_manager: Subscription Manager this server is designated to run for
            **kwargs:
                - ip: str (has default-constant)
                - port: int (has default-constant)
                - socket_init_tries: int (5) max attempts to bind the socket
        """
        super().__init__()
        self.manager = sub_manager
        self.logger: logging.Logger = sub_manager.logger.getChild("Command_Server")
        self.logger.setLevel(logging.INFO)
        ip = kwargs.get('ip', DEFAULT_PUB_SERVER_IP)
        port = kwargs.get('port', DEFAULT_CMD_SERVER_PORT)
        self._enable_zmq = kwargs.get('zmq', True)
        self._enable_ipc = kwargs.get('ipc', True)
        self.zmq_server_address = f"tcp://{ip}:{port}"
        self._poll_time = 1
        self._terminate = th.Event()
        self._socket_init_tries = kwargs.get('socket_init_tries', 5)

        self.read_messages = queue.Queue()
        self.queue = queue.Queue()
        self.command_queue: Dict[int, Dict[int, List[msgs.IECMsg]]] = {}
        self.command_prefixes = Counter()
        self.subscription_manager = sub_manager
        self.socket = None
        self.context = zmq.Context()

    def send_next_command(self, coa: int, ioa: int):
        """
        If a cmd for the given identifier is queued, forward it to the respective handler.

        Designed to be executed externally once prior cmd dealing with the same object
        has been finished.

        Once a new L5-proto is added, change args to Protocol + Identifier

        Args:
            coa: COA of RTU the cmd is designated for
            ioa: address of specific object the cmd is targeting

        Returns:
            None
        """
        if (
            coa in self.command_queue
            and ioa in self.command_queue[coa]
            and len(self.command_queue[coa][coa]) > 0
        ):
            next_msg = self.command_queue[coa][ioa].pop()
            self.subscription_manager.on_subscription_command(next_msg)

    def start(self) -> None:
        self.socket = self.context.socket(zmq.REP)
        for _ in range(self._socket_init_tries):
            try:
                self.socket.bind(self.zmq_server_address)
                break
            except Exception as e:
                self.logger.critical(
                    f"unable to bind cmd server to" f" {self.zmq_server_address} error: {e}"
                )
        self.logger.info(f"Bound to {self.zmq_server_address} and waiting for commands")
        super().start()

    def run(self):
        """
        Periodically pulls for new incoming cmds & subscription init msgs,
            handling them sequentially.
        For each inc msg, one outgoing should be send as long as the incoming msg
            was in a valid format
        """
        with self.socket as sock:
            while not self._terminate.is_set():
                if sock.poll(self._poll_time + 2000):
                    json_msg = sock.recv_json()
                    # self.logger.critical(f'Rcvd {json_msg=}')
                else:
                    self.logger.debug('no msg')
                    continue
                try:
                    msg = self._parse_cmd(json_msg)
                    res = self.handler(msg) if msg != JSON_INVALID else STR_NO_RESP
                    self._send_reply(msg, res)
                except (TypeError, ValueError) as e:
                    self.logger.warning("Error occurred while handling a command message:")
                    self.logger.exception(e)

    def _send_reply(self, msg: ALL_MSGS, res: RECV_SEND_TYPES):
        """
        Sends out the reply for a given inc. cmd

        Args:
            msg: inc cmd
            res: reply to be send

        Returns:
            None
        """
        if res != STR_NO_RESP:
            self.logger.debug(f"Sending: {res}")
            try:
                self.socket.send_string(res.to_json())
            except TypeError:
                self.logger.critical(f"BAD res {res} for msg {msg}")
        else:
            self.socket.send_string(res)

    def _parse_cmd(self, json_msg: str) -> Optional[ALL_MSGS]:
        """
        Attempts to parse incoming raw msg into a valid interpretable msg

        Args:
            json_msg: incoming string

        Returns:
            IECMsg/ SubInit if valid string, else JSON_INVALID = None
        """
        try:
            msg = msgs.from_json(json_msg)
            if not isinstance(msg, msgs.SubscriptionInitMsg):
                assert msg.reference_nr != UNSET_REFERENCE_NR
            self.logger.debug(f"Received msg {msg}")
        except ValueError:
            self.logger.error(
                f"Error: iec obj has invalid format\nsend msg: {json_msg}"
            )
            msg = JSON_INVALID
        return msg

    def add_cmd_to_queue(self, msg: msgs.IECMsg, coa: int, ioa: int = -1):
        """
        If subscription mgr wants to queue this msg, do so by its unique coa-ioa combination
        Queues are implemented as FIFO through lists here

        If adding new L5 protocol: redefine args as "identifier + protocol" combination

        Args:
            msg: msg to be queued
            coa: COA of RTU it's designated for
            ioa: IOA it's designated for

        Returns:
            None
        """
        if coa not in self.command_queue:
            self.command_queue[coa] = {ioa: [msg]}
        elif ioa not in self.command_queue[coa]:
            self.command_queue[coa][ioa] = [msg]
        else:
            self.command_queue[coa][ioa].append(msg)

    def handler(
        self, msg: Union[msgs.IECMsg, msgs.SubscriptionInitMsg]
    ) -> Union[msgs.IECMsg, msgs.SubscriptionInitReply]:
        """
        Main handler for any valid incoming msg

        Args:
            msg: parsed IECMsg to be executed

        Returns:
            Usually a Confirmation/ SubscriptionInitReply, always returns some IECMsg/SubInitReply
        """
        res = STR_NO_RESP
        self.logger.debug(f"Received msg {msg}")

        if isinstance(msg, msgs.IECMsg):
            res = self._handle_iec_cmd(msg)
        # add another case here if new L5-proto is implemented
        elif isinstance(msg, msgs.SubscriptionInitMsg):
            res = self._handle_subscription_init_cmd(msg)

        if res == STR_NO_RESP:
            res = msgs.Confirmation(
                {"status": ConfirmationStatus.WAITING_FOR_SEND}, msg.reference_nr, msg.max_tries
            )
        return res

    def _handle_iec_cmd(self, msg: msgs.IECMsg) -> RECV_SEND_TYPES:
        """
        Handles all IEC-msgs

        Args:
            msg: incoming IECMsg

        Returns:
            a valid reply for an IECMsg
        """
        res = None
        if isinstance(msg, (msgs.TotalInterroReq, msgs.RTUStatusReq, msgs.MtuCacheReq)):
            res = self.manager.on_subscription_command(msg)
        if isinstance(msg, (msgs.ReadDatapoint, msgs.SysInfoControl, msgs.ProcessInfoControl,
                      msgs.FileTransferReq, msgs.ParameterActivate)):

            if isinstance(msg, (msgs.ProcessInfoControl, msgs.ReadDatapoint)):
                res = self.subscription_manager.check_is_executable(msg)
                if res is not None and res.result['status'] == ConfirmationStatus.QUEUED:
                    self.add_cmd_to_queue(msg, res.result['coa'], res.result['ioa'])

            if res is None:
                res = self.subscription_manager.on_subscription_command(msg)

        res = STR_NO_RESP if res is None else res
        return res

    def _handle_subscription_init_cmd(self, msg: msgs.SubscriptionInitMsg) -> msgs.SubscriptionInitReply:
        """
        Sets up new subscriber and creates unique prefix for it.

        Args:
            msg: Incoming SubInit request

        Returns:
            reply with unique subscriber-identity to be used as prefix in new inc & outgoing msgs
        """
        if msg.subscriber_type not in self.command_prefixes:
            res = msgs.SubscriptionInitReply(msg.subscriber_type)
        else:
            new_prefix = (
                f"{msg.subscriber_type}_" f"{self.command_prefixes[msg.subscriber_type] + 1}"
            )
            res = msgs.SubscriptionInitReply(new_prefix)
        self.command_prefixes[msg.subscriber_type] += 1
        return res

    def stop(self):
        self._terminate.set()
