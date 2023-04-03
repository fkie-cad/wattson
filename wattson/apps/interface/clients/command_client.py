import logging
import queue
import sys
import threading
import threading as th
import traceback
from typing import Optional, Union, Tuple

import zmq

from wattson.apps.interface.util.status_logger import StatusLogger
from wattson.util import get_logger, log_contexts

from wattson.apps.interface.clients.zmq_wrapper import ZMQWrapper
from wattson.apps.interface.util import messages as msgs
from wattson.apps.interface.util.constants import DEFAULT_PUB_SERVER_IP, DEFAULT_CMD_SERVER_PORT, NO_RESP, \
    STR_NO_RESP
import time
from queue import Queue


class CommandClient(th.Thread):
    """ Designed to send cmds of any application to the MTU """
    def __init__(
        self,
        mtu_ip: str = DEFAULT_PUB_SERVER_IP,
        mtu_port: int = DEFAULT_CMD_SERVER_PORT,
        **kwargs,
    ):
        """

        Args:
            mtu_ip: Server-side IP
            mtu_port: Server-side port
            **kwargs:
            - node_id: str ('NoApp')
            - subscriber_prefix: str (node_id)
            - log_name: str (CmdClient_{node_id})
        """
        super().__init__()
        self.node_id = kwargs.get('node_id', 'NoApp')
        self.subscriber_prefix = kwargs.get('subscriber_prefix', self.node_id)

        self.namespace = "command_client"

        self.status_logger = kwargs.get("status_logger", StatusLogger("command-client"))

        log_name = kwargs.get('log_name', f"CmdClient_{self.node_id}")
        active_contexts = {log_contexts.SEND_CMD}
        logger = kwargs.get('logger', None)
        if logger is not None:
            self.logger = logger.getChild(log_name)
        else:
            self.logger = get_logger(self.node_id, self.node_id, use_context_logger=True)

        self.server_address = f"tcp://{mtu_ip}:{mtu_port}"
        self._poll_time = 1
        self._max_connections = th.BoundedSemaphore(3)
        self.connected = th.Event()
        self.context = zmq.Context()
        # Time to wait for a ZMQ Reply (Not an IEC Reply!!)
        self.max_reply_time = 10

        self.tasks = Queue()
        self.worker_count = 6
        self.workers = []

        self.command_messages = Queue()
        self.read_messages = Queue()
        self._terminate = th.Event()
        self.connected = kwargs.get("connected", threading.Event())

    def start(self):
        for i in range(self.worker_count):
            worker = ZMQWrapper(
                self.context, self.tasks, sock_info=self.server_address,
                recv_timeout_s=self.max_reply_time,
                status_logger=self.status_logger
            )
            worker.start()
            self.workers.append(worker)
        super().start()

    def run(self):
        """
        Performs initial handshake to receive unique subscriber-ID and then
        periodically checks for new cmds forwarded to the workers.
        """
        self.logger.info(f"Started command client for {self.server_address}")
        self.subscriber_prefix = self._perform_handshake(self.subscriber_prefix)
        while not self._terminate.is_set():
            try:
                msg = self.command_messages.get(timeout=self._poll_time)
                # self.logger.debug("got a cmd msg in the queue: " + str(msg))
            except queue.Empty:
                continue
            if not self.connected:
                self.logger.info(f"Attempting Reconnect...")
                self.subscriber_prefix = self._perform_handshake(self.subscriber_prefix)
            self._handle_next_cmd(msg)

        for i, worker in enumerate(self.workers):
            worker.stop()

    def _handle_next_cmd(self, msg: Union[msgs.IECMsg, msgs.SubscriptionInitMsg]):
        """
        Sends out cmd, queueing its reply

        Args:
            msg: msg to be send

        Returns:
            None
        """
        try:
            reply = self._send_command(msg)
            if isinstance(reply, (msgs.IECMsg, msgs.SubscriptionInitReply)):
                self.read_messages.put(reply)
        except Exception as e:
            self.status_logger.log(f"Exception while sending a cmd msg: {msg}")
            self.status_logger.increase_counter("errors", step=1)
            self.logger.error(f"Exception while sending a cmd msg: {msg}")
            self.logger.exception(e)

    def stop(self):
        self._terminate.set()

    def send_msg(self, msg: msgs.IECMsg) -> Optional[msgs.IECMsg]:
        """
        Public handler to directly send a msg, circumventing the queue

        Args:
            msg: msg to be send

        Returns:
            None if no valid-formatted reply, else the resp. reply
        """
        res = self._send_command(msg)
        if res not in (NO_RESP, STR_NO_RESP):
            return res
        return None

    def _perform_handshake(self, subscriber_type: str) -> str:
        """
        Executes handshake necessary to find unique subscriber ID

        Args:
            subscriber_type: prefix to the ID that identifies the type of application

        Returns:
            Unique Subscription ID to be used as prefix in every new msg from this client
        """
        init_msg = msgs.SubscriptionInitMsg(subscriber_type).to_json()
        reply = self._send_command(init_msg)
        if reply == STR_NO_RESP:
            self.logger.error("Did not receive subscriber ID from server")
            self.status_logger.set_connection_status("handshake", False)
            self.status_logger.increase_counter("error", step=1)
            self.status_logger.error("Command client handshake failed!")
            return subscriber_type
        self.connected.set()
        self.status_logger.set_connection_status("handshake", True)
        self.status_logger.info(f"Command client handshake successful with {reply.subscriber_ID}")
        return reply.subscriber_ID

    def _send_command(
        self, msg: Union[msgs.IECMsg, msgs.SubscriptionInitMsg, str]
    ) -> Union[msgs.IECMsg, msgs.SubscriptionInitReply, str]:
        """
        Builds worker-task and waits until the response is rcvd

        Args:
            msg: msg to be send

        Returns:
            parsed response in msg-format/ STR_NO_RESP constant
        """
        self.logger.warning(f'to send {msg=}')
        event, send_start, task = self._build_task_from_msg(msg)
        self.tasks.put(task)
        response = task.get('reply', STR_NO_RESP)

        if event.wait(self.max_reply_time):
            response = self._handle_task_reply(send_start, task)
        else:
            self.status_logger.increase_counter("error", step=1)
            self.status_logger.error(f"Event Timeout for task {task}")
            self.logger.critical(f"Event Timeout for task {task}!") #, exc_info=True, stack_info=True)

        return response

    def _handle_task_reply(self, send_start: float, task: dict) -> Union[str, msgs.IECMsg]:
        """
        Given finished task, extracts and forwards MTU-response

        Args:
            send_start: ts of task construction (for debugging)
            task: task

        Returns:
            parsed response in msg-format/ STR_NO_RESP constant
        """
        reply = task["reply"]
        response = STR_NO_RESP
        if reply not in (NO_RESP, STR_NO_RESP):
            try:
                response = msgs.from_dict(reply)
            except Exception as e:
                traceback.print_exc(file=sys.stderr)
                print(e)
            send_time = time.time() - send_start
        return response

    def _build_task_from_msg(self, msg: msgs.IECMsg) -> Tuple[th.Event, float, dict]:
        """
        Build worker-task from msg

        Args:
            msg: to be send

        Returns:
            on_reply event of task, ts of creation, Blocking worker task as dict
        """
        if isinstance(msg, msgs.IECMsg):
            json_msg = msg.to_json()
        else:
            json_msg = msg
        self.logger.debug(f"{json_msg} to server at {self.server_address}")
        send_start = time.time()
        event = th.Event()
        task = {
            "json": json_msg,
            "block": True,
            "reply": STR_NO_RESP,
            "on_reply": event
        }
        return event, send_start, task

