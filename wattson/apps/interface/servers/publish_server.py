import logging
import queue
import threading as th
import time
from typing import Any

import zmq

from wattson.util import ContextLogger, log_contexts
from wattson.apps.interface.util import messages as msgs
from wattson.apps.interface.util.constants import *


class PublishingServer(th.Thread):
    """ Minimal example for logging the MTUs IEC104 connections """

    def __init__(self, parent_logger: logging.Logger, **kwargs: Any):
        """
        TODO
        Args:
            parent_logger:
            **kwargs:
                - max_bind_attempts: int (10) nr of attempts to bind the socket to the expected addr
                - ip: str local IP the server should publish from (default localhost)
                - port: int local port the server should publish from
        """
        super().__init__()
        self.name = kwargs.get('name', 'Publishing_Server')
        ip = kwargs.get('ip', DEFAULT_PUB_SERVER_IP)
        port = kwargs.get('port', DEFAULT_PUB_SERVER_PORT)
        self._active_contexts = {log_contexts.APP_PUBLISH}
        self.logger = parent_logger.getChild(self.name)
        #self.logger.setLevel(logging.DEBUG)

        self.server_address = "tcp://{}:{}".format(ip, port)
        self._poll_time = 1
        self._terminate = th.Event()
        self.context = zmq.Context()
        self.socket = None
        self.max_bind_attempts = kwargs.get('max_bind_attempts', 10)

        #self.queue: "mp.Queue[str]" = mp.Queue()
        self.queue = queue.Queue()

    def start(self) -> None:
        """
        Raises:
            RuntimeError if socket could not be bound in the expected amounts of tries
        """
        self.socket = self.context.socket(zmq.PUB)
        bound = self._try_bind()
        if not bound:
            raise RuntimeError(f"Could not start Pub Server")
        super().start()

    def _try_bind(self) -> bool:
        """
        Attempts (limited) amounts of binding the socket to the expected addr

        Returns:
            True iff socket is successfully bound to expected addr
        """
        bound = False
        for _ in range(self.max_bind_attempts):
            try:
                self.socket.bind(self.server_address)
                bound = True
                break
            except Exception as e:
                self.logger.critical(f"unable to bind sub server to" f" {self.server_address} error: {e}")
            time.sleep(0.5)
        return bound

    def run(self):
        # Publisher thread
        self.logger.info("Server started!")
        with self.socket as sock:
            while not self._terminate.is_set():
                try:
                    msg = self._wait_and_log_next_msg()
                    sock.send_string(msg)
                except Exception as e:
                    self.logger.warning("Unable to publish the msg: ")
                    self.logger.exception(e)

    def _wait_and_log_next_msg(self) -> str:
        while not self._terminate.is_set():
            try:
                msg = self.queue.get(timeout=self._poll_time)
            except queue.Empty:
                continue
            orig_msg = msgs.from_json(msg)
            if not orig_msg.reference_nr.startswith('MTU'):
                self.logger.debug(f"{msg}")
            else:
                self.logger.debug(f"{msg}")
            return msg

    def send_msg(self, msg: msgs.IECMsg):
        if msg:
            j_form = msg.to_json()
            if not j_form:
                self.logger.warning(f"msg {msg} serialised to invalid {j_form}!, type: {type(msg)}")
            else:
                self.queue.put(j_form)
        else:
            self.logger.critical("Message is False")

    def stop(self):
        self._terminate.set()
