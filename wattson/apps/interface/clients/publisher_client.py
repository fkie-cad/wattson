import queue
import threading as th
from typing import Optional
import time

import zmq

from wattson.apps.interface.util.status_logger import StatusLogger
from wattson.util import get_logger
from wattson.apps.interface.util import messages as msgs
from wattson.apps.interface.util.constants import DEFAULT_PUB_SERVER_IP, DEFAULT_PUB_SERVER_PORT


class PublisherClient(th.Thread):
    """
    This class sends queries to the MTU from a different application
    """

    def __init__(
        self,
        mtu_ip: str = DEFAULT_PUB_SERVER_IP,
        mtu_port: int = DEFAULT_PUB_SERVER_PORT,
        **kwargs,
    ):
        super().__init__()
        self.node_id = kwargs.get('node_id', 'NoApp')

        log_name = kwargs.get('log_name', f"Pub_Client_{self.node_id}")
        logger = kwargs.get('logger', None)

        self.status_logger = kwargs.get("status_logger", StatusLogger("publisher-client"))
        self.status_logger.set_connection_status("connected", False)

        if logger is not None:
            self.logger = logger.getChild(log_name)
        else:
            self.logger = get_logger(self.node_id, log_name)

        self.server_address = f"tcp://{mtu_ip}:{mtu_port}"
        self.connected = th.Event()
        self.read_messages = queue.Queue()
        self._poll_time = 1
        self.max_connection_attempts = kwargs.get('max_connection_attempts', 20)
        self._terminate = th.Event()
        self.context = zmq.Context()
        self.socket = None
        self.logger.info("init")

    def start(self) -> None:
        self.logger.info("starting")
        self.socket = self.context.socket(zmq.SUB)
        self.socket.subscribe(b'')
        for _ in range(self.max_connection_attempts):
            try:
                self.socket.connect(self.server_address)
                self.status_logger.set_connection_status("connected", True)
                self.logger.debug("connected to pub server at " + self.server_address)
                self.connected.set()
                break
            except Exception as e:
                self.logger.warning(
                    f"unable to connect to pub server at {self.server_address} error: {e}"
                )
                self.status_logger.set_connection_status("connected", False)
                time.sleep(0.5)
        if not self.connected.is_set():
            self.status_logger.error(f"Failed to connect to publisher server at {self.server_address}")
            raise RuntimeError(f"Failed to connect to pub server after "
                               f"{self.max_connection_attempts * 2}s.")

        self.status_logger.info(f"Connected to publisher server at {self.server_address}")
        super().start()

    def run(self):
        self.logger.info("Started")
        with self.socket as sock:
            while not self._terminate.is_set():
                if sock.poll(self._poll_time):
                    json_msg = sock.recv_string()
                    msg = msgs.from_json(json_msg)
                    self.read_messages.put(msg)
            self.connected.clear()

    def stop(self):
        self._terminate.set()

    @property
    def has_update(self) -> bool:
        return not self.read_messages.empty()

    def get_update(self, timeout: Optional[float] = None) -> msgs.IECMsg:
        return self.read_messages.get(block=True, timeout=timeout)
