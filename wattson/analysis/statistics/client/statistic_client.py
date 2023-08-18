import datetime
import logging
import random
import string
import time
from queue import Queue, Empty
from threading import Thread, Event, Lock
from typing import Optional, Any, Union

import zmq

from wattson.analysis.statistics.common.constants import STATISTIC_SERVER_PORT
from wattson.analysis.statistics.common.statistic_message import StatisticMessage
from wattson.util import get_logger


class StatisticClient(Thread):
    id_cache: dict = {}
    global_id: int = 0
    id_lock: Lock = Lock()
    _primary_instance: Optional['StatisticClient'] = None

    def __init__(self, ip: Optional[str], **kwargs: Any):
        super().__init__()

        if StatisticClient._primary_instance is None:
            StatisticClient._primary_instance = self

        self.socket: zmq.Socket
        self.socket = None
        self.ip = ip
        self.port = kwargs.get("port", STATISTIC_SERVER_PORT)
        self._enable = kwargs.get("enable", True)
        self._enable = self._enable and ip is not None
        self.server_address = f"tcp://{self.ip}:{self.port}"

        self._logger = kwargs.get("logger")
        if self._logger is None:
            self._logger = get_logger("Wattson", "StatisticClient")
        else:
            self._logger = self._logger.getChild("StatisticClient")
        #self._logger.setLevel(kwargs.get("log_level", logging.DEBUG))

        random_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
        self.host_name = kwargs.get("host", f"C_{random_name}")
        self._queue = Queue()
        self._stop_requested = Event()

    def start(self) -> None:
        super().start()
        if self._enable:
            try:
                self._logger.info(f"Connecting to {self.server_address}...")
                self.socket = zmq.Context().socket(zmq.REQ)
                self.socket.connect(self.server_address)
            except Exception as e:
                self._logger.error(f"Could not connect to Statistic Server: {e=}")
        else:
            self._stop_requested.set()

    def stop(self):
        self._stop_requested.set()

    def run(self) -> None:
        while not self._stop_requested.is_set():
            try:
                message: StatisticMessage = self._queue.get(True, 1)
                self.socket.send_pyobj(message)
                self.socket.recv_pyobj()
            except Empty:
                continue
            except zmq.error.ZMQError as e:
                self._logger.error(f"Could not send: {e=}")

    def _get_event_id(self, event_class: str, event_name: str):
        key = (event_class, event_name)
        with StatisticClient.id_lock:
            if key in StatisticClient.id_cache:
                event_id = StatisticClient.id_cache[key]
            else:
                event_id = 0
            StatisticClient.id_cache[key] = event_id + 1
            global_id = StatisticClient.global_id
            StatisticClient.global_id += 1
        return event_id, global_id

    def log(self,
            event_name: str,
            event_class: str = "default",
            value: Optional[Any] = None,
            timestamp: Optional[Union[float, datetime.datetime]] = None,
            host: Optional[str] = None,
            data: Optional[Any] = None):

        if not self._enable:
            self._logger.debug(f"Logging is disabled")
            return
        if host is None:
            host = self.host_name
        if timestamp is None:
            timestamp = time.time()
        if type(timestamp) == datetime.datetime:
            timestamp = timestamp.timestamp()

        event_id, global_id = self._get_event_id(event_class, event_name)

        message = StatisticMessage(
            timestamp=timestamp,
            host=host,
            event_class=event_class,
            event_name=event_name,
            event_id=event_id,
            global_id=global_id,
            value=value,
            data=data
        )
        self._queue.put(message)

    @classmethod
    def get_primary_instance(cls) -> Optional['StatisticClient']:
        return StatisticClient._primary_instance
