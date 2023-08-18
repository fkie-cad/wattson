import threading
from typing import Optional, Callable

import zmq

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace


class PublishClient(threading.Thread):
    def __init__(self,
                 socket_string: str,
                 namespace: Optional[Namespace] = None,
                 on_receive_notification_callback: Optional[Callable[[WattsonNotification], None]] = None):
        super().__init__()
        self._socket_string = socket_string
        self._namespace = namespace
        self._on_receive_notification_callback = on_receive_notification_callback
        self._termination_requested = threading.Event()
        self._socket_poll_timeout = 1000
        self._logger = get_logger("PublishClient", "PublishClient")

    def set_on_receive_notification_callback(self, callback: Callable[[WattsonNotification], None]):
        self._on_receive_notification_callback = callback

    def start(self) -> None:
        self._termination_requested.clear()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        if self.is_alive():
            self.join(timeout=timeout)

    def run(self) -> None:
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.SUB) as socket:
                self._logger.info(f"Connecting to {self._socket_string}")
                with socket.connect(self._socket_string):
                    socket.subscribe(b"")
                    while not self._termination_requested.is_set():
                        if not socket.poll(self._socket_poll_timeout):
                            continue
                        notification: WattsonNotification = socket.recv_pyobj()
                        self._logger.debug(f"Received notification {notification.notification_topic}")
                        if self._on_receive_notification_callback is not None:
                            self._on_receive_notification_callback(notification)
