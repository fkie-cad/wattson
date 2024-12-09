import threading
from typing import Optional, Any, Callable, List
import zmq

from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification
from wattson.networking.namespaces.namespace import Namespace
from wattson.util import get_logger


class AppGatewayNotificationClient(threading.Thread):
    def __init__(self,
                 socket_string: str,
                 on_receive_notification: Optional[Callable[[AppGatewayNotification], Any]] = None,
                 namespace: Optional[Namespace] = None):
        super().__init__()
        self._socket_string = socket_string
        self._termination_requested = threading.Event()
        self._socket_poll_timeout = 1000
        self.logger = get_logger("AppGateway.NotificationClient")
        self._on_receive_notification_callback = on_receive_notification
        self._additional_callbacks: List[Callable[[AppGatewayNotification], Any]] = []
        self._namespace = namespace

    def start(self) -> None:
        self._termination_requested.clear()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        if self.is_alive():
            self.join(timeout=timeout)

    def add_additional_callback(self, callback: Callable[[AppGatewayNotification], Any]):
        self._additional_callbacks.append(callback)

    def remove_additional_callback(self, callback: Callable[[AppGatewayNotification], Any]):
        if callback in self._additional_callbacks:
            self._additional_callbacks.remove(callback)

    def run(self) -> None:
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.SUB) as socket:
                self.logger.info(f"Connecting to {self._socket_string}")
                with socket.connect(self._socket_string):
                    socket.subscribe(b"")
                    while not self._termination_requested.is_set():
                        if not socket.poll(self._socket_poll_timeout):
                            continue
                        notification: AppGatewayNotification = socket.recv_pyobj()
                        self.logger.debug(f"Received message {notification.notification_type}")
                        if self._on_receive_notification_callback is not None:
                            self._on_receive_notification_callback(notification)
                        for additional_callback in self._additional_callbacks:
                            try:
                                additional_callback(notification)
                            except Exception as e:
                                self.logger.error(f"Failed to trigger additional notification callback: {e}")
