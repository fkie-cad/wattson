import queue
import threading
from typing import Optional, Callable, List, Any

import zmq

from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification
from wattson.util import get_logger


class AppGatewayNotificationServer(threading.Thread):
    def __init__(self, socket_string: str):
        super().__init__()
        self._termination_requested = threading.Event()
        self._socket_string = socket_string
        self.logger = get_logger("AppGateway.NotificationServer")
        self._send_queue = queue.Queue()
        self._queue_timeout = 1
        self._lock = threading.Lock()
        self._ready_event: threading.Event = threading.Event()
        self._additional_callbacks: List[Callable[[AppGatewayNotification], Any]] = []

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
        with zmq.Context() as context:
            with context.socket(zmq.PUB) as socket:
                self.logger.info(f"Binding to {self._socket_string}")
                try:
                    socket.bind(self._socket_string)
                except zmq.error.ZMQError as e:
                    self.logger.error("Could not bind to socket")
                    raise e
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    try:
                        notification: AppGatewayNotification = self._send_queue.get(block=True, timeout=self._queue_timeout)
                    except queue.Empty:
                        continue
                    socket.send_pyobj(notification)

    def wait_until_ready(self):
        self._ready_event.wait()

    def send_notification(self, notification: AppGatewayNotification):
        self._send_queue.put(notification)
        for additional_callback in self._additional_callbacks:
            try:
                additional_callback(notification)
            except Exception as e:
                self.logger.error(f"Failed to trigger additional notification callback: {e}")

    def notify(self, message_type: AppGatewayMessageType, data: dict):
        self.send_notification(AppGatewayNotification(notification_type=message_type, notification_data=data))
