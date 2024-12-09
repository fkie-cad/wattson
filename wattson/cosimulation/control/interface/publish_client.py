import logging
import threading
import queue
from typing import Optional, Callable

import zmq
import time

from wattson.cosimulation.control.interface.async_resolve import AsyncResolve
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace


class PublishClient(threading.Thread):
    def __init__(self,
                 socket_string: str,
                 namespace: Optional[Namespace] = None,
                 on_receive_notification_callback: Optional[Callable[[WattsonNotification], None]] = None,
                 ):
        super().__init__(daemon=True)
        self._socket_string = socket_string
        self._namespace = namespace
        self._on_receive_notification_callback = on_receive_notification_callback
        self._termination_requested = threading.Event()
        self._worker_termination_requested = threading.Event()
        self._socket_poll_timeout = 1000
        self._queue_poll_timeout = 1
        self._logger = get_logger("PublishClient", "PublishClient")
        # self._logger.setLevel(logging.DEBUG)
        self._worker_thread: Optional[threading.Thread] = None
        self._resolve_queue = queue.Queue()
        self._direct_resolve = [WattsonNotificationTopic.ASYNC_QUERY_RESOLVE]

    def set_on_receive_notification_callback(self, callback: Callable[[WattsonNotification], None]):
        self._on_receive_notification_callback = callback

    def start(self) -> None:
        self._termination_requested.clear()
        self._worker_termination_requested.clear()
        self._worker_thread = threading.Thread(target=self._resolve_notification_thread)
        self._worker_thread.daemon = True
        self._worker_thread.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._worker_termination_requested.set()
        if self._worker_thread is not None and self._worker_thread.is_alive():
            self._logger.debug(f"Waiting for worker to terminate")
            self._worker_thread.join(timeout=10)
            if self._worker_thread.is_alive():
                self._logger.warning(f"Worker thread refuses to terminate")
        self._termination_requested.set()
        if self.is_alive():
            # self._logger.info(f"Waiting for publish client to terminate")
            self.join(timeout=timeout)
        self._logger.debug(f"Stopped")

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

                        """
                        if self._on_receive_notification_callback is not None:
                            AsyncResolve.resolve(self._on_receive_notification_callback, notification)
                        """

                        if notification.notification_topic in self._direct_resolve: 
                            try:
                                if self._on_receive_notification_callback is not None:
                                    self._on_receive_notification_callback(notification)
                            except Exception as e:
                                self._logger.error(f"Direct on_receive_notification_callback threw error: {e=}")
                        else:
                            self._resolve_queue.put(notification)

    def _resolve_notification_thread(self):
        while not self._worker_termination_requested.is_set():
            try:
                notification = self._resolve_queue.get(True, self._queue_poll_timeout)
            except queue.Empty:
                continue
            try:
                if self._on_receive_notification_callback is not None:
                    self._on_receive_notification_callback(notification)
            except Exception as e:
                self._logger.error(f"Async on_receive_notification_callback threw error: {e=}")
