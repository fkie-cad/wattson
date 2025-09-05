import logging
import threading
import queue
from typing import Optional, Callable

import zmq
import time

from wattson.cosimulation.control.constants import WATTSON_BROADCAST_TOPIC
from wattson.cosimulation.control.interface.async_resolve import AsyncResolve
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace


class PublishClient(threading.Thread):
    """
    Represents a thread-based client for receiving and processing notifications from the Wattson CoSimulationController.
    
    .. important::
    
        This class is usually only used by the WattsonClient and should not be instantiated directly.

    """
    def __init__(self,
                 socket_string: str,
                 namespace: Optional[Namespace] = None,
                 on_receive_notification_callback: Optional[Callable[[WattsonNotification], None]] = None,
                 ):
        """
        Initializes a new instance of the PublishClient.

        Args:
            socket_string (str):
                The full socket string to use for the connection (e.g., tcp://127.0.0.1:5555).
            namespace (Optional[Namespace], optional):
                Optionally, a Namespace instance to use for communicating from outside the simulation.
                (Default value = None)
            on_receive_notification_callback (Optional[Callable[[WattsonNotification], None]], optional):
                The callback to be called for each received notification.
                (Default value = None)
        """
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
        self._socket = None
        self._socket_ready_event = threading.Event()

    def set_on_receive_notification_callback(self, callback: Callable[[WattsonNotification], None]):
        """
        Sets the callback to be called for each received notification.

        Args:
            callback (Callable[[WattsonNotification], None]):
                The callback to be called for each received notification.
                The received notification is passed to the callback as an argument.
        """
        self._on_receive_notification_callback = callback

    def start(self) -> None:
        """
        Starts the client and connects to the CoSimulationController.

        """
        self._termination_requested.clear()
        self._worker_termination_requested.clear()
        self._worker_thread = threading.Thread(target=self._resolve_notification_thread)
        self._worker_thread.daemon = True
        self._worker_thread.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        """
        Gracefully stops the client and waits for it to terminate.

        Args:
            timeout (Optional[float], optional):
                An optional timeout in seconds to wait for termination.
                If not specified, the method will wait indefinitely.
                (Default value = None)
        """
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
        """
        Runs the publish client in a dedicated thread.
        This method handles the connection, passes notifications to the respective handlers and waits for the termination request.
        
        .. important::
        
            This method is automatically called by the start() method and may not be run directly.

        """
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.SUB) as socket:
                self._logger.info(f"Connecting to {self._socket_string}")
                self._socket = socket
                with socket.connect(self._socket_string):
                    socket.subscribe(WATTSON_BROADCAST_TOPIC)
                    while not self._termination_requested.is_set():
                        if not socket.poll(self._socket_poll_timeout):
                            continue
                        topic: str = socket.recv_string()
                        notification: WattsonNotification = socket.recv_pyobj()

                        # self._logger.info(f"ZMQ: {topic} // Wattson: {notification.notification_topic}")

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

    def set_registration(self, client_id: str):
        if self._socket is None:
            if not self._socket_ready_event.wait(timeout=5):
                self._logger.error(f"Could not set registration: socket not ready")
                return
        self._socket.subscribe(client_id)

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
