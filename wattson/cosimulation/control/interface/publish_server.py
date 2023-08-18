import copy
import queue
import threading
from typing import TYPE_CHECKING, Optional, List

import zmq

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.networking.namespaces.namespace import Namespace

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_server import WattsonServer


class PublishServer(threading.Thread):
    """
    Handles broadcast messages issued by the WattsonServer
    """
    def __init__(self,
                 simulation_control_server: 'WattsonServer',
                 socket_string: str,
                 namespace: Optional[Namespace] = None):
        """

        :param simulation_control_server: The associated WattsonServer instance
        :param socket_string: The socket's connection string, e.g. tcp://IP:PORT
        """
        super().__init__()
        self._namespace = namespace
        self._simulation_control_server = simulation_control_server
        self._termination_requested = threading.Event()
        self._socket_string = socket_string
        self.logger = self._simulation_control_server.logger.getChild("PublishServer")
        self._send_queue = queue.Queue()
        self._queue_timeout = 1
        self._lock = threading.Lock()
        self._publishing_history: List[WattsonNotification] = []
        self._ready_event = threading.Event()

    def start(self) -> None:
        self._termination_requested.clear()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        self.join(timeout=timeout)

    def wait_until_ready(self):
        self._ready_event.wait()

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def get_history(self, topic: Optional[str] = None):
        with self._lock:
            history = copy.copy(self._publishing_history)
        if topic is None:
            return history
        return [notification for notification in self._publishing_history if notification.notification_topic == topic]

    def run(self) -> None:
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.PUB) as socket:
                self.logger.info(f"Binding to {self._socket_string}")
                socket.bind(self._socket_string)
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    try:
                        notification = self._send_queue.get(block=True, timeout=self._queue_timeout)
                    except queue.Empty:
                        continue
                    socket.send_pyobj(notification)
                    with self._lock:
                        self._publishing_history.append(notification)

    def notify(self, simulation_notification: WattsonNotification):
        """
        Sends the given notification as it is. Only if no recipients are given, the notification is actively broadcasted.
        @param simulation_notification: The notification to send.
        @return:
        """
        if len(simulation_notification.recipients) == 0:
            simulation_notification.recipients = ["*"]
        self._send_queue.put(simulation_notification)

    def broadcast(self, simulation_notification: WattsonNotification):
        """
        Sends the given notification to all connected clients.
        :param simulation_notification: The notification to send.
        :return:
        """
        simulation_notification.recipients = ["*"]
        self._send_queue.put(simulation_notification)

    def multicast(self, simulation_notification: WattsonNotification, recipients: List[str]):
        """
        Sends the given notification to all connected clients, but sets the message's recipient list to
        contain the given peer IDs such that other peers ignore this message.
        :param simulation_notification: The notification to send.
        :param recipients: A list of peer IDs to send the notification to
        :return:
        """
        simulation_notification.recipients = recipients
        self._send_queue.put(simulation_notification)

    def unicast(self, simulation_notification: WattsonNotification, recipient: str):
        """
        Sends the given notification to all connected clients, but sets the message's recipient list to
        the single given peer ID such that other peers ignore this message.
        :param simulation_notification: The notification to send.
        :param recipient: The peer ID of the client to send the notification to
        :return:
        """
        simulation_notification.recipients = [recipient]
        self._send_queue.put(simulation_notification)
