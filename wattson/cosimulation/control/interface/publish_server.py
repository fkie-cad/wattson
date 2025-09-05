import copy
import queue
import threading
import traceback
from typing import TYPE_CHECKING, Optional, List

import pyprctl
import zmq
from wattson.cosimulation.control.constants import WATTSON_BROADCAST_TOPIC
from wattson.cosimulation.control.interface.notification_export_thread import NotificationExportThread

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.networking.namespaces.namespace import Namespace

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_server import WattsonServer


class PublishServer(threading.Thread):
    """Handles broadcast messages issued by the WattsonServer"""
    def __init__(self,
                 simulation_control_server: 'WattsonServer',
                 socket_string: str,
                 namespace: Optional[Namespace] = None,
                 **kwargs):
        """
        

        Args:
            simulation_control_server ('WattsonServer'):
                The associated WattsonServer instance
            socket_string (str):
                The socket's connection string, e.g. tcp://IP:PORT
            namespace (Optional[Namespace], optional):
                
                (Default value = None)
            **kwargs:
                
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
        self._enable_history = kwargs.get("enable_history", False)
        self._export_notifications = kwargs.get("export_notifications", [])
        self._export_folder = kwargs.get("export_folder", None)
        self._export_thread = NotificationExportThread(self._export_folder, self._export_notifications)
        self._publishing_history: List[WattsonNotification] = []
        self._ready_event = threading.Event()

    def start(self) -> None:
        self._termination_requested.clear()
        super().start()
        self._export_thread.start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        self._export_thread.stop(timeout=timeout)
        try:
            self.join(timeout=timeout)
        except RuntimeError:
            pass

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
        pyprctl.set_name("W/Pub")
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.PUB) as socket:
                self.logger.info(f"Binding to {self._socket_string}")
                socket.bind(self._socket_string)
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    try:
                        notification: WattsonNotification = self._send_queue.get(block=True, timeout=self._queue_timeout)
                    except queue.Empty:
                        continue
                    try:
                        # Derive topic
                        if len(notification.recipients) == 1 and notification.recipients[0] != "*":
                            # Unicast Topic
                            zmq_topic = str(notification.recipients[0])
                        else:
                            zmq_topic = str(WATTSON_BROADCAST_TOPIC)
                        socket.send_string(zmq_topic, zmq.SNDMORE)
                        socket.send_pyobj(notification)  #, zmq.NOBLOCK)
                        self._check_append_history(notification)
                        self._check_export_notification(notification)
                    except Exception as e:
                        self.logger.error(f"{e=}")
                        self.logger.error(f"Could not sent: {notification.notification_topic} // {notification.notification_data}")
                        self.logger.error(traceback.format_exc())

    def notify(self, simulation_notification: WattsonNotification):
        """
        Sends the given notification as it is. Only if no recipients are given, the notification is actively broadcasted.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send.
        """
        if len(simulation_notification.recipients) == 0:
            simulation_notification.recipients = ["*"]
        self._send_queue.put(simulation_notification)

    def broadcast(self, simulation_notification: WattsonNotification):
        """
        Sends the given notification to all connected clients.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send.
        """
        simulation_notification.recipients = ["*"]
        self._send_queue.put(simulation_notification)

    def multicast(self, simulation_notification: WattsonNotification, recipients: List[str]):
        """
        Sends the given notification to all connected clients, but sets the message's recipient list to contain the given peer IDs such that
        other peers ignore this message.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send.
            recipients (List[str]):
                A list of peer IDs to send the notification to
        """
        simulation_notification.recipients = recipients
        self._send_queue.put(simulation_notification)

    def unicast(self, simulation_notification: WattsonNotification, recipient: str):
        """
        Sends the given notification to all connected clients, but sets the message's recipient list to the single given peer ID such that other
        peers ignore this message.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send.
            recipient (str):
                The peer ID of the client to send the notification to
        """
        simulation_notification.recipients = [recipient]
        self._send_queue.put(simulation_notification)

    def _check_append_history(self, notification: WattsonNotification):
        if self._enable_history is False:
            return

        if isinstance(self._enable_history, list):
            if notification.notification_topic not in self._enable_history:
                return
        elif self._enable_history is not True:
            return

        with self._lock:
            self._publishing_history.append(notification)

    def _check_export_notification(self, notification: WattsonNotification):
        self._export_thread.queue(notification)


