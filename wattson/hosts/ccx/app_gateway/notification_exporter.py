import json
import queue
import threading
from pathlib import Path
from typing import Union

from wattson.hosts.ccx.app_gateway import AppGatewayClient
from wattson.hosts.ccx.app_gateway import AppGatewayServer
from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification
from wattson.hosts.ccx.app_gateway.notification_client import AppGatewayNotificationClient
from wattson.hosts.ccx.app_gateway.notification_server import AppGatewayNotificationServer
from wattson.util.json.pickle_encoder import PickleEncoder


class NotificationExporter(threading.Thread):
    def __init__(self, app_gateway: Union['AppGatewayClient', 'AppGatewayServer'], export_file: Path):
        super().__init__()
        self._app_gateway = app_gateway
        self.notification_handler: Union[AppGatewayNotificationClient, AppGatewayNotificationServer]
        if isinstance(app_gateway, AppGatewayClient):
            self.notification_handler = self._app_gateway._publisher
        else:
            self.notification_handler = self._app_gateway._notification_server
        self._notification_queue = queue.Queue()
        self._export_file = export_file
        self._enabled: bool = False
        self._stop_requested: threading.Event = threading.Event()

    def start(self):
        super().start()
        self._enabled = True
        self._stop_requested.clear()
        self.notification_handler.add_additional_callback(self._on_notification)

    def stop(self):
        self._stop_requested.set()
        self._enabled = False
        self.notification_handler.remove_additional_callback(self._on_notification)

    def _on_notification(self, notification: AppGatewayNotification):
        if self._enabled:
            self._notification_queue.put(notification)

    def run(self):
        with self._export_file.open('w') as export_file:
            while not self._stop_requested.is_set():
                try:
                    notification = self._notification_queue.get(timeout=1)
                except queue.Empty:
                    continue
                line = json.dumps(notification.to_dict(), cls=PickleEncoder)
                export_file.write(line + '\n')
