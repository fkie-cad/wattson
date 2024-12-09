import dataclasses
import time
from typing import Any, Optional

from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType


class AppGatewayNotification:
    def __init__(self, notification_type: AppGatewayMessageType, notification_data: Any = None, timestamp: Optional[float] = None):
        self.notification_type = notification_type
        self.notification_data = notification_data
        if self.notification_data is None:
            self.notification_data = {}
        self.timestamp = timestamp if timestamp is not None else time.time()

    def to_dict(self) -> dict:
        return {
            'type': self.notification_type.name,
            'data': self.notification_data,
            'timestamp': self.timestamp
        }
