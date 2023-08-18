import time
from typing import List, Optional


class WattsonNotification:
    def __init__(self, notification_topic: Optional[str] = None, notification_data: Optional[dict] = None):
        self.recipients: List[str] = []
        self.notification_topic = notification_topic
        self.notification_data = notification_data
        self._timestamp = time.time()

    @property
    def timestamp(self) -> float:
        return self._timestamp

    def to_dict(self) -> dict:
        return {
            "recipients": self.recipients,
            "notification_topic": self.notification_topic,
            "notification_data": self.notification_data,
            "timestamp": self.timestamp
        }

    @staticmethod
    def from_dict(d: dict) -> 'WattsonNotification':
        n = WattsonNotification(
            notification_topic=d.get("notification_topic"),
            notification_data=d.get("notification_data"),
        )
        n.recipients = d.get("recipients", [])
        n._timestamp = d.get("timestamp")
        return n
