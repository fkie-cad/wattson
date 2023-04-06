import datetime
import time
from typing import Any, Optional, Union


class StatisticMessage:
    def __init__(self,
                 timestamp: Optional[Union[int, float, datetime.datetime]] = None,
                 host: Optional[str] = None,
                 event_class: Optional[str] = None,
                 event_name: Optional[str] = None,
                 event_id: Optional[int] = None,
                 global_id: Optional[int] = None,
                 value: Optional[Any] = None,
                 data: Optional[Any] = None):
        if timestamp is None:
            timestamp = time.time()
        self.timestamp = timestamp
        if type(self.timestamp) == datetime.datetime:
            self.timestamp = self.timestamp.timestamp()
        self.host = host
        self.event_class = event_class
        self.event_name = event_name
        self.event_id = event_id
        self.value = value
        self.global_id = global_id
        self.data = data

    def to_dict(self):
        return {
            "time": self.timestamp,
            "host": self.host,
            "event_class": self.event_class,
            "event_name": self.event_name,
            "event_id": self.event_id,
            "global_id": self.event_id,
            "value": self.value,
            "data": self.data
        }
