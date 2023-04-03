import datetime
import time
from typing import Callable, Union


class VirtualTime:
    instance: 'VirtualTime' = None

    def __init__(self):
        self._time_function = time.time
        self._step = 0
        self._step_size = 1
        self._ref_timestamp = time.time()
        self._mapped_timestamp = None
        self._speed = 1
        self._start_time = time.time()
        self._internal_time = time.time

    def set_static(self, timestamp: float):
        self._time_function = lambda: timestamp
        return self

    def set_step(self, start_timestamp: Union[datetime.datetime, float], step_size_s: float = 1.0):
        self._step = 0
        self._step_size = step_size_s
        self._ref_timestamp = start_timestamp if type(start_timestamp) == float else start_timestamp.timestamp()
        self._time_function = self._step_time
        return self

    def step(self):
        self._step += 1

    def set_scaled(self, real_timestamp: float, mapped_timestamp: float, speed: float = 1):
        self._ref_timestamp = real_timestamp
        self._mapped_timestamp = mapped_timestamp
        self._speed = speed
        self._start_time = time.time()
        self._time_function = self._scaled_time
        return self

    def set_internal_time(self, time_function: Callable[[], float]):
        self._internal_time = time_function
        return self

    def get_internal_time(self) -> float:
        return self._internal_time()

    def set_base_time(self, timestamp: float):
        self._start_time = timestamp
        return self

    def _step_time(self):
        return self._ref_timestamp + (self._step * self._step_size)

    def _scaled_time(self):
        passed = time.time() - self._start_time
        passed_virtual = passed * self._speed
        virtual = self._mapped_timestamp + passed_virtual
        return virtual

    def time(self) -> float:
        return self._time_function()

    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.time(), tz=datetime.timezone.utc)

    @staticmethod
    def get_instance():
        if VirtualTime.instance is None:
            VirtualTime.instance = VirtualTime()
        return VirtualTime.instance
