import abc
from typing import Dict, Any


class Noise(abc.ABC):
    @staticmethod
    def clamp(value, min_value, max_value):
        return max(min_value, min(value, max_value))

    @abc.abstractmethod
    def apply(self, value: Any) -> Any:
        ...
