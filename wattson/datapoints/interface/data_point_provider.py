from abc import ABC, abstractmethod
from typing import Union, Callable, Optional, TYPE_CHECKING, Any, List

from wattson.datapoints.interface import DataPointValue
if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class DataPointProvider(ABC):
    """
    Provider that implement this interface serve the purpose of reading and writing values from and to
    a specific backend.
    """
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        self.config = provider_configuration
        self.data_points = points
        self.manager = manager
        self.callbacks: List[Optional[Callable[[str, DataPointValue, str, str], None]]] = []
        self.filter_ids = set()

    def set_on_change(self, callback: Optional[Callable[[str, DataPointValue, str, str], None]]):
        self.callbacks = [callback]

    def add_on_change(self, callback: Optional[Callable[[str, DataPointValue, str, str], None]]):
        self.callbacks.append(callback)

    def add_filter_ids(self, ids):
        self.filter_ids.update(ids)

    def get_sim_start_time(self) -> Optional[float]:
        return None

    @abstractmethod
    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        pass

    @abstractmethod
    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass
