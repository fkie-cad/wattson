import pickle
from pathlib import Path

from wattson.cosimulation.simulators.network.constants import DEFAULT_SERVICE_PRIORITY
from wattson.services.service_priority import ServicePriority
from wattson.util.performance.performance_decorator import performance_assert


class ServiceConfiguration(dict):
    @property
    def service_type(self) -> str:
        return self.get("service-type", "empty")

    @property
    def name(self) -> str:
        return self.get("name", f"{self.service_type}")

    @property
    def priority(self) -> 'ServicePriority':
        """
        Returns the starting priority of this service.
        Higher priority-services should be started first
        :return: The priority
        """
        if "priority" not in self:
            self["priority"] = ServicePriority(priority_value=DEFAULT_SERVICE_PRIORITY, is_local=True)
        return self.get("priority")

    @priority.setter
    def priority(self, priority: float):
        self.set_priority(priority, is_local=True)

    def set_priority(self, priority: float, is_local: bool = True):
        """
        Sets the service's priority to the given value.
        If relative is True, the given value is interpreted as the desired difference to the
        DEFAULT_SERVICE_PRIORITY, i.e., it will be added to this value. Otherwise, the given value will be set directly.
        :param priority: The desired priority (offset)
        :param is_local: Indicator for whether the given priority should be seen as a node-specific offset or an absolute value
        :return:
        """
        priority_instance = self.priority
        if is_local:
            priority_instance.set_local(priority)
        else:
            priority_instance.set_global(priority)

    @staticmethod
    def dump(node_configuration: 'ServiceConfiguration', file: Path):
        with file.open("wb") as f:
            pickle.dump(node_configuration, f)

    @staticmethod
    def load(file: Path) -> 'ServiceConfiguration':
        with file.open("rb") as f:
            node_configuration = pickle.load(f)
        if not isinstance(node_configuration, ServiceConfiguration):
            raise ValueError("Loaded instance is no ServiceConfiguration")
        return node_configuration

    @performance_assert(0.2)
    def to_dict(self) -> dict:
        d = self.copy()
        if "priority" in d:
            d.pop("priority")
        return d
