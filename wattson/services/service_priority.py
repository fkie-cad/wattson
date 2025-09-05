from typing import TYPE_CHECKING, Optional

from wattson.services.service_priority_remote_representation import ServicePriorityRemoteRepresentation

if TYPE_CHECKING:
    from wattson.services.wattson_service import WattsonService
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class ServicePriority:
    """
    Represents the priority of a service for determining the starting order.
    A higher value indicates a higher priority.
    The priority can be treated globally ("global") or relatively to the responsible network node ("local").

    """
    def __init__(self,
                 service: Optional['WattsonService'] = None,
                 priority_value: float = 0,
                 is_local: bool = True):
        self._service: Optional['WattsonService'] = service
        self._is_local: bool = is_local
        self._is_absolute: bool = False
        self._local_priority: float = priority_value
        self._global_priority: float = priority_value

    @staticmethod
    def from_service_priority(service: 'WattsonService', priority: 'ServicePriority') -> 'ServicePriority':
        """
        Creates a ServicePriority from a template, linking the given WattsonService.

        Args:
            service ('WattsonService'):
                The WattsonService instance
            priority ('ServicePriority'):
                The original ServicePriority

        Returns:
            'ServicePriority': A cloned ServicePriority representing the same priority, linked to the given WattsonService.
        """
        clone = ServicePriority(service=service)
        clone._is_local = priority._is_local
        clone._local_priority = priority._local_priority
        clone._global_priority = priority._global_priority
        return clone

    def get_service(self) -> 'WattsonService':
        return self._service

    def get_global(self, node: Optional['WattsonNetworkNode'] = None) -> float:
        if not self._is_local or self._is_absolute:
            return self._global_priority
        if node is None:
            node = self._service.network_node
        return node.priority + self._local_priority

    def get_local(self, node: Optional['WattsonNetworkNode'] = None) -> float:
        if self._is_local or self._is_absolute:
            return self._local_priority
        if node is None:
            node = self._service.network_node
        return self._global_priority - node.priority

    def set_global(self, priority: float):
        self._is_local = False
        self._global_priority = priority

    def set_local(self, priority: float):
        self._is_local = True
        self._local_priority = priority

    def to_remote_representation(self) -> ServicePriorityRemoteRepresentation:
        return ServicePriorityRemoteRepresentation({
            "is_local": self._is_local,
            "local": self._local_priority,
            "global": self._global_priority
        })

    @staticmethod
    def from_remote_representation(remote_representation: ServicePriorityRemoteRepresentation) -> 'ServicePriority':
        priority = ServicePriority()
        priority._local_priority = remote_representation["local"]
        priority._global_priority = remote_representation["global"]
        priority._is_local = remote_representation["is_local"]
        priority._is_absolute = True
        return priority
