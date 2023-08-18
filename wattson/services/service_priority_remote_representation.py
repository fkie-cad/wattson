from typing import TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient
    from wattson.services.service_priority import ServicePriority


class ServicePriorityRemoteRepresentation(WattsonRemoteRepresentation):
    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> 'ServicePriority':
        from wattson.services.service_priority import ServicePriority
        return ServicePriority.from_remote_representation(self)
