import dataclasses
from typing import ClassVar, Optional

from wattson.cosimulation.simulators.network.components.interface.network_router import NetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.services.configuration import ServiceConfiguration
from wattson.services.routing.wattson_fr_routing_multi_service import WattsonFrRoutingMultiService


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkRouter(WattsonNetworkHost, NetworkRouter):
    """
    A network router that does not rely on IP mininet
    """
    class_id: ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()

    def get_prefix(self):
        return "r"

    def get_routing_service(self) -> Optional[WattsonFrRoutingMultiService]:
        for service in self.get_services().values():
            if isinstance(service, WattsonFrRoutingMultiService):
                return service
        return None

    def start(self):
        super().start()
        self.exec("systcl -w net.ipv4.ip_forward=1")
        self.exec("systcl -w net.ipv6.conf.all.forwarding=1")
        # Search if routing service exists
        service = self.get_routing_service()
        if service is None:
            service = WattsonFrRoutingMultiService(service_configuration=ServiceConfiguration(),
                                                   network_node=self)
            self.add_service(service=service)

    def stop(self):
        super().stop()
        service = self.get_routing_service()
        if service is not None:
            service.stop()

    def update_default_route(self) -> bool:
        return False

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__,
        })
        return d
