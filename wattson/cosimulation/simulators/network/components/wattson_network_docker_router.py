import dataclasses
from typing import ClassVar, Optional

from wattson.cosimulation.simulators.network.components.interface.network_router import NetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
from wattson.services.configuration import ServiceConfiguration
from wattson.services.routing.wattson_fr_routing_multi_service import WattsonFrRoutingMultiService


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkDockerRouter(WattsonNetworkRouter, WattsonNetworkDockerHost):
    """A network router that does not rely on IP mininet"""

    @classmethod
    def get_class_id(cls):
        # Share value with default hosts
        return WattsonNetworkRouter.get_class_id()

    @classmethod
    def set_class_id(cls, class_id: int):
        # Share value with default hosts
        WattsonNetworkRouter.set_class_id(class_id)

    def __post_init__(self):
        super().__post_init__()
        self.config.setdefault("capabilities", []).extend(["NET_BIND_SERVICE", "NET_RAW", "SYS_ADMIN"])
        self.config["privileged"] = True

    def get_prefix(self):
        return "r"
