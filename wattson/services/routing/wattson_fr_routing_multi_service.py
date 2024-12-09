from typing import List, TYPE_CHECKING

from wattson.services.routing.wattson_ospf_service import WattsonOSPFService
from wattson.services.routing.wattson_zebra_service import WattsonZebraService
from wattson.services.wattson_multi_service import WattsonMultiService
from wattson.services.configuration import ServiceConfiguration

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonFrRoutingMultiService(WattsonMultiService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        services = [
            WattsonZebraService(ServiceConfiguration(), network_node=network_node),
            WattsonOSPFService(ServiceConfiguration(), network_node=network_node)
        ]
        service_configuration["max_wait"] = 1
        super().__init__(service_configuration, network_node, services)
