import ipaddress
from typing import Optional, TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient
    from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
    from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode


class RemoteNetworkInterface(RemoteNetworkEntity, NetworkInterface):
    def __init__(self, entity_id: str, wattson_client: 'WattsonClient', auto_sync: bool = True):
        super().__init__(entity_id, wattson_client, auto_sync)

    def has_ip(self) -> bool:
        return self.state.get("ip_address") is not None

    def get_ip_address(self) -> Optional[ipaddress.IPv4Address]:
        return self.state.get("ip_address")

    def set_ip_address(self, ip_address: Optional[ipaddress.IPv4Address]):
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SET_INTERFACE_IP,
            query_data={
                "entity_id": self.entity_id,
                "ip_address": ip_address
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", "")
            self.logger.error(f"Could not set IP address: {error=}")
            return False
        self.state["ip_address"] = ip_address
        return True

    def get_subnet_prefix_length(self) -> Optional[int]:
        return self.state.get("subnet_prefix_length")

    def get_mac_address(self) -> Optional[str]:
        return self.state.get("mac_address")

    def up(self):
        raise NotImplementedError("NIY")

    def down(self):
        raise NotImplementedError("NIY")

    @property
    def is_management(self) -> bool:
        return self.state.get("is_management", False)

    def get_system_name(self) -> Optional[str]:
        return self.state.get("system_name")

    def get_link(self) -> Optional['RemoteNetworkLink']:
        from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
        link = self._wattson_client.get_remote_network_link(self.state.get("link_id"))
        if not isinstance(link, RemoteNetworkLink):
            return None
        return link

    def get_node(self) -> Optional['RemoteNetworkNode']:
        from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
        node = self._wattson_client.get_remote_network_node(self.state.get("node_id"))
        if not isinstance(node, RemoteNetworkNode):
            return None
        return node
