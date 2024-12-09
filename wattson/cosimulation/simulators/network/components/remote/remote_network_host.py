from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType


class RemoteNetworkHost(RemoteNetworkNode, NetworkHost):
    def loopback_up(self) -> bool:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "action": "loopback_up",
                "entity_id": self.entity_id
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Could not bring loopback interface up: {error=}")
            return False
        return True

    def update_default_route(self) -> bool:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "action": "update_default_route",
                "entity_id": self.entity_id
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Could not update default route: {error=}")
            return False
        return True

    def get_routes_list(self) -> list:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "action": "get_routes_list",
                "entity_id": self.entity_id
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Could not get routes: {error=}")
            return []
        return response.data.get("routes", [])
