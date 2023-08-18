from typing import Any, TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.interface.network_link import NetworkLink
from wattson.cosimulation.simulators.network.components.network_link_model import NetworkLinkModel
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface


class RemoteNetworkLink(RemoteNetworkEntity, NetworkLink):
    def get_link_model(self) -> NetworkLinkModel:
        self.synchronize()
        return self.state.get("link_model")

    def _on_link_property_change(self, link_property: str, value: Any):
        self.logger.info(f"Requesting {link_property=} set to {value=}")
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SET_LINK_PROPERTY,
            query_data={"entity_id": self.entity_id, "property_name": link_property, "property_value": value}
        )
        resp = self._wattson_client.query(query)
        if not resp.is_successful():
            error = resp.data.get("error")
            self.logger.error(f"Failed to change property {link_property} to {value}: {error=}")

    def update_from_remote_representation(self, remote_representation: RemoteNetworkEntityRepresentation) -> bool:
        success = super().update_from_remote_representation(remote_representation=remote_representation)
        self.get_link_model().set_on_change_callback(self._on_link_property_change)
        return success

    def _get_interface(self, entity_id: str):
        from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
        interface = self._wattson_client.get_remote_network_interface_by_id(entity_id=entity_id)

        if not isinstance(interface, RemoteNetworkInterface):
            return None
        return interface

    def get_interface_a(self) -> 'RemoteNetworkInterface':
        return self._get_interface(self.state.get("interface_a_id"))

    def get_interface_b(self) -> 'RemoteNetworkInterface':
        return self._get_interface(self.state.get("interface_b_id"))

    def is_up(self) -> bool:
        self.synchronize()
        return self.state.get("is_up", False)

    def up(self):
        self._set_link_state(WattsonNetworkQueryType.SET_LINK_UP, "up")

    def down(self):
        self._set_link_state(WattsonNetworkQueryType.SET_LINK_DOWN, "down")

    def get_link_state(self) -> dict:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.GET_LINK_STATE,
            query_data={"entity_id": self.entity_id}
        )
        resp = self._wattson_client.query(query)
        if not resp.is_successful():
            error = resp.data.get("error")
            return {
                "result": error
            }
        return resp.data.get("link_state", {})

    def _set_link_state(self, action: WattsonNetworkQueryType, action_name: str):
        query = WattsonNetworkQuery(
            query_type=action,
            query_data={"entity_id": self.entity_id}
        )
        resp = self._wattson_client.query(query)
        if not resp.is_successful():
            error = resp.data.get("error")
            self.logger.error(f"Failed to set link {self.entity_id} {action_name}: {error=}")
