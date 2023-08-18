from typing import TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_factory import RemoteNetworkEntityFactory

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class RemoteNetworkEntityRepresentation(WattsonRemoteRepresentation):
    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> WattsonRemoteObject:
        return RemoteNetworkEntityFactory.get_remote_network_entity(wattson_client=wattson_client, remote_data_dict=self)
