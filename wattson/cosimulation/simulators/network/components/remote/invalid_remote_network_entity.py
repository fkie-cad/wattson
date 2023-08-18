from typing import TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class InvalidRemoteNetworkEntity(RemoteNetworkEntity):
    def __init__(self, entity_id: str, wattson_client: 'WattsonClient'):
        super().__init__(entity_id, wattson_client, False)
