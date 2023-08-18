import dataclasses
from wattson.cosimulation.simulators.network.components.interface.network_vm_host import NetworkVmHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkVmHost(WattsonNetworkHost, NetworkVmHost):


    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__
        })
        return d
