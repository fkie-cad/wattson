from wattson.cosimulation.simulators.network.components.interface.network_switch import NetworkSwitch
from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode


class RemoteNetworkSwitch(RemoteNetworkNode, NetworkSwitch):
    pass
