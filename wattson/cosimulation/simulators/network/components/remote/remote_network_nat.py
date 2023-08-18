from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost
from wattson.cosimulation.simulators.network.components.interface.network_nat import NetworkNAT
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost


class RemoteNetworkNAT(RemoteNetworkHost, NetworkNAT):
    def allow_all_traffic(self):
        pass

    def allow_traffic_from_host(self, host: NetworkHost):
        pass

    def block_traffic_from_host(self, host: NetworkHost):
        pass

    def block_all_traffic(self):
        pass
