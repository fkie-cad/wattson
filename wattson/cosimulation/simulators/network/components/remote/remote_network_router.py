from wattson.cosimulation.simulators.network.components.interface.network_router import NetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost


class RemoteNetworkRouter(RemoteNetworkHost, NetworkRouter):
    pass
