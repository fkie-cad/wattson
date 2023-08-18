import abc

from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost


class NetworkRouter(NetworkHost, abc.ABC):
    @staticmethod
    def get_prefix():
        return "r"
