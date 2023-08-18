import abc

from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost


class NetworkNAT(NetworkHost, abc.ABC):
    @staticmethod
    def get_prefix():
        return "h"

    @abc.abstractmethod
    def allow_all_traffic(self):
        ...

    @abc.abstractmethod
    def allow_traffic_from_host(self, host: NetworkHost):
        ...

    @abc.abstractmethod
    def block_traffic_from_host(self, host: NetworkHost):
        ...

    @abc.abstractmethod
    def block_all_traffic(self):
        ...
