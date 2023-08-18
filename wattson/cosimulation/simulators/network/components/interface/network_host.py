import abc

from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode


class NetworkHost(NetworkNode, abc.ABC):
    @staticmethod
    def get_prefix():
        return "h"

    @abc.abstractmethod
    def loopback_up(self) -> bool:
        ...

    @abc.abstractmethod
    def update_default_route(self) -> bool:
        ...
