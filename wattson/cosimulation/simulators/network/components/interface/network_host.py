import abc
from typing import Optional

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

    @abc.abstractmethod
    def get_routes_list(self) -> list:
        ...

    def get_default_route_dict(self) -> Optional[dict]:
        routes = self.get_routes_list()
        for route in routes:
            if route.get("destination") == "default":
                return route
        return None
