import abc
from typing import Optional

from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost


class NetworkVmHost(NetworkHost, abc.ABC):
    @staticmethod
    def get_prefix():
        return "h"
