import abc
from typing import Optional

from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode


class NetworkSwitch(NetworkNode, abc.ABC):
    def reset_flows(self):
        pass

    def enable_rstp(self):
        """Enable participation in rapid spanning tree protocol"""
        pass

    def disable_rstp(self):
        """Disable participation in rapid spanning tree protocol"""
        pass

    def clear_mirrors(self):
        """Clear any existing mirror interfaces"""
        pass

    @property
    def is_manageable(self) -> bool:
        return False

    def get_switch_management_ip_address_str(self, with_subnet_length: bool = True) -> Optional[str]:
        return None

    def get_switch_management_prefix_length(self) -> Optional[int]:
        return 32
