import abc

from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode


class NetworkSwitch(NetworkNode, abc.ABC):
    def reset_flows(self):
        pass

    def enable_rstp(self):
        """
        Enable participation in rapid spanning tree protocol
        @return:
        """
        pass

    def disable_rstp(self):
        """
        Disable participation in rapid spanning tree protocol
        @return:
        """
        pass

    def clear_mirrors(self):
        """
        Clear any existing mirror interfaces
        @return:
        """
        pass
