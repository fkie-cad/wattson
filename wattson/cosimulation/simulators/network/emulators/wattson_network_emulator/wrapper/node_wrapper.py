import abc
from abc import ABC

from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.entity_wrapper import EntityWrapper


class NodeWrapper(EntityWrapper, ABC):
    def add_interface(self, interface: WattsonNetworkInterface):
        pass

    def remove_interface(self, interface: WattsonNetworkInterface):
        pass
