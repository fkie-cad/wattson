import typing

from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace


class OvsWrapper(NodeWrapper):
    def get_namespace(self) -> Namespace:
        return self.emulator.get_main_namespace()

    @property
    def switch(self) -> WattsonNetworkSwitch:
        return typing.cast(WattsonNetworkSwitch, self.entity)

    def create(self):
        # Create OVS bridge
        code0, lines = self.emulator.get_main_namespace().exec(["ovs-vsctl", "add-br", self.switch.system_id])
        if not code0:
            self.logger.error("Failed to created OVS bridge")
            self.logger.debug("\n".join(lines))
        # Move to namespace
        code0, lines = self.emulator.get_main_namespace().exec(["ip", "link", "set", self.switch.system_id, "netns", self.get_namespace().name])
        if not code0:
            self.logger.error("Failed to move OVS bridge")
            self.logger.info("\n".join(lines))

    def clean(self):
        self.emulator.get_main_namespace().exec(["ovs-vsctl", "del-br", self.switch.system_id])
        self.entity.stop()

    def add_interface(self, interface: WattsonNetworkInterface):
        code0, _ = self.get_namespace().exec(["ovs-vsctl", "add-port", self.switch.system_id, interface.interface_name])
        return code0

    def remove_interface(self, interface: WattsonNetworkInterface):
        if self.switch.is_started:
            return True
        code0, _ = self.get_namespace().exec(["ovs-vsctl", "del-port", self.switch.system_id, interface.interface_name])
        return code0
