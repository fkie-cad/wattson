import threading
import typing

from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace
from wattson.util import get_logger


class OvsWrapper(NodeWrapper):
    _batch_enabled: bool = False
    _batch_commands: list = []
    _batch_namespace: Namespace = None

    @staticmethod
    def enable_batch():
        OvsWrapper._batch_enabled = True

    @staticmethod
    def disable_batch():
        OvsWrapper._batch_enabled = False

    @staticmethod
    def flush_batch() -> typing.Tuple[bool, typing.List[str]]:
        if len(OvsWrapper._batch_commands) == 0:
            return True, []

        batch_command = ["ovs-vsctl"]
        for i, command in enumerate(OvsWrapper._batch_commands):
            # Remove ovs-vsctl part
            command.pop(0)
            if i > 0:
                batch_command.append("--")
            batch_command.extend(command)
        OvsWrapper._batch_commands = []
        code0, lines = OvsWrapper._batch_namespace.exec(batch_command)
        if not code0:
            get_logger("OvsWrapper").error("\n".join(lines))
        return code0, lines

    def get_namespace(self) -> Namespace:
        return self.emulator.get_main_namespace()

    @property
    def switch(self) -> WattsonNetworkSwitch:
        return typing.cast(WattsonNetworkSwitch, self.entity)

    def create(self) -> bool:
        # Create OVS bridge
        code0, lines = self.emulator.get_main_namespace().exec(["ovs-vsctl", "add-br", self.switch.system_id])
        if not code0:
            self.logger.error("Failed to created OVS bridge")
            self.logger.error("\n".join(lines))
            return False
        # Move to namespace
        code0, lines = self.emulator.get_main_namespace().exec(["ip", "link", "set", self.switch.system_id, "netns", self.get_namespace().name])
        if not code0:
            self.logger.error("Failed to move OVS bridge")
            self.logger.info("\n".join(lines))
            return False
        return True

    def clean(self):
        cmd = ["ovs-vsctl", "del-br", self.switch.system_id]
        if OvsWrapper._batch_enabled:
            if OvsWrapper._batch_namespace is None:
                OvsWrapper._batch_namespace = self.get_namespace()
            OvsWrapper._batch_commands.append(cmd)
        else:
            code0, lines = self.emulator.get_main_namespace().exec(cmd)
            if not code0:
                self.logger.error("Failed to clean OVS bridge")
                self.logger.info("\n".join(lines))
        self.entity.stop()

    def add_interface(self, interface: WattsonNetworkInterface):
        code0, _ = self.get_namespace().exec(["ovs-vsctl", "add-port", self.switch.system_id, interface.interface_name])
        return code0

    def remove_interface(self, interface: WattsonNetworkInterface):
        if not self.switch.is_started:
            return True
        code0, _ = self.get_namespace().exec(["ovs-vsctl", "del-port", self.switch.system_id, interface.interface_name])
        return code0
