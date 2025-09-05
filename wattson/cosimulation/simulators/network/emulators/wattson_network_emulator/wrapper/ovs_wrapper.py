import threading
import traceback
import typing
from inspect import trace

from numba.cuda.cudadrv.drvapi import cu_stream
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace
from wattson.util import get_logger


class OvsWrapper(NodeWrapper):
    _batch_enabled: bool = False
    _batch_commands: list = []
    _batch_actions: typing.List[typing.Callable] = []
    _batch_namespace: Namespace = None
    _ovs_lock: threading.RLock = threading.RLock()

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

        flat_command = " ".join(batch_command)

        # get_logger("OvsWrapper").info(f"Batch command length: {len(flat_command)}")

        OvsWrapper._batch_commands = []
        code0, lines = OvsWrapper._batch_namespace.exec(batch_command)
        if not code0:
            get_logger("OvsWrapper").error("\n".join(lines))

        for action in OvsWrapper._batch_actions:
            try:
                action_success = action()
                if not action_success:
                    code0 = False
                    get_logger("OvsWrapper").error(f"Could not execute batch action {action.__name__}")
            except Exception as e:
                code0 = False
                get_logger("OvsWrapper").error(f"Exception in batch action {action.__name__}")
                get_logger("OvsWrapper").error(traceback.format_exc())
        OvsWrapper._batch_actions = []

        return code0, lines

    def get_namespace(self) -> Namespace:
        return self.emulator.get_main_namespace()

    def set_batch_namespace(self):
        if OvsWrapper._batch_namespace is None:
            OvsWrapper._batch_namespace = self.get_namespace()

    @property
    def switch(self) -> WattsonNetworkSwitch:
        return typing.cast(WattsonNetworkSwitch, self.entity)

    def batch_create(self):
        self.set_batch_namespace()
        OvsWrapper.enable_batch()
        OvsWrapper._batch_commands.append(["ovs_vsctl", "add-br", self.switch.system_id])
        OvsWrapper._batch_actions.append(self._move_to_namespace)
        return True

    def create(self) -> bool:
        if OvsWrapper._batch_enabled:
            return self.batch_create()

        # Create OVS bridge
        with self._ovs_lock:
            code0, lines = self.emulator.get_main_namespace().exec(["ovs-vsctl", "add-br", self.switch.system_id])
            if not code0:
                self.logger.error("Failed to created OVS bridge")
                self.logger.error("\n".join(lines))
                return False
            # Move to namespace
            code0, lines = self._move_to_namespace(full_output=True)
            if not code0:
                self.logger.error("Failed to move OVS bridge")
                self.logger.info("\n".join(lines))
                return False
            return True

    def _move_to_namespace(self, full_output: bool = False):
        code0, lines = self.emulator.get_main_namespace().exec(["ip", "link", "set", self.switch.system_id, "netns", self.get_namespace().name])
        if full_output:
            return code0, lines
        elif not code0:
            self.logger.error(f"Failed to move OVS bridge {self.switch.entity_id} // {self.switch.system_id}: \n {'\n'.join(lines)}")
        return code0

    def clean(self):
        cmd = ["ovs-vsctl", "del-br", self.switch.system_id]
        if OvsWrapper._batch_enabled:
            self.set_batch_namespace()
            OvsWrapper._batch_commands.append(cmd)
        else:
            code0, lines = self.emulator.get_main_namespace().exec(cmd)
            if not code0:
                self.logger.error("Failed to clean OVS bridge")
                self.logger.info("\n".join(lines))
        self.entity.stop()

    def add_interface(self, interface: WattsonNetworkInterface):
        cmd = ["ovs-vsctl", "add-port", self.switch.system_id, interface.interface_name]
        if OvsWrapper._batch_enabled:
            self.set_batch_namespace()
            OvsWrapper._batch_commands.append(cmd)
            return
        with self._ovs_lock:
            code0, _ = self.get_namespace().exec(cmd)
            return code0

    def remove_interface(self, interface: WattsonNetworkInterface):
        if not self.switch.is_started:
            return True
        cmd = ["ovs-vsctl", "del-port", self.switch.system_id, interface.interface_name]
        if OvsWrapper._batch_enabled:
            self.set_batch_namespace()
            OvsWrapper._batch_commands.append(cmd)
            return
        with self._ovs_lock:
            code0, _ = self.get_namespace().exec(cmd)
            return code0
