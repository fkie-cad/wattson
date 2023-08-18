import dataclasses
from typing import List, TYPE_CHECKING, ClassVar, Optional

from wattson.cosimulation.exceptions import InvalidInterfaceException
from wattson.cosimulation.simulators.network.components.interface.network_switch import NetworkSwitch
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkSwitch(WattsonNetworkNode, NetworkSwitch):
    class_id: ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()
        self.add_role("switch")

    def get_prefix(self):
        return "s"

    def on_interface_start(self, interface: 'WattsonNetworkInterface'):
        # Check if physical interface exists
        if self.is_ovs() and not interface.is_virtual():
            if self.exec(["ip", "link", "show", interface.interface_name])[0] != 0:
                # Interface does not exist: Manually add it
                codes = []
                resp = []
                code, res = self.exec(["ip", "link", "add", interface.interface_name, "type", "dummy"])
                resp.extend(res)
                codes.append(code)
                code, res = self.exec(["ip", "link", "set", "dev", interface.interface_name, "up"])
                resp.extend(res)
                codes.append(code)
                code, res = self.exec(["ovs-vsctl", "add-port", self.system_id, interface.interface_name])
                codes.append(code)
                resp.extend(res)
                if codes == [0, 0, 0]:
                    self.logger.info(f"Manually added interface {interface.interface_name} to {self.display_name}")
                else:
                    self.logger.error(f"Failed ot create interface {interface.interface_name} for {self.display_name}")
                    self.logger.error("\n".join(resp))

        self._handle_special_interfaces([interface])
        # self.reset_flows()

    def start(self):
        super().start()
        self._handle_special_interfaces()

    def stop(self):
        super().stop()

    def start_emulation_instance(self):
        if hasattr(self.emulation_instance, "start"):
            self.emulation_instance.start(self.network_emulator.get_controllers())
            if hasattr(self.emulation_instance, "batchStartup"):
                self.emulation_instance.batchStartup([self.emulation_instance])

    def stop_emulation_instance(self):
        if hasattr(self.emulation_instance, "stop"):
            if hasattr(self.emulation_instance, "shell") and self.emulation_instance.shell is None:
                # Mininet Instance already exited
                return
            self.emulation_instance.stop(False)

    def start_pcap(self, interface: Optional['WattsonNetworkInterface'] = None) -> List['WattsonService']:
        if interface is not None:
            return super().start_pcap(interface=interface)
        from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
        # PCAP at all interfaces: Use or create span / mirror port interface
        # Search for any existing mirror / span interface
        for switch_interface in self.get_interfaces():
            if switch_interface.is_mirror_port():
                mirror_port = switch_interface
                # Already existing mirror port
                return self.start_pcap(interface=mirror_port)
        # Create mirror port
        mirror_port = WattsonNetworkInterface(
            id=None,
            display_name="Mirror",
            node=self,
            config={"mirror": True}
        )
        self.logger.info(f"Creating new Mirror: {mirror_port.entity_id}")
        self.network_emulator.add_interface(node=self, interface=mirror_port)
        return self.start_pcap(interface=mirror_port)

    def is_ovs(self) -> bool:
        return True
        """
        try:
            import mininet.node
        except ImportError as e:
            return False
        return isinstance(self.emulation_instance, mininet.node.OVSSwitch)
        """

    def get_emulation_entity_config(self) -> dict:
        return {
            "rstp": self.config.get("rstp", True),
            "failMode": self.config.get("fail_mode", "standalone"),
            "stp": self.config.get("stp", False)
        }

    def enable_rstp(self):
        self.config["rstp"] = True
        if self.is_started and self.is_ovs():
            if self.emulation_instance is not None:
                self.emulation_instance.params["rstp"] = True
            code, line = self.exec(["ovs-vsctl", "set", "bridge", self.system_id, "rstp_enable=true"])
            if code != 0:
                self.logger.error("Could not enable RSTP")
                self.logger.error(f"{' '.join(line)}")
                return False
            return True

    def disable_rstp(self):
        self.config["rstp"] = False
        if self.is_started and self.is_ovs():
            if self.emulation_instance is not None:
                self.emulation_instance.params["rstp"] = False
            code, line = self.exec(["ovs-vsctl", "set", "bridge", self.system_id, "rstp_enable=false"])
            if code != 0:
                self.logger.error("Could not disable RSTP")
                self.logger.error(f"{' '.join(line)}")
                return False
            return True

    def reset_flows(self):
        if not self.is_ovs():
            return
        code, lines = self.exec(["ovs-ofctl", "dump-flows", self.system_id])
        if code != 0:
            self.logger.error("Could not read flows")
            return
        for flow in lines:
            if "actions=CONTROLLER" in flow:
                continue
            if "actions=NORMAL" in flow:
                continue
            parts = flow.split(",")
            for part in parts:
                part = part.strip()
                if "=" in part:
                    key, value = part.split("=", 1)
                    if key == "dl_src" or key == "dl_src":
                        self.exec(["ovs-ofctl", "del-flows", self.system_id, f"{key}={value}"])
                        break

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__,
        })
        return d

    def _handle_special_interfaces(self, interfaces: Optional[List['WattsonNetworkInterface']] = None):
        if not self._is_started:
            return
        if not self.is_ovs():
            return
        if interfaces is None:
            interfaces = self.interfaces
        for interface in interfaces:
            if not interface.is_started:
                continue
            if interface.is_physical():
                physical_name = interface.get_physical_name()
                # TODO: Allow other instances besides OVS Switch?
                # Add physical interface to switch bridge
                # Add to bridge
                code, res = self.exec([f"ovs-vsctl", "--may-exist", "add-port",
                                       self.system_id, physical_name])
                if code == 0:
                    self.logger.info(f"Added physical interface {physical_name} to switch {self.entity_id}")
                else:
                    self.logger.error(f"Could not add physical interface {physical_name} to "
                                      f"switch {self.entity_id} (Bridge {self.system_id})")
                    for line in res:
                        self.logger.error(line)

            if interface.is_tap_port():
                if interface.is_physical():
                    raise InvalidInterfaceException(f"Cannot use a physical interface as a tap interface")
                self.logger.critical(f"Tap interfaces are not yet implemented!")

            if interface.is_mirror_port():
                # Declare port as mirror port
                code, res = self.exec([f"ovs-vsctl", "--may-exist", "add-port",
                                       self.system_id, interface.get_system_name()])
                if code != 0:
                    self.logger.error(f"Could not add interface {interface.get_system_name()} to switch {self.system_id}")
                    continue
                code, res = self.exec(["ovs-vsctl",
                                       "--", "--id=@p", "get", "port", interface.get_system_name(),
                                       "--", "--id=@m", f"create mirror name={interface.entity_id}",
                                       "select-all=true", "output-port=@p",
                                       "--", "set", "bridge", self.system_id,
                                       "mirrors=@m"])
                if code == 0:
                    self.logger.info(f"Set {interface.entity_id} as the active mirror for switch {self.entity_id}")
                else:
                    self.logger.error(f"Could not set {interface.entity_id} as the active mirror for "
                                      f"switch {self.entity_id}: {res=}")
                    for line in res:
                        self.logger.error(line)
