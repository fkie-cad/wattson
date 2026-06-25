import dataclasses
from typing import List, TYPE_CHECKING, ClassVar, Optional

from wattson.cosimulation.exceptions import InvalidInterfaceException
from wattson.cosimulation.simulators.network.components.interface.network_switch import NetworkSwitch
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.configuration import ServiceConfiguration
from wattson.services.wattson_python_service import WattsonPythonService
from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
    from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator


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

    def find_admin_interface(self) -> Optional['WattsonNetworkInterface']:
        for interface in self.get_interfaces():
            if interface.config.get("use_for_management", False):
                return interface
        return None

    def get_spare_interface(self) -> Optional['WattsonNetworkInterface']:
        for interface in self.get_interfaces():
            if interface.config.get("spare", False):
                if interface.get_link() is None:
                    return interface
        return None

    def on_scenario_loaded(self, network_emulator: 'NetworkEmulator') -> None:
        self.ensure_management_host()

    def ensure_management_host(self):
        if self.network_emulator is None:
            raise RuntimeError("Cannot add management host without network emulator reference")
        if self.should_be_manageable() and self.is_ovs():
            if not self.has_child_node_role("switch-management"):
                # Add management node
                from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
                management_host = WattsonNetworkHost(id=f"{self.id}-mgm", display_name=f"{self.display_name} (MGM)")
                management_host.add_role("switch-management")
                self.add_child_node(management_host)
                interface_switch = self.find_admin_interface()

                self.network_emulator.connect_nodes(management_host, self, interface_a_options={
                    "ip": self.get_switch_management_ip_address_str(False),
                    "prefix_length": self.get_switch_management_prefix_length()
                }, interface_b=interface_switch)

                service_configuration = ServiceConfiguration()
                service_configuration.update({
                    "name": "WattsonOvsManager",
                    "wattson_client_config": {
                        "query_socket": "!sim-control-query-socket",
                        "publish_socket": "!sim-control-publish-socket",
                    },
                    "nodeid": "!nodeid",
                    "entityid": "!entityid",
                    "ip": "!ip",
                    "scenario_path": "!scenario_path",
                    "ovs_socket_string": "!ovs-socket-string",
                    "ovs_socket_path": "!ovs-socket-absolute",
                    "ovs_switch_entities": [self.entity_id],
                    "ovs_switch_bridges": []
                })
                from wattson.apps.ovscc.deployment import OvsCCDeployment
                management_host.add_service(WattsonPythonService(OvsCCDeployment, service_configuration, management_host))

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

    def should_be_manageable(self) -> bool:
        return self.config.get("configuration", {}).get("is_manageable", False)

    def is_manageable(self) -> bool:
        return self.has_child_node_role("switch-management")

    def get_switch_management_ip_address_str(self, with_subnet_length: bool = True) -> Optional[str]:
        if not self.should_be_manageable():
            return None
        ip_address_string: Optional[str] = self.config.get("configuration", {}).get("management_ip_address")
        if ip_address_string is None:
            return None
        if with_subnet_length:
            return ip_address_string
        return ip_address_string.split("/")[0]

    def get_switch_management_prefix_length(self) -> Optional[int]:
        ip_address_string = self.get_switch_management_ip_address_str(with_subnet_length=True)
        if ip_address_string is None:
            return None
        parts = ip_address_string.split("/")
        if len(parts) == 2:
            return int(parts[1])
        return None

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
                self.enable_mirror(interface)

    def enable_mirror(self, interface: 'WattsonNetworkInterface') -> bool:
        # Declare port as mirror port
        interface.set_mirror_port(True)

        code, res = self.exec(
            [f"ovs-vsctl", "--may-exist", "add-port",
             self.system_id, interface.get_system_name()]
            )
        if code != 0:
            self.logger.error(f"Could not add interface {interface.get_system_name()} to switch {self.system_id}")
            return False

        cmd = ["ovs-vsctl",
               "--", "--id=@p", "get", "port", interface.get_system_name(),
               "--", "--id=@m", f"create mirror name={interface.entity_id}",
               "select-all=true", "output-port=@p",
               "--", "set", "bridge", self.system_id,
               "mirrors=@m"]
        # Shell seems to be required here...
        code, res = self.exec(cmd, shell=True)
        if code == 0:
            self.logger.info(f"Set {interface.entity_id} as the active mirror for switch {self.entity_id} ({self.display_name})")
            return True
        else:
            self.logger.error(
                f"Could not set {interface.entity_id} as the active mirror for "
                f"switch {self.entity_id}: {res=}"
            )
            for line in res:
                self.logger.error(line)
        return False

    def disable_mirror(self, interface: 'WattsonNetworkInterface') -> bool:
        cmd = [
            "ovs-vsctl",
            "--", "--id=@p", "get", "mirror", interface.entity_id,
            "--", "remove", "bridge", self.system_id, "mirrors", "@p"
        ]
        code, res = self.exec(cmd, shell=True)
        if code == 0:
            interface.set_mirror_port(False)
            self.logger.info(f"Disabled {interface.entity_id} as mirror for {self.entity_id}")
            return True
        self.logger.error(f"Could not disable mirror port {interface.entity_id} at {self.entity_id}:\n" + "\n".join(res))
        return False

    def clear_mirrors(self):
        if not self.is_ovs():
            return False
        code, lines = self.exec(["ovs-vsctl", "clear", "bridge", self.system_id, "mirrors"])
        for interface in self.interfaces:
            if interface.is_mirror_port():
                self.logger.info(f"Disabling mirror port {interface.entity_id} ({interface.interface_name})")
                interface.set_mirror_port(False)
        if code != 0:
            self.logger.error("\n".join(lines))
        return code == 0
