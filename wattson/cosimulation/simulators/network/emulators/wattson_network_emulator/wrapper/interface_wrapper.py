import typing

from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.network_link_model import NetworkLinkModel
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.entity_wrapper import EntityWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace

if typing.TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator


class InterfaceWrapper(EntityWrapper):

    def __init__(self, entity: NetworkEntity, emulator: 'WattsonNetworkEmulator'):
        super().__init__(entity, emulator)
        self._previous_link_model = NetworkLinkModel()

    def get_namespace(self) -> Namespace:
        # Namespace is given by associated node
        return self.emulator.get_namespace(self.interface.get_node())

    @property
    def interface(self) -> WattsonNetworkInterface:
        return typing.cast(WattsonNetworkInterface, self.entity)

    def exists(self):
        code0, lines = self.get_namespace().exec(["ip", "link", "show", self.interface.interface_name])
        return code0

    def create(self):
        if self.interface.is_physical():
            self.push_to_namespace()
            return
        if self.interface.is_tap_port():
            self.logger.warning("Tap port management not implemented")
            return
        if self.interface.is_virtual():
            # Create virtual interface
            if self.interface.link is None:
                # Create placeholder interface
                self.logger.info(f"Manually creating interface")
                success, lines = self.get_namespace().exec(["ip", "link", "add", self.interface.interface_name, "type", "dummy"])
                for line in lines:
                    print(line)
                self.push_to_namespace()
                self.configure()
                return
            else:
                # Link will handle creation
                return
        self.logger.error("Unknown interface type - cannot be handled")

    def clean(self):
        interface = self.interface
        if interface.is_physical():
            # Will be pushed to default namespace automatically
            return
        if interface.is_tap_port():
            self.logger.warning("Tap port management not implemented")
            return
        if interface.is_virtual():
            node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
            node_wrapper.remove_interface(interface)
            self.get_namespace().exec(["ip", "link", "delete", interface.interface_name])
            return
        self.logger.error("Unknown interface type - cannot be handled")

    def down(self) -> bool:
        """
        Sets the interface down
        @return: Whether the command was successful
        """
        code0, _ = self.get_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "down"])
        return code0

    def up(self) -> bool:
        """
        Sets the interface up
        @return: Whether the command was successful
        """
        self.interface.up()
        code0, _ = self.get_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "up"])
        return code0

    def configure(self):
        """
        Configures the interface with IP, MAC, and other properties
        @return:
        """
        interface = self.interface
        if not self.exists():
            self.logger.warning(f"Cannot configure interface {interface.interface_name}")
            return False
        self.down()
        # Set mac if given
        self.update_mac_address()
        # Set IP
        self.update_ip_address()
        self.up()

    def update_mac_address(self):
        interface = self.interface
        if interface.mac_address is not None:
            code0, lines = self.get_namespace().exec(["ip", "link", "set", "dev", interface.interface_name, "address", interface.mac_address])
            if not code0:
                self.logger.warning(f"Cannot set MAC for interface {interface.interface_name}")
                self.logger.debug("\n".join(lines))

    def update_ip_address(self):
        interface = self.interface
        # Remove all IPs
        self.logger.debug(f"Removing IP of interface {interface.interface_name}")
        code0, lines = self.get_namespace().exec(["ip", "addr", "flush", "dev", interface.interface_name])
        if not code0:
            self.logger.warning(f"Cannot remove IP for interface {interface.interface_name}")
            self.logger.debug("\n".join(lines))
        # Set IP if given
        if interface.ip_address_string is not None:
            self.logger.debug(f"Setting interface {interface.interface_name} IP to {interface.ip_address_string}")
            code0, lines = self.get_namespace().exec(["ip", "addr", "add", interface.ip_address_string, "dev", interface.interface_name])
            if not code0:
                self.logger.warning(f"Cannot set IP for interface {interface.interface_name}")
                self.logger.debug("\n".join(lines))

    def pull_to_main_namespace(self) -> bool:
        """
        Moves the interface from its associated namespace to the main namespace
        @return:
        """
        interface = self.interface
        node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
        node_wrapper.remove_interface(interface)
        code0, lines = self.get_namespace().exec(["ip", "link", "set", interface.interface_name, "netns", self.emulator.get_main_namespace().name])
        if not code0:
            self.logger.warning(f"Cannot move interface {interface.interface_name}")
            self.logger.debug("\n".join(lines))
            return False
        return True

    def push_to_namespace(self, namespace: typing.Optional[Namespace] = None) -> bool:
        """
        Pushes the interface from the main namespace to the given or associated namespace
        @param namespace: The namespace to put this interface into. If None, the nodes namespace is used.
        @return:
        """
        """
        Moves the interface from its associated namespace to the main namespace
        @return:
        """
        interface = self.interface
        if namespace is None:
            namespace = self.get_namespace()
        code0, lines = self.emulator.get_main_namespace().exec(["ip", "link", "set", interface.interface_name, "netns", namespace.name])
        if not code0:
            self.logger.warning(f"Cannot move interface {interface.interface_name}")
            self.logger.debug("\n".join(lines))
            return False
        node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
        node_wrapper.add_interface(interface)
        return True

    def apply_tc_properties(self, link_model: NetworkLinkModel) -> bool:
        """
        Applies the properties specified by the link wrapper to this interface.
        Requires tc.
        @param link_model: The link wrapper to use
        @return: Whether the properties could be applied
        """
        namespace = self.get_namespace()
        interface = self.interface

        code0, _ = namespace.exec(["which", "tc"])
        if not code0:
            self.logger.warning(f"Cannot apply link wrapper to interface - tc not found")
            return False

        if interface.is_physical():
            self.logger.warning(f"Refusing to apply link wrapper to physical interface")
            return False

        # TODO: Support asymmetric models
        reset_required = False

        bw = link_model.bandwidth_mbps
        reset_required |= bw is None and self._previous_link_model.bandwidth_mbps is not None

        delay = link_model.delay_ms
        reset_required |= delay is None and self._previous_link_model.delay_ms is not None

        jitter = link_model.jitter_ms
        reset_required |= jitter is None and self._previous_link_model.jitter_ms is not None

        packet_loss = link_model.packet_loss_percent
        reset_required |= packet_loss is None and self._previous_link_model.packet_loss_percent is not None

        parent = "root"

        success = True

        if self.is_tc_enabled():
            if reset_required:
                action = "add"
                code0, lines = namespace.exec(["tc", "qdisc", "del", "dev", interface.interface_name, parent])
                if not code0:
                    self.logger.error(f"Could not delete tc configuration")
                    self.logger.error("\n".join(lines))
                    success = False
            else:
                action = "change"
        else:
            # Clear and reapply
            action = "add"

        # Apply bandwidth
        if bw is not None:
            if action == "add":
                code0, lines = namespace.exec(["tc", "qdisc", "add", "dev", interface.interface_name, "root", "handle", "5:0", "htb", "default", "1"])
                if not code0:
                    self.logger.error(f"Could not add tc configuration")
                    self.logger.error("\n".join(lines))
                    success = False
            parent = "parent 5:0"
            code0, lines = namespace.exec(["tc", "class", action, "dev", interface.interface_name, parent,
                                           "classid", "5:1", "htb", "rate", f"{bw}Mbit", "burst", "15k"])
            if not code0:
                self.logger.error(f"Could not set bandwidth configuration")
                self.logger.error("\n".join(lines))
                success = False
            parent = "parent 5:1"

        # Apply delay, jitter and loss
        if delay is not None or jitter is not None or parent is not None:
            cmd = ["tc", "qdisc", action, "dev", interface.interface_name, parent, "handle 10:", "netem"]
            if delay is not None:
                cmd.extend(["delay", f"{delay}ms"])
            if jitter is not None:
                cmd.extend([f"{jitter}ms"])
            if packet_loss is not None:
                cmd.extend(["loss", f"{packet_loss}"])
            parent = "parent 10:1"
            code0, lines = namespace.exec(cmd)
            if not code0:
                self.logger.error(f"Could not set delay/jitter/loss configuration")
                self.logger.error("\n".join(lines))
                success = False

        # Copy link wrapper
        self._previous_link_model = link_model.to_remote_representation()
        return success

    def is_tc_enabled(self) -> bool:
        code0, lines = self.get_namespace().exec(["tc", "qdisc", "show", "dev", self.interface.interface_name])
        if not code0:
            self.logger.error("Could not query tc status")
            self.logger.error("\n".join(lines))
            return False
        out = " ".join(lines)
        return "noqueue" not in out and "priomap" not in out

    def _tc_bandwidth_command(self, bandwidth: float) -> typing.List[str]:
        pass
