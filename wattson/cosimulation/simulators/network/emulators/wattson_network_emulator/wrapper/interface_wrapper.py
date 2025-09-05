import math
import time
import typing
from typing import Optional

from powerowl.performance.function_performance import measure
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

    def get_additional_namespace(self) -> Namespace:
        # Namespace is given by associated node
        return self.emulator.get_additional_namespace(self.interface.get_node())

    @property
    def interface(self) -> WattsonNetworkInterface:
        return typing.cast(WattsonNetworkInterface, self.entity)

    def exists(self):
        interfaces = self.interface.node.interfaces_list_existing()
        for interface in interfaces:
            if interface["name"] == self.interface.interface_name:
                return True
        return False

    def wait_exists(self, timeout: float = 5) -> bool:
        """
        Waits for the interface to exist for the given timeout.
        Args:
            timeout: The timeout in seconds to wait for the interface. (Default value = 5)

        Returns:
            True if the interface exists, False otherwise.
        """
        if timeout < 0:
            return self.exists()

        poll_interval = 1
        if poll_interval > timeout:
            poll_interval = timeout
        else:
            polls = math.ceil(timeout / poll_interval)
            poll_interval = timeout / polls
        wait_end = time.time() + timeout
        while time.time() < wait_end:
            if self.exists():
                return True
            time.sleep(poll_interval)
        return self.exists()

    def create(self, wait_timeout: typing.Optional[float] = None) -> bool:
        if self.interface.is_physical():
            self.flush_ip()
            self.push_to_namespace()
            self.up()
            return True
        if self.interface.is_tap_port():
            self.logger.warning("Tap port management not implemented")
            return False
        if self.interface.is_virtual():
            # Create virtual interface
            if self.interface.link is None:
                # Create placeholder interface
                self.logger.info(f"Manually creating interface")
                self.logger.warning(f"Not implemented for Windows!")
                success, lines = self.get_namespace().exec(["ip", "link", "add", self.interface.interface_name, "type", "dummy"])
                for line in lines:
                    print(line)
                self.push_to_namespace()
                self.configure(wait_timeout=2)
                return True
            else:
                # Link will handle creation
                return True
        self.logger.error("Unknown interface type - cannot be handled")
        return False

    def clean(self):
        interface = self.interface
        if interface.is_physical():
            # Will be pushed to default namespace automatically
            node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
            node_wrapper.remove_interface(interface)
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

    def flush_ip(self) -> bool:
        """
        Flushes the IP from the interface.


        Returns:
            bool: Whether the command was successful
        """
        return self.interface.node.interface_flush_ip(self.interface)

        code0, lines = self.get_namespace().exec(["ip", "address", "flush", "dev", self.interface.interface_name])
        # if self.has_remote_namespace():
        #    remote_success, remote_lines = self.get_remote_namespace().exec()
        if not code0:
            self.logger.error(f"\n".join(lines))
        return code0

    def down(self) -> bool:
        """
        Sets the interface down


        Returns:
            bool: Whether the command was successful
        """
        if self.has_additional_namespace():
            self.get_additional_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "down"])
        return self.interface.down()
        code0, _ = self.get_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "down"])
        return code0

    def up(self) -> bool:
        """
        Sets the interface up


        Returns:
            bool: Whether the command was successful
        """
        if self.has_additional_namespace():
            self.get_additional_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "up"])

        return self.interface.up()
        code0, _ = self.get_namespace().exec(["ip", "link", "set", "dev", self.interface.interface_name, "up"])
        return code0

    def configure(self, wait_timeout: typing.Optional[float] = None) -> bool:
        """Configures the interface with IP, MAC, and other properties"""
        interface = self.interface

        if (wait_timeout is None and not self.exists()) or (wait_timeout is not None and not self.wait_exists(wait_timeout)):
            self.logger.warning(f"Cannot configure interface {interface.interface_name}")
            # self.logger.warning(repr(self.interface.node.interfaces_list_existing()))
            return False
        # self.down()
        # Set mac if given
        self.update_mac_address()
        # Set IP
        self.update_ip_address()
        return self.up()

    def update_mac_address(self):
        interface = self.interface
        if interface.mac_address is not None:
            code0, lines = self.get_additional_namespace().exec(["ip", "link", "set", "dev", interface.interface_name, "address", interface.mac_address])
            if not code0:
                self.logger.warning(f"Cannot set MAC for interface {interface.interface_name}")
                self.logger.debug("\n".join(lines))

    def update_ip_address(self):
        interface = self.interface
        # Remove all IPs
        self.logger.debug(f"Removing IP of interface {interface.interface_name}")
        self.interface.node.interface_flush_ip(interface)
        """
        code0, lines = self.get_namespace().exec(["ip", "addr", "flush", "dev", interface.interface_name])
        if not code0:
            self.logger.warning(f"Cannot remove IP for interface {interface.interface_name}")
            self.logger.debug("\n".join(lines))
        """
        # Set IP if given
        if interface.ip_address_string is not None:
            self.logger.debug(f"Setting interface {interface.interface_name} IP to {interface.ip_address_string}")
            self.interface.node.interface_set_ip(interface)
            """
            code0, lines = self.get_namespace().exec(["ip", "addr", "add", interface.ip_address_string, "dev", interface.interface_name])
            if not code0:
                self.logger.warning(f"Cannot set IP for interface {interface.interface_name}")
                self.logger.debug("\n".join(lines))
            """

    def pull_to_main_namespace(self) -> bool:
        """
        Moves the interface from its associated namespace to the main namespace

        """
        interface = self.interface
        node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
        node_wrapper.remove_interface(interface)
        if self.get_namespace().is_network_namespace:
            code0, lines = self.get_namespace().exec(["ip", "link", "set", interface.interface_name, "netns", self.emulator.get_main_namespace().name])
            if not code0:
                self.logger.warning(f"Cannot move interface {interface.interface_name}")
                self.logger.debug("\n".join(lines))
                return False
        return True

    def push_to_namespace(self, namespace: typing.Optional[Namespace] = None) -> bool:
        """
        Pushes the interface from the main namespace to the given or associated namespace

        Args:
            namespace (typing.Optional[Namespace], optional):
                The namespace to put this interface into. If None, the nodes namespace is used.
                (Default value = None)
        """
        interface = self.interface
        if namespace is None:
            namespace = self.get_namespace()
        if namespace.is_network_namespace:
            code0, lines = self.emulator.get_main_namespace().exec(["ip", "link", "set", interface.interface_name, "netns", namespace.name])
            if not code0:
                self.logger.warning(f"Cannot move interface {interface.interface_name}")
                self.logger.warning("\n".join(lines))
                return False
        node_wrapper = typing.cast(NodeWrapper, self.emulator.get_wrapper(interface.node))
        node_wrapper.add_interface(interface)
        return True

    def apply_tc_properties(self, link_model: NetworkLinkModel) -> bool:
        """
        Applies the properties specified by the link wrapper to this interface.
        Requires tc.

        Args:
            link_model (NetworkLinkModel):
                The link wrapper to use

        Returns:
            bool: Whether the properties could be applied
        """
        # Additional namespace is external, i.e., native for the host machine
        namespace = self.get_additional_namespace()
        interface = self.interface

        """
        if self.interface.node.os != "linux":
            self.logger.warning(f"Cannot apply TC properties for {self.interface.node.os} - only linux supported")
            return False
        """

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

        parent = ["root"]

        success = True

        if self.is_tc_enabled():
            if reset_required:
                action = "add"
                code0, lines = namespace.exec(["tc", "qdisc", "del", "dev", interface.interface_name] + parent)
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
            parent = ["parent", "5:0"]
            code0, lines = namespace.exec(
                ["tc", "class", action, "dev", interface.interface_name] + parent + ["classid", "5:1", "htb", "rate", f"{bw}Mbit", "burst", "15k"]
            )
            if not code0:
                self.logger.error(f"Could not set bandwidth configuration")
                self.logger.error("\n".join(lines))
                success = False
            parent = ["parent", "5:1"]

        # Apply delay, jitter and loss
        if delay is not None or jitter is not None or parent is not None:
            cmd = ["tc", "qdisc", action, "dev", interface.interface_name] + parent + ["handle", "10:", "netem"]
            if delay is not None:
                cmd.extend(["delay", f"{delay}ms"])
            if jitter is not None:
                cmd.extend([f"{jitter}ms"])
            if packet_loss is not None:
                cmd.extend(["loss", f"{packet_loss}"])
            parent = ["parent", "10:1"]
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
