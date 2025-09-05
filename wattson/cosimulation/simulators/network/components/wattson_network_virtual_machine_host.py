import dataclasses
import json
import typing
from typing import Optional, List, Dict

from wattson.cosimulation.simulators.network.components.interface.network_virtual_machine_host import NetworkVirtualMachineHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import \
    RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.networking.namespaces.virtual_machine_namespace import VirtualMachineNamespace


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkVirtualMachineHost(WattsonNetworkHost, NetworkVirtualMachineHost):
    def __post_init__(self):
        super().__post_init__()
        self._os: Optional[str] = None

    @property
    def os(self) -> str:
        return self.get_namespace().get_os()

    def get_namespace(self) -> VirtualMachineNamespace:
        return typing.cast(VirtualMachineNamespace, super().get_namespace())

    @classmethod
    def get_class_id(cls):
        # Share value with default hosts
        return WattsonNetworkHost.get_class_id()

    @classmethod
    def set_class_id(cls, class_id: int):
        # Share value with default hosts
        WattsonNetworkHost.set_class_id(class_id)

    def get_prefix(self):
        return "h"

    def start(self):
        super().start()

    def stop(self):
        super().stop()

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__
        })
        return d

    def set_route(self, target: str, interface: Optional['WattsonNetworkInterface'] = None, gateway_ip: Optional[str] = None) -> bool:
        if self.os == "linux":
            return super().set_route(target=target, interface=interface, gateway_ip=gateway_ip)
        elif self.os == "windows":
            if interface is None and gateway_ip is None:
                return False
            cmd = ["netsh", "interface", "ipv4", "add", "route"]
            if target == "default":
                target = "0.0.0.0/0"
            cmd.extend([f'prefix="{target}"'])
            if interface is not None:
                cmd.extend([f'interface="{interface.interface_name}"'])
            if gateway_ip is not None:
                cmd.extend([f'nexthop="{gateway_ip}"'])
            cmd.extend(["store=active"])
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False

        self.logger.debug("Setting route")
        self.logger.debug(" ".join(cmd))
        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not set route for {target}")
            self.logger.error(lines)
            return False

    def clear_routes(self) -> bool:
        if self.os == "linux":
            return super().clear_routes()
        elif self.os == "windows":
            cmd = ["route", "-f"]
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False

        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not clear routes")
            self.logger.error(lines)
            return False

    def interface_up(self, interface: 'WattsonNetworkInterface') -> bool:
        self.logger.debug(f"Setting interface {interface.interface_name} up")
        if self.os == "linux":
            return super().interface_up(interface)
        elif self.os == "windows":
            cmd = ["netsh", "interface", "set", "interface", f'name="{interface.interface_name}"', "admin=enabled"]
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False

        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not set interface {interface.interface_name} up")
            self.logger.error(lines)
            return False

    def interface_down(self, interface: 'WattsonNetworkInterface') -> bool:
        self.logger.debug(f"Setting interface {interface.interface_name} down")
        if self.os == "linux":
            return super().interface_down(interface)
        elif self.os == "windows":
            cmd = ["netsh", "interface", "set", "interface", f'name="{interface.interface_name}"', "admin=disabled"]
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False

        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not set interface {interface.interface_name} down")
            self.logger.error(lines)
            return False

    def interface_flush_ip(self, interface: 'WattsonNetworkInterface') -> bool:
        self.logger.debug(f"Flushing IPs of interface {interface.interface_name}")
        if self.os == "linux":
            return super().interface_flush_ip(interface)
        elif self.os == "windows":
            # Get current addresses
            interfaces = self.interfaces_list_existing()
            cmds = []
            addresses = []
            for host_interface in interfaces:
                if host_interface["name"] == interface.interface_name:
                    addresses = [addr["ip-address"] for addr in host_interface["ip-addresses"] if addr["ip-address-type"] == "ipv4"]
            for address in addresses:
                cmds.append(["netsh", "interface", "ipv4", "delete", "address", interface.interface_name, address, "gateway=all"]) #, "store=active"])
            for cmd in cmds:
                code, lines = self.exec(cmd)
                if not code == 0:
                    self.logger.error(f"Could not remove IP from interface {interface.interface_name}")
                    self.logger.error(lines)
                    return False
            return True
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False

    def interface_set_ip(self, interface: 'WattsonNetworkInterface') -> bool:
        if interface.ip_address_string is None:
            return True
        self.logger.debug(f"Setting IP of interface {interface.interface_name} to {interface.ip_address_string}")
        if self.os == "linux":
            return super().interface_set_ip(interface)
        elif self.os == "windows":
            cmd = ["netsh", "interface", "ipv4", "set", "address", f'name="{interface.interface_name}"',
                   "static", f'address="{interface.ip_address_string}"'] #, "store=active"]
        else:
            self.logger.error(f"Unsupported OS: {self.os}")
            return False
        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not set IP of {interface.get_system_name()} to {interface.ip_address_string}")
            self.logger.debug(lines)
        return code == 0

    def interface_rename(self, old_name: str, new_name: str) -> bool:
        """
        Renames a physical interface on the node

        Args:
            old_name (str):
                The original interface name
            new_name (str):
                The new interface name

        Returns:
            bool: Whether the action was successful
        """
        self.logger.debug(f"Renaming interface {old_name} to {new_name}")
        if self.os == "linux":
            return super().interface_rename(old_name=old_name, new_name=new_name)
        elif self.os == "windows":
            cmd = ["netsh", "interface", "set", "interface", f'name="{old_name}"', f'newname="{new_name}"']
        else:
            self.logger.error(f"Unknown OS: {self.os}")
            return False
        code, lines = self.exec(cmd)
        if not code == 0:
            self.logger.error(f"Could not rename interface {old_name} to {new_name}")
            self.logger.error(lines)
            return False
        return True

    def interfaces_list_existing(self) -> List[Dict]:
        if self.os == "linux":
            return super().interfaces_list_existing()
        return self.get_namespace().get_interfaces()
