import dataclasses
import ipaddress
from typing import TYPE_CHECKING, Optional, ClassVar

from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_link import WattsonNetworkLink
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkInterface(WattsonNetworkEntity, NetworkInterface):
    id: Optional[str] = None
    mac_address: Optional[str] = None
    ip_address: Optional[ipaddress.IPv4Address] = None
    subnet_prefix_length: Optional[int] = None
    node: Optional['WattsonNetworkNode'] = None
    link: Optional['WattsonNetworkLink'] = None
    is_management: bool = False

    class_id: ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()

    def start(self):
        if not self.is_started:
            super().start()
        self.node.on_interface_start(self)

    def get_prefix(self) -> str:
        return "i"

    @property
    def entity_id(self) -> str:
        return self.interface_id

    @property
    def interface_id(self) -> str:
        return f"{self.node.node_id}-{self.id}"

    def get_next_node(self) -> Optional['WattsonNetworkNode']:
        """
        Determines the next node this interface connects to (if any).
        I.e., this-interface <-> link <-> other-interface <-> returned-node
        @return: The next network node if any
        """
        if self.link is not None:
            other_interface = self.link.interface_a if self.link.interface_b == self else self.link.interface_b
            if other_interface is None:
                return None
            return other_interface.node
        return None

    @property
    def interface_name(self) -> str:
        """
        The hardware name of the interface
        @return:
        """
        return f"{self.node.system_id}-{self.system_id}"

    def generate_name(self):
        if self.system_name is None:
            if self.is_management:
                return self.node.get_free_interface_name("mgm")
            elif self.is_mirror_port():
                return self.node.get_free_interface_name("mir")
            elif self.is_tap_port():
                return self.node.get_free_interface_name("tap")
            elif self.is_physical():
                return self.node.get_free_interface_name("phy")
            else:
                return self.node.get_free_interface_name()
        return self.system_name

    def get_ip_address(self) -> Optional[ipaddress.IPv4Address]:
        return self.ip_address

    def set_ip_address(self, ip_address: Optional[ipaddress.IPv4Address] = None) -> bool:
        self.ip_address = ip_address
        self.network_emulator.on_entity_change(self, "ip_address_set")
        return True

    def get_subnet_prefix_length(self) -> Optional[int]:
        return self.subnet_prefix_length

    def get_mac_address(self) -> Optional[str]:
        return self.mac_address

    def has_ip(self) -> bool:
        return self.ip_address is not None

    def is_mirror_port(self) -> bool:
        return self.config.get("mirror", False)

    def is_virtual(self) -> bool:
        return self.config.get("type", "virtual") == "virtual"

    def is_physical(self) -> bool:
        return self.config.get("type", "virtual") == "physical"

    def is_tap_port(self) -> bool:
        return self.config.get("type", "virtual") == "tap"

    def get_physical_name(self) -> str:
        if not self.is_physical():
            raise KeyError(f"Interface {self.interface_id} has no physical interface assigned")
        return self.config["physical"]

    def get_tap_info(self) -> dict:
        return {
            "mode": self.config["mode"],
            "target": self.config["target"]
        }

    def get_tap_name(self) -> str:
        return f"{self.node.entity_id}-{self.id}"

    def get_link(self) -> Optional['WattsonNetworkLink']:
        return self.link

    def get_node(self) -> Optional['WattsonNetworkNode']:
        return self.node

    def up(self):
        if self.emulation_instance is not None and hasattr(self.emulation_instance, "up"):
            self.emulation_instance.up()

    def down(self):
        if self.emulation_instance is not None and hasattr(self.emulation_instance, "down"):
            self.emulation_instance.down()

    def get_system_name(self) -> Optional[str]:
        if self.is_physical():
            return self.get_physical_name()
        if self.is_tap_port():
            return self.get_tap_name()
        return self.interface_name

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "class": self.__class__.__name__,
            "mac_address": self.get_mac_address(),
            "ip_address": self.get_ip_address(),
            "is_management": self.is_management,
            "node_id": self.node.entity_id,
            "system_name": self.get_system_name(),
            "subnet_prefix_length": self.get_subnet_prefix_length()
        })
        if self.link is not None:
            d["link_id"] = self.link.entity_id
        return d


