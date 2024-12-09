import abc
import ipaddress
from typing import Optional, TYPE_CHECKING, List

from wattson.cosimulation.exceptions import NetworkException
from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.interface.network_link import NetworkLink
    from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode


class NetworkInterface(NetworkEntity, abc.ABC):
    @abc.abstractmethod
    def get_ip_address(self) -> Optional[ipaddress.IPv4Address]:
        ...

    @abc.abstractmethod
    def set_ip_address(self, ip_address: Optional[ipaddress.IPv4Address]):
        """
        Update the ip address of this interface
        @param ip_address: The ip to use or None
        @return:
        """
        ...

    @abc.abstractmethod
    def get_subnet_prefix_length(self) -> Optional[int]:
        ...

    @abc.abstractmethod
    def get_mac_address(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def has_ip(self) -> bool:
        """
        @return: True iff this interface has an IP address assigned
        """
        ...

    @property
    def ip_address_string(self) -> Optional[str]:
        """
        @return: The IP address of this interface with subnet length indicator
        """
        if self.get_ip_address() is None or self.get_subnet_prefix_length() is None:
            return None
        return f"{self.get_ip_address()}/{self.get_subnet_prefix_length()}"

    @property
    def ip_address_short_string(self) -> Optional[str]:
        """
        @return: The IP address of this interface without subnet length indicator
        """
        if self.get_ip_address() is None:
            return None
        return f"{self.get_ip_address()}"

    @property
    @abc.abstractmethod
    def is_management(self) -> bool:
        """
        @return: True iff this interface belongs to the management network
        """
        ...

    @abc.abstractmethod
    def get_system_name(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_link(self) -> Optional['NetworkLink']:
        ...

    @abc.abstractmethod
    def get_node(self) -> Optional['NetworkNode']:
        ...

    @abc.abstractmethod
    def up(self):
        """
        Set the interface up
        @return:
        """
        ...

    @abc.abstractmethod
    def down(self):
        """
        Set the interface down
        @return:
        """
        ...

    def get_subnet(self, include_management: bool = True, exclude_interfaces: Optional[List['NetworkInterface']] = None) -> Optional[ipaddress.IPv4Network]:
        subnet = None
        if not include_management and self.is_management:
            return None
        if exclude_interfaces is None:
            exclude_interfaces = []
        if self in exclude_interfaces:
            return subnet
        exclude_interfaces.append(self)
        if self.has_ip():
            subnet = ipaddress.IPv4Network(f"{self.get_ip_address()}/{self.get_subnet_prefix_length()}", strict=False)
            return subnet
        else:
            link = self.get_link()
            if link is not None:
                connected_interface = link.get_interface_a() if link.get_interface_b() == self else link.get_interface_b()
                if connected_interface.has_ip():
                    return connected_interface.get_subnet(exclude_interfaces=exclude_interfaces)
                else:
                    node = connected_interface.get_node()
                    if node is None:
                        return None
                    from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost
                    if isinstance(node, NetworkHost):
                        return None
                    subnets = node.get_subnets(include_management=include_management, exclude_interfaces=exclude_interfaces)
                    if len(subnets) > 1:
                        raise NetworkException(f"Interface {self.get_system_name()} assigned to multiple subnets ({[repr(subnet) for subnet in subnets]})")
                    if len(subnets) == 0:
                        return None
                    return subnets[0]
        return None

    def __repr__(self):
        return f"Interface {self.entity_id} ({self.ip_address_string} // {self.get_mac_address()})"
