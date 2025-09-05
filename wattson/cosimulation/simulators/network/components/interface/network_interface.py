import abc
import ipaddress
import time
from typing import Optional, TYPE_CHECKING, List, Dict

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

        Args:
            ip_address (Optional[ipaddress.IPv4Address]):
                The ip to use or None
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
        


        Returns:
            bool: True iff this interface has an IP address assigned
        """
        ...

    @property
    def ip_address_string(self) -> Optional[str]:
        """
        


        Returns:
            Optional[str]: The IP address of this interface with subnet length indicator
        """
        if self.get_ip_address() is None or self.get_subnet_prefix_length() is None:
            return None
        return f"{self.get_ip_address()}/{self.get_subnet_prefix_length()}"

    @property
    def ip_address_short_string(self) -> Optional[str]:
        """
        


        Returns:
            Optional[str]: The IP address of this interface without subnet length indicator
        """
        if self.get_ip_address() is None:
            return None
        return f"{self.get_ip_address()}"

    @property
    @abc.abstractmethod
    def is_management(self) -> bool:
        """
        


        Returns:
            bool: True iff this interface belongs to the management network
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
        """Set the interface up"""
        ...

    @abc.abstractmethod
    def down(self):
        """Set the interface down"""
        ...

    def get_subnet(self, include_management: bool = True, exclude_interfaces: Optional[List['NetworkInterface']] = None,
                   subnet_cache: Optional[Dict[str, ipaddress.IPv4Network]] = None) -> Optional[ipaddress.IPv4Network]:
        subnet = None
        if not include_management and self.is_management:
            return None
        if exclude_interfaces is None:
            exclude_interfaces = []

        def _fill_cache(_interface, _subnet):
            if subnet_cache is None:
                return
            subnet_cache[_interface.entity_id] = _subnet
            from wattson.cosimulation.simulators.network.components.interface.network_switch import NetworkSwitch
            if isinstance(_interface.get_node(), NetworkSwitch):
                for _switch_interface in _interface.get_node().get_interfaces():
                    subnet_cache[_switch_interface.entity_id] = _subnet

        if subnet_cache is not None and self.entity_id in subnet_cache:
            return subnet_cache[self.entity_id]

        if self in exclude_interfaces:
            return subnet
        exclude_interfaces.append(self)

        if self.has_ip():
            subnet = ipaddress.IPv4Network(f"{self.get_ip_address()}/{self.get_subnet_prefix_length()}", strict=False)
            _fill_cache(self, subnet)
            return subnet

        link = self.get_link()
        if link is not None:
            connected_interface = link.get_interface_a() if link.get_interface_b() == self else link.get_interface_b()
            if connected_interface.has_ip():
                return connected_interface.get_subnet(exclude_interfaces=exclude_interfaces, subnet_cache=subnet_cache)
            else:
                node = connected_interface.get_node()
                if node is None:
                    return None
                # exclude_nodes.append(node)
                from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost
                if isinstance(node, NetworkHost):
                    return None
                exclude_interfaces.append(connected_interface)
                subnets = node.get_subnets(include_management=include_management, exclude_interfaces=exclude_interfaces, subnet_cache=subnet_cache)
                if len(subnets) > 1:
                    raise NetworkException(f"Interface {self.get_system_name()} assigned to multiple subnets ({[repr(subnet) for subnet in subnets]})")
                if len(subnets) == 0:
                    return None
                _fill_cache(self, subnets[0])
                return subnets[0]
        return None

    def __repr__(self):
        return f"Interface {self.entity_id} ({self.ip_address_string} // {self.get_mac_address()})"
