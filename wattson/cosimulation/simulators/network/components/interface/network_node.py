import abc
import ipaddress
from pathlib import Path
from typing import List, TYPE_CHECKING, Optional, Dict, Callable, Union, Tuple

from wattson.cosimulation.exceptions import ServiceNotFoundException
from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.services.wattson_service_interface import WattsonServiceInterface

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface


class NetworkNode(NetworkEntity, abc.ABC):

    @abc.abstractmethod
    def exec(self, cmd):
        ...

    def get_hostname(self) -> str:
        return self.entity_id

    @property
    def node_id(self):
        return self.prefix_id(self.id)

    @staticmethod
    def get_prefix():
        return "n"

    @staticmethod
    def prefix_id(node_id) -> str:
        if node_id[0] in "0123456789":
            return f"n{node_id}"
        return node_id

    @abc.abstractmethod
    def add_role(self, role: str):
        ...

    @abc.abstractmethod
    def delete_role(self, role: str):
        ...

    @abc.abstractmethod
    def get_config(self) -> dict:
        ...

    def get_role(self) -> Optional[str]:
        roles = self.get_roles()
        if len(roles) == 0:
            return None
        return roles[0]

    @abc.abstractmethod
    def get_roles(self) -> List[str]:
        ...

    def has_role(self, role: str) -> bool:
        return role in self.get_roles()

    def get_subnets(self, include_management: bool = False, exclude_interfaces: Optional[List['NetworkInterface']] = None) -> List[ipaddress.IPv4Network]:
        subnets = []
        if exclude_interfaces is None:
            exclude_interfaces = []
        for interface in self.get_interfaces():
            subnet = interface.get_subnet(include_management=include_management, exclude_interfaces=exclude_interfaces)
            if subnet is not None and subnet not in subnets:
                subnets.append(subnet)
        return subnets

    def has_subnet(self, subnet: ipaddress.IPv4Network) -> bool:
        """
        Checks whether this node has an interface that belongs to the given subnet.
        @param subnet: The subnet to check for
        @return: Whether this node lies within the given subnet
        """
        for existing_subnet in self.get_subnets():
            if existing_subnet.supernet_of(subnet):
                return True
        return False

    """
    INTERFACES
    """
    @abc.abstractmethod
    def add_interface(self, interface: 'NetworkInterface'):
        """
        Adds an Interface and links it to this node.
        :param interface: The interface instance
        :return:
        """
        ...

    @abc.abstractmethod
    def get_interfaces(self) -> List['NetworkInterface']:
        ...

    def get_interface(self, entity_id: str) -> Optional['NetworkInterface']:
        for interface in self.get_interfaces():
            if interface.entity_id == entity_id:
                return interface
        return None

    @abc.abstractmethod
    def start_pcap(self, interface: Optional['NetworkInterface'] = None) -> List['WattsonServiceInterface']:
        """
        Start a packet capture on this node for the given interface.
        If the interface is None, all packets for all interfaces are captured.
        @param interface: (Optional) interface to start the PCAP for
        @return: A list of services that represent the packet capturing processes.
        """
        ...

    @abc.abstractmethod
    def stop_pcap(self, interface: Optional['NetworkInterface'] = None):
        """
        Stops a packet capture on this node for the given interface.
        If the interface is None, packet captures for all interfaces are stopped.
        @param interface: (Optional) interface to stop the PCAP
        @return:
        """
        ...

    def get_ip_addresses(self) -> List[str]:
        """
        Returns a list of all IP addresses of this node.
        Each IP is formatted as a string, indicating the IP itself in decimal notation and the prefix length.
        E.g.: 192.168.0.1/24
        :return: A list of (formatted) IP addresses.
        """
        addresses = []
        for interface in self.get_interfaces():
            if interface.has_ip():
                addresses.append(interface.ip_address_string)
        return addresses

    def get_management_interface(self) -> Optional['NetworkInterface']:
        for interface in self.get_interfaces():
            if interface.is_management and interface.has_ip():
                return interface
        return None

    def is_pure_management_node(self) -> bool:
        """
        Returns whether this node is reachable, but only reachable via the management network.
        Nodes without interfaces are NOT pure management nodes
        @return: If the node is a pure management node
        """
        interfaces = self.get_interfaces()
        for interface in interfaces:
            if not interface.is_management:
                return False
        return len(interfaces) > 0

    def is_not_pure_management_node(self) -> bool:
        """
        Returns whether this node has any interface that is not a management interface.
        @return:
        """
        interfaces = self.get_interfaces()
        for interface in interfaces:
            if not interface.is_management:
                return True
        return False

    def get_neighbors(self) -> List['NetworkNode']:
        neighbors = []
        for interface in self.get_interfaces():
            link = interface.get_link()
            if link is not None:
                node = link.get_other_interface(interface).get_node()
                if node is not None:
                    neighbors.append(node)
        return neighbors

    def has_ip(self, ip: Union[ipaddress.IPv4Address, str]) -> bool:
        """
        Checks whether this node has an interface with the given IP address
        @param ip: The IP address to check for
        @return: True iff any interface of this node has the given IP address
        """
        if isinstance(ip, str):
            ip = ipaddress.IPv4Address(ip)
        for interface in self.get_interfaces():
            if interface.has_ip() and interface.get_ip_address() == ip:
                return True
        return False

    def get_management_ip_address_string(self, with_subnet_length: bool = True) -> Optional[str]:
        """
        Returns the IP address of the management interface of this node, if it exists.
        Otherwise, None is returned.
        :return: The node's management IP address, or None
        """
        management_interface = self.get_management_interface()
        if management_interface is None:
            return None
        if with_subnet_length:
            return management_interface.ip_address_string
        return management_interface.ip_address_short_string

    def get_primary_ip_address_string(self, with_subnet_length: bool = True) -> Optional[str]:
        """
        Returns the IP address of the first non-management interface of this node, if it exists.
        Otherwise, None is returned.
        :return: The node's first non-management IP address, or None
        """
        for interface in self.get_interfaces():
            if interface.has_ip() and not interface.is_management:
                if with_subnet_length:
                    return interface.ip_address_string
                return interface.ip_address_short_string
        return None

    """
    SERVICES
    """
    def has_services(self) -> bool:
        """
        Returns True iff the node's configuration already has a service section
        :return:
        """
        return len(self.get_services()) > 0

    @abc.abstractmethod
    def add_service(self, service: WattsonServiceInterface):
        """
        Adds a service to this node. This does not start the service.
        :param service: The service to add to this node.
        :return:
        """
        ...

    def get_service(self, service_id: int) -> WattsonServiceInterface:
        services = self.get_services()
        if service_id not in self.get_services():
            raise ServiceNotFoundException(f"Node {self.entity_id} does not have a service with ID {service_id}")
        return services[service_id]

    @abc.abstractmethod
    def get_services(self) -> Dict[int, WattsonServiceInterface]:
        ...

    def start_service(self, service_id: int):
        service = self.get_service(service_id)
        service.start()

    def start_services(self):
        for _, service in self.get_services().items():
            service.start()

    def stop_service(self, service_id: int, wait_seconds: float = 5, auto_kill: bool = False,
                     async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None):
        service = self.get_service(service_id)
        service.stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)

    def stop_services(self):
        for _, service in self.get_services().items():
            service.stop()

    def get_service_by_name(self, service_name: str) -> WattsonServiceInterface:
        for service_id, service in self.get_services().items():
            if service.name == service_name:
                return service
        raise ServiceNotFoundException(f"Node {self.entity_id} has no service with name {service_name}")

    def has_service(self, name_or_id: Union[int, str]) -> bool:
        if isinstance(name_or_id, int):
            return name_or_id in self.get_services()
        try:
            self.get_service_by_name(service_name=name_or_id)
        except ServiceNotFoundException:
            return False
        return True

    """
    Port mirroring
    """
    def enable_mirror(self, interface: 'NetworkInterface') -> bool:
        """
        Enable network mirroring at this node and use the given interface as mirror port
        @param interface: The interface to use as mirror port
        @return: Whether the action has been successfully performed
        """
        return False

    def disable_mirror(self, interface: 'NetworkInterface') -> bool:
        """
        Disable mirroring output to the given interface
        @param interface: The interface used as mirror port
        @return: Whether the action has been successfully performed
        """
        return False

    def clear_mirrors(self) -> bool:
        """
        Clear all existing mirror ports at this node
        @return: Whether the action has been successfully performed
        """
        return False

    """
    Interface control
    """
    def interface_set_mac(self, interface: 'NetworkInterface') -> bool:
        """
        Set the MAC / hardware address for the given interface
        @param interface: The interface to set the MAC address for
        @return: Whether the action has been successfully performed
        """
        return False

    def interface_up(self, interface: 'NetworkInterface') -> bool:
        """
        Set an interface up
        @param interface: The interface to set up
        @return: Whether the action has been successfully performed
        """
        return False

    def interface_down(self, interface: 'NetworkInterface') -> bool:
        """
        Set an interface down
        @param interface: The interface to set down
        @return: Whether the action has been successfully performed
        """
        return False

    def interface_flush_ip(self, interface: 'NetworkInterface') -> bool:
        """
        Flush (remove) the IP address of an interface
        @param interface: The interface to flush the IP address of
        @return: Whether the action has been successfully performed
        """
        return False

    def interface_set_ip(self, interface: 'NetworkInterface') -> bool:
        """
        Set the IP address for an interface
        @param interface: The interface to set the IP address of
        @return: Whether the action has been successfully performed
        """
        return False

    def interfaces_list_existing(self) -> List[Dict]:
        """
        Returns a list of actual interfaces existing on the node.
        Each interface information consists of the interface's name, its hardware address, ip addresses, and potential statistics.
        @return: A list of dictionaries containing interface information
        """
        return []

    """
    FILES
    """
    def file_get_contents(self, path: Path) -> Optional[str]:
        """
        Returns the contents of a file on the file system of this node.
        @param path: The path of the file to read.
        @return: The contents of the file or None if the file does not exist or is not readable.
        """
        return None

    def file_put_contents(self, path: Path, contents: str) -> Tuple[bool, Optional[Path]]:
        """
        Writes the given content string to the file on the file system of this node specified by path.
        Returns whether the operation succeeded.
        @param path: The path of the file to write to.
        @param contents: The contents of the file to write.
        @return: Whether the operation succeeded and the potentially transformed path of the target file.
        """
        return False, None
