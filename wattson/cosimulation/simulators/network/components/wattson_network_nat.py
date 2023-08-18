import dataclasses
import ipaddress
from pathlib import Path
from typing import ClassVar

from wattson.cosimulation.simulators.network.components.interface.network_nat import NetworkNAT
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkNAT(WattsonNetworkHost, NetworkNAT):
    class_id: ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()
        self._allowed_hosts = set()
        self._queued_host_routes = []
        self._forward_state = 0
        self._internet_networks = None

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
        _, lines = self.exec("sysctl -n net.ipv4.ip_forward")
        self._forward_state = lines[0].strip()
        self.block_all_traffic()
        self.update_allowed_hosts()
        self.exec("sysctl net.ipv4.ip_forward=1")
        for host in self._queued_host_routes:
            self.set_internet_route(for_host=host)
        self._queued_host_routes = []

    def stop(self):
        self.block_all_traffic()
        # Reset forwarding
        self.exec(f"sysctl net.ipv4.ip_forward={self._forward_state}")
        super().stop()

    def update_default_route(self) -> bool:
        return False

    def allow_all_traffic(self):
        for host in self.network_emulator.get_hosts():
            self.allow_traffic_from_host(host)

    def update_allowed_hosts(self):
        for host_id in self._allowed_hosts:
            host = self.network_emulator.get_host(host_id)
            self._allow_traffic_from_host(host)

    def allow_traffic_from_host(self, host: WattsonNetworkHost):
        if host.entity_id in self._allowed_hosts:
            return
        self._allowed_hosts.add(host.entity_id)
        self._allow_traffic_from_host(host)

    def block_traffic_from_host(self, host: WattsonNetworkHost):
        if host.entity_id not in self._allowed_hosts:
            return
        self._allowed_hosts.remove(host.entity_id)
        self._block_traffic_from_host(host)

    def _block_traffic_from_host(self, host: WattsonNetworkHost):
        """
        Adjust iptables to block traffic
        @param host:
        @return:
        """
        if not self.is_started:
            return
        nat_interface = self.get_nat_interface().interface_name
        for interface in host.get_interfaces():
            ip = interface.ip_address_short_string
            self.exec(f"iptables -D FORWARD -i {nat_interface} -s {ip}")
            self.exec(f"iptables -D FORWARD -i {nat_interface} -d {ip}")
            self.exec(f"iptables -t nat -D POSTROUTING -s {ip} '!' -d {ip} -j MASQUERADE")

    def _allow_traffic_from_host(self, host: WattsonNetworkHost):
        """
        Adjust iptables to allow traffic
        @param host:
        @return:
        """
        if not self.is_started:
            return
        nat_interface = self.get_nat_interface().interface_name
        self.logger.info(f"Allowing NAT traffic from {host.entity_id} ({host.system_name})")
        for interface in host.get_interfaces():
            if not interface.has_ip():
                continue
            ip = interface.ip_address_short_string
            self.exec(f"iptables -I FORWARD -i {nat_interface} -d {ip} -j DROP")
            self.exec(f"iptables -A FORWARD -i {nat_interface} -s {ip} -j ACCEPT")
            self.exec(f"iptables -A FORWARD -o {nat_interface} -d {ip} -j ACCEPT")
            self.exec(f"iptables -t nat -A POSTROUTING -s {ip} '!' -d {ip} -j MASQUERADE")

    def block_all_traffic(self):
        if not self.is_started:
            return
        # Clear NAT rules
        self.exec(f"iptables -D FORWARD -i {self.get_nat_interface().interface_name}")
        for entity_id in self._allowed_hosts:
            host = self.network_emulator.get_host(entity_id)
            self._block_traffic_from_host(host)

    def set_internet_route(self, for_host: WattsonNetworkHost):
        if not self.is_started:
            self._queued_host_routes.append(for_host)
            return
        # Add route
        self.logger.info(f"Setting NAT route for {for_host.entity_id}")
        if self._internet_networks is None:
            self._derive_internet_networks()

        nat_ip = self.get_nat_interface().ip_address_short_string

        for network in self._internet_networks:
            net_id = f"{network.network_address}/{network.prefixlen}"
            for_host.exec(f"ip route add {net_id} via {nat_ip}")

        if for_host.__class__ == WattsonNetworkHost:
            namespace = for_host.get_namespace()
            self.exec(f"mkdir -p /etc/netns/{namespace.name}")
            dns = "nameserver 8.8.8.8"
            with Path(f"/etc/netns/{namespace.name}/resolv.conf").open("w") as f:
                f.write(dns)

    def clear_internet_route(self, for_host: WattsonNetworkHost):
        if self._internet_networks is None:
            self._derive_internet_networks()

        for network in self._internet_networks:
            net_id = f"{network.network_address}/{network.prefixlen}"
            for_host.exec(f"ip route del {net_id}")

    def _derive_internet_networks(self):
        networks = self.network_emulator.get_all_networks()
        internet_networks = [ipaddress.IPv4Network("0.0.0.0/0")]
        for network in networks:
            refined_internet = []
            for inet in internet_networks:
                if network.overlaps(inet):
                    refined_internet.extend(inet.address_exclude(network))
                else:
                    refined_internet.append(inet)
            internet_networks = refined_internet
        internet_networks = [network for network in internet_networks if network.is_global]
        self._internet_networks = internet_networks

    def get_nat_interface(self) -> WattsonNetworkInterface | None:
        for interface in self.get_interfaces():
            return interface
        return None

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__,
        })
        return d
