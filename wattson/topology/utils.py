from typing import Union, TYPE_CHECKING
if TYPE_CHECKING:
    import wattson

import ipaddress
from wattson.topology.constants import DEFAULT_LINK_LOSS, DEFAULT_LINK_JITTER, DEFAULT_LINK_DELAY, \
    DEFAULT_LINK_DATARATE


class TopologyUtils:
    def __init__(self, importer: "wattson.topology.network_manager.NetworkManager", namespace: str):
        self.namespace = namespace
        self.importer = importer

    def get_any_switch(self):
        """
        Get any switch of the network or topology.
        :return: The first found switch of the network or topology.
        """
        return self.importer.get_switches()[0]

    def get_linkopts(self, link: dict) -> dict:
        bw = link["data-rate"] if "data-rate" in link else DEFAULT_LINK_DATARATE
        delay = link["delay"] if "delay" in link else DEFAULT_LINK_DELAY
        jitter = link["jitter"] if "jitter" in link else DEFAULT_LINK_JITTER
        loss = link["loss"] if "loss" in link else DEFAULT_LINK_LOSS

        linkopts = {
            "delay": delay,
            "bw": self.importer.mininet_manager.parse_bandwidth(bw),
            "loss": loss,
            "jitter": jitter
        }
        return linkopts

    def get_iface(self, iface: str):
        hid, iid = self.parse_interface(iface)
        return self.get_host_interface(hid, iid)

    def get_iface_ip(self, iface: str):
        interface = self.get_iface(iface)
        if interface is not None and "ip" in interface:
            return interface["ip"]
        return None

    def get_iface_mac(self, iface: str):
        hid, iid = self.parse_interface(iface)
        interface = self.get_iface(iface)
        if interface is not None and "mac" in interface:
            return interface["mac"]
        if not self.importer.config.get("deterministic_mac", False):
            return None
        # Calculate MAC
        return self.importer.get_deterministic_mac(hid, iid)

    def get_host_interface(self, host: Union[str, dict], iid: str):
        h_id = host
        host = self.importer.get_node(host)
        if host is None:
            self.importer.logger.error(f"Host {h_id} does not exist!")
            return None
        for iface in host["interfaces"]:
            if iface["id"] == iid:
                return iface
        return None

    def get_primary_host_ip(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        ips = self.get_host_ips(host)
        if len(ips) == 0:
            return None
        return ips[0]

    def get_primary_host_ips(self):
        ips = {}
        for node in self.importer.get_nodes():
            primip = self.get_primary_host_ip(node)
            if primip is not None:
                ips[node["id"]] = primip.split("/")[0]
        return ips

    def get_management_ips(self):
        ips = {}
        for node in self.importer.get_nodes():
            ip = self.get_host_ip_for_target(node, self.importer.get_main_management_switch())
            if ip is not None:
                ips[node["id"]] = ip.split("/")[0]
        return ips

    def get_management_network(self) -> str:
        return self.importer.management_network

    def get_host_ip_for_target(self, host: Union[str, dict], target: Union[str, dict]):
        host = self.importer.get_node(host)
        target = self.importer.get_node(target)
        hid = host["id"]
        tid = target["id"]
        links = self.importer.graph["links"]

        for link in links:
            ifaces = link["interfaces"]
            nleft, ileft = self.parse_interface(ifaces[0])
            nright, iright = self.parse_interface(ifaces[1])
            hiface = None
            if nleft == hid and nright == tid:
                hiface = ifaces[0]
            if nright == hid and nleft == tid:
                hiface = ifaces[1]
            if hiface is not None:
                hip = self.get_iface_ip(hiface)
                if hip is not None:
                    return hip
        return None

    def get_used_ips(self):
        ifaces = [
            node["interfaces"] for i, node in self.importer.graph["nodes"].items()
        ]

        flatten = [item for sublist in ifaces for item in sublist]

        return [iface["ip"].split("/")[0] for iface in flatten if "ip" in iface]

    def get_host_ip(self, host):
        return self.get_primary_host_ip(host)

    def get_host_management_ip(self, host):
        ip = self.get_host_ip_for_target(host, self.importer.get_main_management_switch())
        if ip is not None:
            return ip.split("/")[0]

        return None

    def get_switch_for_subnet(self, ip: str):
        """
        Searches the network for a valid attachment point for a host with a specific
        IP address.
        Returns the host id of a switch in case of success, False otherwise.
        Note that the host id might vary from the Mininet hostname.
        Use get_hostname on the return value of this method to receive the hostname.
        """
        if "/" in ip:
            ip = ip.split("/")[0]
        ip = ipaddress.ip_address(ip)

        nodes = self.importer.graph["nodes"]

        for nid, node in nodes.items():
            if node["type"] == "switch":
                hosts = self.get_connected_hosts(node)
                for host in hosts:
                    hip = self.get_host_ip_for_target(host, nid)
                    if hip is not None:
                        subnet = ipaddress.ip_network(hip, strict=False)
                        if ip in subnet:
                            return nid

        return False

    def get_connected_hosts(self, host: Union[str, dict]):
        nodes = self.importer.graph["nodes"]
        links = self.importer.graph["links"]
        host = self.importer.get_node(host)
        hid = host["id"]
        hosts = []
        for link in links:
            nleft, ileft = self.parse_interface(link["interfaces"][0])
            nright, iright = self.parse_interface(link["interfaces"][1])
            if nleft == hid:
                hosts.append(nodes[nright])
            if nright == hid:
                hosts.append(nodes[nleft])
        return hosts

    def gen_iface_id(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        ifaces = host["interfaces"]
        iids = [i["id"] for i in ifaces]
        inum = len(ifaces) + 1
        iid = f"i{inum}"
        while iid in iids:
            inum += 1
            iid = f"i{inum}"
        return iid

    def gen_link_id(self):
        links = self.importer.graph["links"]
        lids = [link["id"] for link in links]
        lnum = len(links) + 1
        lid = f"l{lnum}"
        while lid in lids:
            lnum = len(links) + 1
            lid = f"l{lnum}"
        return lid

    def get_ip_for_switch_subnet(self, switch: Union[str, dict], exclude=None):
        """
        Based on the hosts attached to the given switch, determines an unused IP address
        for a new host to be attached to the switch to use.
        :param switch: The switch that the new host should be attached to
        :param exclude: A list of host IDs to exclude from traversal
        :return: The IP address to use for the new host
        """
        if exclude is None:
            exclude = []
        is_traversal_root = len(exclude) == 0
        switch = self.importer.get_node(switch)
        hosts = self.get_connected_hosts(switch)
        ip = self.get_ip_of_host_at_switch(exclude, hosts, switch)

        if is_traversal_root and ip is None:
            raise RuntimeError("Could no to determine the IP subnet for the given Switch")

        if ip is None:
            return None

        #ip = self._add_subnetmask_to_ip(ip)
        used_ips = self.get_used_ips()
        #subnet = ipaddress.ip_network(ip, strict=False)
        subnet = self.importer.get_network(ip)
        for possible_ip in subnet.hosts():
            if str(possible_ip) not in used_ips:
                return TopologyUtils.ipaddress_to_string_with_subnetmask(possible_ip, subnet)
        raise RuntimeError("Could not determine a usable IP address")

    def _add_subnetmask_to_ip(self, ip):
        if "/" not in str(ip):
            ip = f"{str(ip)}/{self.importer.config['subnet_prefix_length']}"
        return ip

    def get_ip_of_host_at_switch(self, exclude, hosts, switch):
        """
        Given a switch and the set of hosts or nodes connected to this switch,
        get the IP address of the interface of any host that connects to the given switch
        or a switch connected to the given switch.
        :param exclude: A list of nodeids to exclude from the traversal
        :param hosts: The list of hosts attached to the switch
        :param switch: The switch to start the traversal
        :return: The IP address of any host's interface connected to the switch.
        """
        ip = None
        for host in hosts:
            if host["id"] in exclude:
                continue
            if host["type"] == "switch":
                exclude.append(host["id"])
                ip = self.get_ip_for_switch_subnet(host, exclude)
            else:
                ip = self.get_host_ip_for_target(host, switch)
            if ip is not None:
                break
        return ip

    @staticmethod
    def get_host_ips(host: dict):
        """
        Returns a list of all IP addresses (x.x.x.x/x) of the given host
        """
        ips = []
        interfaces = []

        if "interfaces" in host:
            interfaces += host["interfaces"]
        if "tap_interfaces" in host:
            interfaces += host["tap_interfaces"]

        for iface in interfaces:
            if "ip" in iface and iface["ip"] != "":
                ips.append(iface["ip"])
        return ips

    @staticmethod
    def parse_interface(iface: str):
        iinfo = iface.split(".")
        assert len(iinfo) == 2
        return iinfo[0], iinfo[1]

    @staticmethod
    def ipaddress_to_string_with_subnetmask(possible_ip, subnet):
        return f"{str(possible_ip)}/{subnet.prefixlen}"

    @staticmethod
    def get_node_type(node):
        if node["type"] == "switch":
            return "switch"
        if node["type"] == "router":
            return "router"
        return "host"
