import ipaddress
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from wattson.cosimulation.exceptions import *
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_docker_router import WattsonNetworkDockerRouter
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_link import WattsonNetworkLink
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.roles.ip_tables_firewall import IPTablesFirewall
from wattson.services.configuration import ServiceConfiguration
from wattson.services.management.wattson_webmin_service import WattsonWebminService

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator


class NetworkScenarioLoader:
    def load_scenario(self, scenario_path: Path, network_emulator: 'NetworkEmulator'):
        network_file = scenario_path.joinpath("network.yml")
        if not network_file.exists():
            raise InvalidScenarioException(f"Network configuration does not exist in {network_file}")
        with network_file.open("r") as f:
            network_data = yaml.load(f, Loader=yaml.SafeLoader)

        # Add Nodes
        for node_id, node in network_data["nodes"].items():
            self._check_node(node)
            node_type = node["type"]
            node_roles = node["roles"]
            if node_type == "host" and "router" in node_roles:
                if "firewall" in node_roles:
                    node["image"] = "wattson-router"
                    network_node = WattsonNetworkDockerRouter(id=node["id"], config=node)
                    network_emulator.add_router(network_node)
                    # Add webmin service
                    service = WattsonWebminService(service_configuration=ServiceConfiguration(), network_node=network_node)
                    network_node.add_service(service)
                else:
                    # Add default router
                    network_node = WattsonNetworkRouter(id=node["id"], config=node)
                    network_emulator.add_router(network_node)
            elif node_type == "host":
                network_node = WattsonNetworkHost(id=node["id"], config=node)
                network_emulator.add_host(network_node)
            elif node_type == "docker-host":
                network_node = WattsonNetworkDockerHost(id=node["id"], config=node)
                network_emulator.add_host(network_node)
            elif node_type == "switch":
                network_node = WattsonNetworkSwitch(id=node["id"], config=node)
                network_emulator.add_switch(network_node)
            else:
                raise InvalidNetworkNodeException(f"Unknown node type {node_type}")
            network_node.config = node.copy()
            for interface_config in node["interfaces"]:
                interface = WattsonNetworkInterface(id=interface_config["id"], node=network_node)
                if "ip" in interface_config and interface_config["ip"] != "":
                    ip_subnet = ipaddress.IPv4Network(interface_config["ip"], strict=False)
                    ip_address = ipaddress.IPv4Address(interface_config["ip"].split("/")[0])
                    interface.ip_address = ip_address
                    interface.subnet_prefix_length = ip_subnet.prefixlen
                if "mac" in interface_config:
                    interface.mac_address = interface_config["mac"]
                network_emulator.add_interface(network_node, interface)

            if isinstance(network_node, WattsonNetworkHost) and network_node.config.get("requires_internet_connection", False):
                nat = network_emulator.add_nat_to_management_network()
                nat.allow_traffic_from_host(network_node)
                nat.set_internet_route(network_node)

        # Links
        for link_id, link in network_data["links"].items():
            self._check_link(link)
            network_link = WattsonNetworkLink(id=link["id"], config=link)
            node_a_id, interface_a_id = link["interfaces"][0].split(".")
            node_b_id, interface_b_id = link["interfaces"][1].split(".")
            interface_a = network_emulator.get_interface(node_a_id, interface_a_id)
            interface_b = network_emulator.get_interface(node_b_id, interface_b_id)
            network_link.interface_a = interface_a
            network_link.interface_b = interface_b
            network_emulator.add_link(network_link)

    @staticmethod
    def _prefix_node_id(node_id: str) -> str:
        return WattsonNetworkNode.prefix_id(node_id)

    @staticmethod
    def _prefix_link_id(link_id: str) -> str:
        return WattsonNetworkLink.prefix_id(link_id)

    def _check_node(self, node: dict):
        if "id" not in node or len(node["id"]) == 0:
            raise InvalidNetworkNodeException("Node requires non-empty 'id'")
        if "name" not in node:
            node["name"] = node["id"]
        if "type" not in node:
            raise InvalidNetworkNodeException("Node requires 'type'")
        roles = []
        if "role" in node:
            roles.append(node["role"])
        if "roles" in node:
            roles.extend([role for role in node["roles"] if role not in roles])
        if len(roles) == 0:
            roles.append(node["type"])
        node["roles"] = roles
        if "interfaces" not in node:
            node["interfaces"] = []

    def _check_link(self, link: dict):
        if "id" not in link or len(link["id"]) == 0:
            raise InvalidNetworkLinkException("Link requires non-empty 'id'")
        # Prefix link ids containing numbers with l
        link["id"] = self._prefix_link_id(link["id"])
        if "interfaces" not in link:
            raise InvalidNetworkLinkException("Link requires interfaces")
        if len(link["interfaces"]) != 2:
            raise InvalidNetworkLinkException("Link requires exactly 2 interfaces")
        iface_0 = link["interfaces"][0].split(".")
        iface_1 = link["interfaces"][1].split(".")
        if len(iface_0) != 2:
            raise InvalidInterfaceException(f"Invalid interface ID: {'.'.join(iface_0)}")
        if len(iface_1) != 2:
            raise InvalidInterfaceException(f"Invalid interface ID: {'.'.join(iface_1)}")

