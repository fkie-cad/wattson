import ipaddress

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.util.misc import deep_update


class YamlScenarioExtension(ScenarioExtension):
    def provides_pre_physical(self) -> bool:
        return True

    def extend_pre_physical(self):
        config = self.config
        logger = self.co_simulation_controller.logger.getChild("YamlScenarioExtension")
        network_emulator = self.co_simulation_controller.network_emulator

        # Nodes
        for node_id, node in config.get("nodes", {}).items():
            node_type = node.get("type")
            if node_type is None:
                logger.warning(f"Cannot handle node {node_id} as no node type is given")
                continue
            if network_emulator.has_entity(node_id):
                logger.warning(f"Cannot handle node {node_id} as it already exists")
                continue
            if node_type == "host":
                network_node = WattsonNetworkHost(id=node_id, config=node)
                network_emulator.add_host(network_node)
            elif node_type == "docker-host":
                network_node = WattsonNetworkDockerHost(id=node_id, config=node)
                network_emulator.add_host(network_node)
            elif node_type == "router":
                network_node = WattsonNetworkRouter(id=node_id, config=node)
                network_emulator.add_router(network_node)
            elif node_type == "switch":
                network_node = WattsonNetworkSwitch(id=node_id, config=node)
                network_emulator.add_switch(network_node)
            else:
                logger.warning(f"Cannot add node {node_id} of unknown type {node_type}")
                continue
            # Interfaces
            for i, interface in enumerate(node.get("interfaces", [])):
                ip = interface.get("ip")
                prefix_len = None
                mac = interface.get("mac")
                connect_to_node = None
                connect_to_id = interface.get("connect_to")
                if connect_to_id is not None:
                    connect_to_node = network_emulator.get_node(connect_to_id)

                if ip == "auto":
                    if node_type == "switch":
                        ip = None
                    else:
                        if connect_to_node is not None:
                            subnets = connect_to_node.get_subnets(include_management=False)
                            if len(subnets) == 0:
                                logger.warning(f"Cannot determine IP for {node_id}.{i}")
                                ip = None
                            else:
                                subnet = subnets[0]
                                prefix_len = subnet.prefixlen
                                ip = network_emulator.get_unused_ip(subnet)
                elif ip is not None:
                    ip = str(ip)
                    if "/" in ip:
                        parts = ip.split("/")
                        prefix_len = int(parts[1])
                        ip = parts[0]
                    ip = ipaddress.IPv4Address(ip)

                interface_config = {
                    "ip_address": ip,
                    "mac_address": mac,
                    "subnet_prefix_length": prefix_len
                }
                if connect_to_node is not None:
                    network_emulator.connect_nodes(network_node, connect_to_node, interface_a_options=interface_config)
                else:
                    network_emulator.add_interface(network_node, WattsonNetworkInterface(**interface_config))

                if isinstance(network_node, WattsonNetworkHost) and network_node.config.get("requires_internet_connection", False):
                    nat = network_emulator.add_nat_to_management_network()
                    nat.allow_traffic_from_host(network_node)
                    nat.set_internet_route(network_node)

        # Configuration
        configuration = config.get("config", {})
        if isinstance(configuration, dict):
            updated_configuration = deep_update(self.co_simulation_controller.configuration_store.get_configuration("configuration", {}), configuration)
            self.co_simulation_controller.configuration_store.register_configuration("configuration", updated_configuration)
