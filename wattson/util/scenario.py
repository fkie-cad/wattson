from typing import List, Union

import networkx
import pandapower as pp

from wattson.cosimulation.simulators.network.components.interface.network_link import NetworkLink
from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode
from wattson.cosimulation.simulators.network.constants import MANAGEMENT_SWITCH
from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator


def get_powergrid_size(grid: pp.pandapowerNet) -> float:
    """
    Returns the size of the power grid as a single value

    Args:
        grid (pp.pandapowerNet):
            The pandapower grid

    Returns:
        float: The grid size
    """
    sizes = {}
    for key in ["bus", "trafo", "sgen", "load", "line"]:
        sizes[key] = len(grid[key].index)
    size = sum(list(sizes.values()))
    return size


def get_ict_size(graph: dict) -> float:
    # Count hosts, switches and routers
    return len(graph["nodes"])


def get_network_statistics(network: NetworkEmulator) -> dict:
    def is_management_only(link_or_node: Union[NetworkLink, NetworkNode]) -> bool:
        if isinstance(link_or_node, NetworkLink):
            i_a = link_or_node.get_interface_a()
            i_b = link_or_node.get_interface_b()
            if i_a is not None:
                if i_a.is_management:
                    return True
            if i_b is not None:
                if i_b.is_management:
                    return True
            return False
        elif isinstance(link_or_node, NetworkNode):
            if len(link_or_node.get_interfaces()) == 0:
                return False

            for interface in link_or_node.get_interfaces():
                if not interface.is_management:
                    return False
            return True

    num_nodes = len([node for node in network.get_nodes() if not is_management_only(node)])
    num_hosts_all = len([node for node in network.get_hosts() if not is_management_only(node)])
    num_routers = len([node for node in network.get_routers() if not is_management_only(node)])
    num_hosts = num_hosts_all - num_routers
    num_switches = len([node for node in network.get_switches() if not is_management_only(node)])
    num_rtus = len([node for node in network.find_nodes_by_role("rtu") if not is_management_only(node)])
    num_links = len([link for link in network.get_links() if not is_management_only(link)])

    num_links_mgm = len([link for link in network.get_links() if is_management_only(link)])
    num_nodes_mgm = len([link for link in network.get_nodes() if is_management_only(link)])

    return {
        "nodes": num_nodes,
        "hosts_all": num_hosts_all,
        "routers": num_routers,
        "hosts": num_hosts,
        "switches": num_switches,
        "rtus": num_rtus,
        "links": num_links,
        "management_links": num_links_mgm,
        "management_nodes": num_nodes_mgm
    }


def get_ict_graph(graph: dict, include_management: bool = False) -> networkx.Graph:
    g = networkx.Graph()
    for node_id in graph["nodes"].keys():
        if not include_management and MANAGEMENT_SWITCH in node_id:
            continue
        g.add_node(node_id)
    for link in graph["links"]:
        source_node = link["interfaces"][0].split(".")[0]
        target_node = link["interfaces"][1].split(".")[0]
        if g.has_node(source_node) and g.has_node(target_node):
            g.add_edge(source_node, target_node)
    return g


def get_ict_nodes_by_type(graph: dict, node_type: Union[str, List[str]]) -> list:
    if type(node_type) == str:
        node_type = [node_type]
    return [n for n in graph["nodes"].values() if n["type"] in node_type]


def get_ict_stats(graph: dict) -> dict:
    stats = {}

    def _nodes_by_type(_types) -> int:
        return len(get_ict_nodes_by_type(graph, _types))

    stats["nodes"] = len(graph["nodes"])
    stats["links"] = len(graph["links"])
    stats["switches"] = _nodes_by_type("switch")
    stats["routers"] = _nodes_by_type("router")
    stats["hosts"] = _nodes_by_type(["host", "rtu", "mtu", "field", "attacker"])
    stats["rtus"] = _nodes_by_type("rtu")
    stats["mtus"] = _nodes_by_type("mtu")
    return stats


def get_data_point_size(data_points: dict) -> float:
    size = 0
    for device, points in data_points.items():
        size += len(points)
    return size


def get_data_point_mtu_size(graph: dict, data_points: dict) -> float:
    size = 0
    mtus = [n["id"] for n in get_ict_nodes_by_type(graph, "mtu")]
    for device, points in data_points.items():
        if device in mtus:
            size += len(points)
    return size