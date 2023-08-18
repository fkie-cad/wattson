from typing import List, Union

import networkx
import pandapower as pp

from wattson.cosimulation.simulators.network.constants import MANAGEMENT_SWITCH


def get_powergrid_size(grid: pp.pandapowerNet) -> float:
    """
    Returns the size of the power grid as a single value
    :param grid: The pandapower grid
    :return: The grid size
    """
    sizes = {}
    for key in ["bus", "trafo", "sgen", "load", "line"]:
        sizes[key] = len(grid[key].index)
    size = sum(list(sizes.values()))
    return size


def get_ict_size(graph: dict) -> float:
    # Count hosts, switches and routers
    return len(graph["nodes"])


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