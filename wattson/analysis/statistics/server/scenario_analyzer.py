import networkx

from wattson.analysis.statistics.common.statistic_message import StatisticMessage
from wattson.util.scenario import get_ict_stats, get_ict_nodes_by_type, get_ict_graph
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.analysis.statistics.server.statistic_server import StatisticServer


class ScenarioAnalyzer:
    def __init__(self, stat_server, power_net, network, data_points):
        self.stat_server: 'StatisticServer' = stat_server
        self.power_net = power_net
        self.network = network
        self.data_points = data_points

    def analyze(self):
        self._analyze_power_grid()
        self._analyze_ict_network()
        self._analyze_data_points()

    def _analyze_power_grid(self):
        pass

    def _analyze_ict_network(self):
        # General stats
        stats = get_ict_stats(self.network)
        for key, value in stats.items():
            msg = StatisticMessage(host="analyzer", event_class="ict.statistic", event_name=key, value=value)
            self.stat_server.log(msg)

        def log_hop_count(source, target, hops):
            msg = StatisticMessage(host="analyzer", event_class="ict.hops",
                                   event_name=f"{source}->{target}", value=hops)
            self.stat_server.log(msg)

        # RTU Hop Count
        graph = get_ict_graph(self.network)
        mtus = get_ict_nodes_by_type(self.network, "mtu")
        for mtu in mtus:
            # Get associated RTUs
            mtu_id = mtu["id"]
            rtu_ids = mtu["rtu_ips"].keys()
            for rtu_id in rtu_ids:
                hop_count = networkx.shortest_path_length(graph, mtu_id, rtu_id)
                log_hop_count(mtu_id, rtu_id, hop_count)

    def _analyze_data_points(self):
        pass
