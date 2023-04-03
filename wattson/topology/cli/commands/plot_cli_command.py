from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler
from networkx import Graph, write_graphml


class PlotCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = cli.importer
        self.cli.register_command("plot", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        self._plot_topology()
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "plot": None
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return self.description(prefix)

    def description(self, prefix: List[str]) -> str:
        return "Export the network topology as graphml"

    def _plot_topology(self):
        topo = self.importer.get_topology()
        graph: Graph = topo.convertTo(Graph, False, False)
        for id, e in enumerate(graph.edges()):
            f = e[0]
            t = e[1]
            graph[f][t].update({"id": id})
        print("Writing graph...")
        write_graphml(graph, "topology.graphml")
        print("GraphML written to 'topology.graphml'")