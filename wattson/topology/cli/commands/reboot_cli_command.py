from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler
from networkx import Graph, write_graphml


class RebootCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = cli.importer
        self.cli.register_command("reboot", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        self.importer.request_restart()
        return False

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "reboot": None
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return self.description(prefix)

    def description(self, prefix: List[str]) -> str:
        return "Restart the whole simulation"
