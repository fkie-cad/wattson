from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class MininetCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = cli.importer
        self.cli.register_command("mncli", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        from ipmininet.cli import IPCLI
        IPCLI(self.importer.mininet_manager.get_mininet())
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "mncli": None
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return self.description(prefix)

    def description(self, prefix: List[str]) -> str:
        return "Open the Mininet CLI"
