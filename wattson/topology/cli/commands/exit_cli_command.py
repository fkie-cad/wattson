from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class ExitCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("exit", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        print("Exiting")
        return False

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "exit": None
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return "Exit the Simulation"

    def description(self, prefix: List[str]) -> str:
        return "Exit the Simulation"
