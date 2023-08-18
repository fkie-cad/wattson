from typing import Optional, List, TYPE_CHECKING

from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType

if TYPE_CHECKING:
    from wattson.cosimulation.cli.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class ShutdownCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("shutdown", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        print("Shutting down Wattson")
        resp = self.cli.wattson_client.query(WattsonQuery(
            query_type=WattsonQueryType.REQUEST_SHUTDOWN
        ))
        if resp.is_successful():
            print("Wattson is stopping, exiting CLI")
            return False
        print("Failed to shutdown Wattson")
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "shutdown": {"children": None, "description": "Shutdown and exit"}
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return "Shutdown Wattson"

    def description(self, prefix: List[str]) -> str:
        return "Shutdown Wattson"
