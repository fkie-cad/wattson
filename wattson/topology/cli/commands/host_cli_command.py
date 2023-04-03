from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class HostCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("host", self)

        self._available_commands = ["stop", "start", "restart", "info",
                                    "log", "logf",
                                    "cli", "term", "kfish", "pcap",
                                    "switch", "link"]

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            print("Host related commands. Use 'help host' for more details")
            return True
        if len(command) != 2:
            self.cli.invalid_command(command)
            print("Try 'help host' for details")
            return True

        n_command = [command[1], command[0]]
        return self.cli.handle_command(n_command)

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        command_dict = {cmd: None for cmd in self._available_commands}
        return {
            "host": self.cli.get_host_completion_dict(command_dict)
        }

    def description(self, prefix: List[str]) -> str:
        return "Host related commands"

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        if subcommand is None or len(subcommand) != 2:
            return f"""
            Host related commands.
            Usage: 'host <hostid> <command>'
            Available commands:
            {'  '.join(sorted(self._available_commands))}
            
            For help on individual host commands, use 'help <command>'
            """
        n_command = [subcommand[1], subcommand[0]]
        n_prefix, handler = self.cli.get_handler(n_command)
        if handler is None:
            self.cli.invalid_command(n_command)
            return "Use 'help host' for available host commands"
        return handler.help(n_prefix, n_command[len(n_prefix):])
