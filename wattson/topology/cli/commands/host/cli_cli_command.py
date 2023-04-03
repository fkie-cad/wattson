from typing import Optional, List, TYPE_CHECKING

from wattson.topology.cli.host_cli_starter import HostCLIStarter

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class CliCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("cli", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) != 1:
            self.cli.invalid_command(command)
            return True
        host_id = command[0]
        host = self.cli.importer.get_node(host_id)
        if host is None:
            print(f"Unknown host: {host_id}")
            return True

        hostname = self.importer.host_manager.get_hostname(host)
        ip = self.importer.utils.get_host_management_ip(host).split("/")[0]
        HostCLIStarter(self.importer, ip=ip, name=hostname)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            prefix[0]: self.cli.get_host_completion_dict()
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return f"""
        {self.description(prefix)}
        Usage: '{prefix[0]} <hostid>'
        """

    def description(self, prefix: List[str]) -> str:
        return "Open a Wattson CLI for the running process on a host"
