from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class HostsCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("hosts", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) != 1:
            self.cli.invalid_command(command)
            return True

        cmd = command[0]

        if cmd == "list":
            hosts = self.importer.get_hosts()
            for i, host in enumerate(hosts):
                print(str(i).rjust(3, " ") + ": " + host["id"])
        elif cmd == "stop":
            hosts = self.importer.get_hosts()
            for h in hosts:
                self.importer.deployment.host_stop(h)
        elif cmd == "start":
            hosts = self.importer.get_hosts()
            for h in hosts:
                if not self.importer.deployment.host_is_running(h):
                    self.importer.deployment.host_start(h)
        elif cmd == "restart":
            hosts = self.importer.get_hosts()
            print("Stopping...")
            for h in hosts:
                print(f"{h['id']}", end="  ", flush=True)
                self.importer.deployment.host_stop(h)
            print("")
            print("Starting...")
            for h in hosts:
                print(f"{h['id']}", end="  ", flush=True)
                self.importer.deployment.host_start(h)
            print("")
        else:
            self.cli.unknown_command(["hosts", cmd])
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "hosts": {
                "list": None,
                "start": None,
                "stop": None,
                "restart": None
            }
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return f"""
        {self.description(prefix)}
        Usage: {prefix[0]} <action>
        
        Available actions: list, start, stop, restart
        """

    def description(self, prefix: List[str]) -> str:
        return f"Control and monitor multiple hosts at once"
