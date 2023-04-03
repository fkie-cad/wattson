from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class TapCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("tap", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            self.cli.invalid_command(command)
            return True

        cmd = command[0]

        if cmd == "list":
            devices = self.importer.get_tap_devices()
            for dev in devices:
                print(dev["dev"])
        elif cmd == "restart-all":
            devices = self.importer.get_tap_devices()
            print(f"Restarting tap devices")
            for dev in devices:
                print(f"... {dev}")
                self.importer.restart_tap_bridge(dev)
        elif cmd == "restart" and len(command) == 2:
            dev = command[1]
            self.importer.restart_tap_bridge(dev)
        else:
            self.cli.unknown_command(["tap", cmd])
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            "tap": {
                "list": None,
                "restart-all": None,
                "restart": {d["dev"]: None for d in self.importer.get_tap_devices()}
            }
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        if subcommand is not None and len(subcommand) == 1:
            if subcommand[0] == "list":
                return f"""
                {self.description(prefix)}
                
                List available tap devices
                """
            elif subcommand[0] == "restart":
                return f"""
                {self.description(prefix)}
                
                Restart the tap device management process.
                Usage: 'tap restart <tap-device>'
                """
        return f"""
        {self.description(prefix)}
        Usage: '{prefix[0]} <action> [<tap-device>]'

        Available actions: list, restart
        """

    def description(self, prefix: List[str]) -> str:
        return f"Manage inter-namespace tap devices"
