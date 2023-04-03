from typing import TYPE_CHECKING, List, Optional, Dict, Any

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class SwitchCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("switch", self)
        self._available_commands = ["list", "stop", "pcap", "interfaces"]

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            print("Switch control related commands. Use 'help switch' for more details")
            return False
        elif command[0] == "list":
            for switch in self.cli.importer.get_switches():
                print(switch["id"])
        elif len(command) == 2:
            if command[1] == "stop":
                switch = self.cli.importer.host_manager.get_net_host(command[0])
                switch.stop()
            elif command[1] == "pcap":
                switch = self.cli.importer.get_node(command[0])
                if switch is None:
                    print(f"Switch {command[0]} does not exist.")
                    return True
                self.cli.importer.deployment.start_pcap(switch)
            elif command[1] == "interfaces":
                switch = self.cli.importer.get_node(command[0])
                if switch is None:
                    print(f"Switch {command[0]} does not exist.")
                    return True
                bridge = self.cli.importer.ghn(switch)
                cmd = f"ovs-vsctl list-ports {bridge}"
                code, output = self.cli.importer.exec_with_output(cmd)
                if code != 0:
                    print("Could not get interfaces")
                else:
                    for line in output:
                        print(line)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        _cmds = self._available_commands[1:]
        command_dict = {cmd: None for cmd in _cmds}
        switches = [switch["id"] for switch in self.cli.importer.get_switches()]
        switch_command_dict = {sid: command_dict for sid in switches}
        switch_command_dict["list"] = None
        return {
            "switch": switch_command_dict
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> str:
        if not subcommand:
            return f"Control switch properties.\nPossible commands: {self._available_commands}"
        elif subcommand == ["list"]:
            return "List all links.\ne.g. link list"
        elif subcommand == ["stop"]:
            return "Stop a switch.\ne.g. switch 'switch_id' stop"
        elif subcommand == ["pcap"]:
            return "Start a PCAP recording at this switch: switch 'switch_id' pcap"

    def description(self, prefix: List[str]) -> str:
        return "Control switches"
