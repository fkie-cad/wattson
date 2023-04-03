from typing import TYPE_CHECKING, List, Optional, Dict, Any
from wattson.topology.utils import TopologyUtils

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class LinkCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("link", self)
        self.utils = TopologyUtils(self.cli.importer, self.cli.importer.namespace)
        self._available_commands = [
            "down", "up", "remove", "stop", "modify", "find", "list"
        ]

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            print("Link control related commands. Use 'help link' for more details")
            return False
        elif command[0] == "down" or command[0] == "up":
            self.cli.importer.link_manager.set_link_status(command[1], command[0])
        elif command[0] == "remove":
            self.cli.importer.link_manager.remove_link(command[1])
        elif command[0] == "stop":
            self.cli.importer.link_manager.stop_link(command[1])
        elif command[0] == "modify":
            value = float(command[3]) if command[2] == "loss" else command[3]
            if command[2] == "delay" and "ms" not in command[3]:
                print("Delay value needs to be specified in ms.")
            if command[2] == "bw" and "bps" not in command[3]:
                print("Bandwidth value needs to be specified in bps/kbps/mbps/gbps.")
            self.cli.importer.link_manager.set_link_parameter(command[1], command[2], value)
        elif command[0] == "list":
            for link in self.cli.importer.get_links():
                print(link)
        elif command[0] == "find":
            if len(command) == 3:
                connections = self.cli.importer.link_manager.find_links(command[1], command[2])
            else:
                connections = self.cli.importer.link_manager.find_links(command[1])
            for connection in connections:
                print(connection.link)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {"link": self.cli.get_link_completion_dict(self._available_commands)}

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> str:
        if not subcommand:
            return f"Control link properties.\nPossible commands: {self._available_commands}"
        elif subcommand == ["down"]:
            return "Turn a link down.\ne.g. link down 'link_id'"
        elif subcommand == ["up"]:
            return "Turn a link up.\ne.g. link up 'link_id'"
        elif subcommand == ["remove"]:
            return "Remove a link from the network.\ne.g. link remove 'link_id'"
        elif subcommand == ["stop"]:
            return "Stop a link.\ne.g. link stop 'link_id'"
        elif subcommand == ["modify"]:
            return "Modify a property of the link.\ne.g. link modify 'link_id' [bw, jitter, delay, loss] 'new_value'"
        elif subcommand == ["list"]:
            return "List all links in the network as dictionaries.\ne.g. link list"
        elif subcommand == ["find"]:
            return "Returns all links either:\n" \
                   "1. From one node to all its neighbors\n" \
                   "2. From one node to another\n" \
                   "e.g. link find 'node_id'\n" \
                   "e.g. link find 'node_id1' 'node_id2'"

    def description(self, prefix: List[str]) -> str:
        return "Control link properties"
