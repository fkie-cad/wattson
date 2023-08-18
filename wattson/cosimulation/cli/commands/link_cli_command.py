from typing import TYPE_CHECKING, List, Optional

from wattson.cosimulation.exceptions import NetworkEntityNotFoundException
from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator

if TYPE_CHECKING:
    from wattson.cosimulation.cli.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class LinkCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("link", self)
        self.remote_network_emulator = RemoteNetworkEmulator.get_instance(wattson_client=self.cli.wattson_client)
        self._available_commands = [
            "info", "down", "up", "remove", "modify", "find", "list"
        ]

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            print("Link control related commands. Use 'help link' for more details")
            return False
        elif command[0] == "info":
            link = self.get_link(command[1])
            if link is None:
                print("Requested link not found")
                return True
            self._print_link_info(link, short=False)
        elif command[0] == "down" or command[0] == "up":
            link = self.get_link(command[1])
            if link is None:
                print("Requested link not found")
                return True
            if command[0] == "up":
                print("Bringing link up")
                link.up()
            elif command[0] == "down":
                print("Bringing link down")
                link.down()
        elif command[0] == "remove":
            link = self.get_link(command[1])
            if link is None:
                print("Requested link not found")
                return True
            print(f"Removing link {link.entity_id}")
            self.remote_network_emulator.remove_link(link)
        elif command[0] == "modify":
            if len(command) != 4:
                print("Invalid query. Use link modify $link_id $property $value")
                return True
            link = self.get_link(command[1])
            if link is None:
                print("Requested link not found")
                return True
            p = command[2]
            value = command[3]
            query = WattsonNetworkQuery(
                query_type=WattsonNetworkQueryType.SET_LINK_PROPERTY,
                query_data={
                    "entity_id": link.entity_id,
                    "property_name": p,
                    "property_value": value
                }
            )
            response = self.cli.wattson_client.query(query=query)
            if response.is_successful():
                print("Property set")
                return True
            error = response.data.get("error")
            print(f"Could not set property: {error=}")
            return True
        elif command[0] == "list":
            for link in self.remote_network_emulator.get_links():
                self._print_link_info(link, short=True)

        elif command[0] == "find":
            print("NIY")
        return True

    def get_link(self, entity_id: str) -> Optional[RemoteNetworkLink]:
        try:
            return self.remote_network_emulator.get_link(link=entity_id)
        except NetworkEntityNotFoundException:
            return None

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        links = self.remote_network_emulator.get_links()
        link_completion_dict = {
            command: {
                "children": {
                    link.entity_id: {"children": None, "description": self.get_link_connection_string(link)}
                    for link in links
                },
                "description": None
            } for command in self._available_commands
        }
        link_completion_dict["list"] = {"children": None, "description": "List all available links"}

        # Modify dict
        properties = ["bandwidth", "delay", "jitter", "packet_loss"]
        property_dict = {
            p: {
                "children": None,
                "description": None
            } for p in properties
        }
        for link_dict in link_completion_dict["modify"]["children"].values():
            link_dict["children"] = property_dict

        return {
            "link": {
                "children": link_completion_dict,
                "description": "Network link management"
            }
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> str:
        if not subcommand:
            return f"Control link properties.\nPossible commands: {self._available_commands}"
        elif subcommand == ["down"]:
            return "Turn a link down.\ne.g. link down 'link_id'"
        elif subcommand == ["up"]:
            return "Turn a link up.\ne.g. link up 'link_id'"
        elif subcommand == ["remove"]:
            return "Remove a link from the network.\ne.g. link remove 'link_id'"
        elif subcommand == ["modify"]:
            return "Modify a property of the link.\ne.g. link modify 'link_id' [bandwidth, jitter, delay, packet_loss] 'new_value'"
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

    @staticmethod
    def get_link_connection_string(link: RemoteNetworkLink):
        return f"{link.get_interface_a().get_node().display_name} <-> {link.get_interface_b().get_node().display_name}"

    @staticmethod
    def _print_link_info(link: RemoteNetworkLink, short: bool = False):
        if short:
            print(f"{link.entity_id} ({LinkCliCommand.get_link_connection_string(link)})")
        else:
            interface_a = link.get_interface_a()
            interface_b = link.get_interface_b()
            node_a = interface_a.get_node()
            node_b = interface_b.get_node()
            print(f"Link {link.entity_id} // {link.system_name}")
            print(f"    Node A:      {node_a.display_name} ({node_a.entity_id})")
            print(f"    Node B:      {node_b.display_name} ({node_b.entity_id})")
            print(f"    Interface A: {interface_a.display_name}  ({interface_a.entity_id} // {interface_a.get_system_name()})")
            print(f"    Interface B: {interface_b.display_name}  ({interface_b.entity_id} // {interface_b.get_system_name()})")
            print(f"    Link Model")
            print(f"        Bandwidth:  {link.get_link_model().bandwidth_mbps} Mbps")
            print(f"        Delay:      {link.get_link_model().delay_ms} ms")
            print(f"        Jitter:     {link.get_link_model().jitter_ms} ms")
            print(f"        PacketLoss: {link.get_link_model().packet_loss_percent} %")
            print(f"    Link State")
            print(f"        Is Up:      {link.is_up()}")
            for key, value in link.get_link_state()["state"].items():
                print(f"        {key}: {repr(value)}")
