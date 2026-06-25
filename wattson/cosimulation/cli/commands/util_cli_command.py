import json
import time
from typing import Optional, List, TYPE_CHECKING, Dict, Any, Union

import tabulate

from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.services.wattson_remote_service import WattsonRemoteService

if TYPE_CHECKING:
    from wattson.cosimulation.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class UtilCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self._nodes: Optional[Dict] = None
        self.cli.register_command("util", self)

        self._info_timestamp = 0
        self._info_timeout_seconds = 20
        self.time = self.cli.wattson_client.get_wattson_time()

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) < 1:
            self.cli.invalid_command(command)
            return True

        sub_command = command[0]
        if sub_command in ["restart-routing"]:
            return self._handle_basic_command(command, prefix)

        self.cli.unknown_command(command)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        choices = {
            "restart-routing": {
                "children": {},
                "description": "Restart routing services on all routers"
            }
        }
        return choices

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        if subcommand is not None and len(subcommand) > 0:

            pass

        return f"""
        {self.description(prefix)}
        Usage: util [action]
               
        Actions:
            restart-routing: Restart all routing services
            
        """

    def description(self, prefix: List[str]) -> str:
        return "Utility interactions to interact with different aspects of the simulation."

    def _load_available_nodes(self):
        self._nodes = {}
        response = self.cli.wattson_client.query(WattsonNetworkQuery(query_type=WattsonNetworkQueryType.GET_NODES))
        if not response.is_successful():
            print("Failed to load list of network nodes")
            return
        self._nodes = response.data.get("nodes", {})

    def _handle_basic_command(self, command: List[str], prefix: List[str]) -> bool:
        sub_command = command[0]
        if sub_command == "restart-routing":
            self.cli.wattson_client.query(WattsonNetworkQuery(query_type=WattsonNetworkQueryType.RESTART_ROUTING))
            return True
        return True
