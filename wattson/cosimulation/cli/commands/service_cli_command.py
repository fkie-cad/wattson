import time
from typing import Optional, List, TYPE_CHECKING, Dict

import tabulate

from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.services.wattson_remote_service import WattsonRemoteService

if TYPE_CHECKING:
    from wattson.cosimulation.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class ServiceCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self._nodes: Optional[Dict] = None
        self.cli.register_command("service", self)

        self._services: Dict[int, WattsonRemoteService] = {}
        self._info_timestamp = 0
        self._info_timeout_seconds = 20
        self.time = self.cli.wattson_client.get_wattson_time()

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) < 1:
            self.cli.invalid_command(command)
            return True

        sub_command = command[0]
        if sub_command in ["start", "stop", "restart", "kill", "poll", "info"]:
            return self._handle_basic_command(command, prefix)

        self.cli.unknown_command(command)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        self._load_available_nodes()
        self._update_services(True)
        service_list = {str(service.id): {"children": [], "description": None} for service in self._services.values()}
        actions = ["start", "stop", "restart", "kill", "poll", "info"]
        return {
            prefix[0]: {
                "children": {
                    action: {"children": service_list, "description": f"{action} service"}
                    for action in actions
                },
                "description": "Manage services"
            }
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        if subcommand is not None and len(subcommand) > 0:

            pass

        return f"""
        {self.description(prefix)}
        Usage: service [action] [$service_id || $entity_id] [options]
               
        Actions:
            start:   Start a service
            stop:    Stop a service
            restart: restart a service
            list:    list available services (optional for a single entity only)
            refresh: force refreshing the available services
            add:     Add a service to an existing entity
        """

    def description(self, prefix: List[str]) -> str:
        return "Interact with services running within the network emulation and create new ones."

    def get_service(self, service_id: int) -> WattsonRemoteService:
        return self._services.setdefault(
            service_id,
            WattsonRemoteService(self.cli.wattson_client, service_id=service_id, auto_sync=False)
        )

    def _update_services(self, force: bool = False):
        if not force and not self.time.time() - self._info_timestamp > self._info_timeout_seconds:
            return
        response = self.cli.wattson_client.query(WattsonNetworkQuery(query_type=WattsonNetworkQueryType.GET_SERVICES))
        if response.is_successful():
            services = response.data.get("services")
            for service_info in services:
                service = self.get_service(service_id=service_info["service_id"])
                service.sync_from_remote_representation(service_info)
            self._info_timestamp = self.time.time()
        else:
            print("Failed to update service information")

    def _load_available_nodes(self):
        self._nodes = {}
        response = self.cli.wattson_client.query(WattsonNetworkQuery(query_type=WattsonNetworkQueryType.GET_NODES))
        if not response.is_successful():
            print("Failed to load list of network nodes")
            return
        self._nodes = response.data.get("nodes", {})

    def _handle_basic_command(self, command: List[str], prefix: List[str]) -> bool:
        sub_command = command[0]
        if sub_command == "refresh":
            self._update_services(force=True)
            return True
        if sub_command in ["start", "stop", "poll", "restart", "kill", "info"]:
            if len(command) < 2:
                self.cli.invalid_command(command)
                return True
            service_id = int(command[1])
            service = self.get_service(service_id)
            if not service.connected():
                print("Service not available")
                return True
            if sub_command == "start":
                if service.start():
                    print("Service started")
                else:
                    print("Failed to start service")
                return True
            if sub_command == "stop":
                if service.stop():
                    print("Service stopped")
                else:
                    print("Failed to stop service")
                return True
            if sub_command == "restart":
                if service.restart():
                    print("Service restarted")
                else:
                    print("Failed to restart service")
                return True
            if sub_command == "poll":
                res = service.poll()
                if res is None:
                    print("Service is still active")
                else:
                    print(f"Service terminated with {res}")
                return True
            if sub_command == "kill":
                if service.kill():
                    print("Killing service")
                else:
                    print("Failed to kill service")
                return True
            if sub_command == "info":
                rows = []
                for key, value in service.get_info().items():
                    rows.append((key, value))
                print(tabulate.tabulate(rows))
                return True
            return True
