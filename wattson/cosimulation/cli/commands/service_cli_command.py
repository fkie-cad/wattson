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
        if sub_command in ["start", "stop", "restart", "kill", "poll", "info", "config", "restart-all", "start-all", "stop-all"]:
            return self._handle_basic_command(command, prefix)

        self.cli.unknown_command(command)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        self._load_available_nodes()
        self._update_services(True)
        service_list = {str(service.id): {"children": {}, "description": None} for service in self._services.values()}
        actions = ["start", "stop", "restart", "kill", "poll", "info", "config", "restart-all", "stop-all", "start-all"]
        choices = {
            prefix[0]: {
                "children": {
                    action: {"children": service_list, "description": f"{action} service"}
                    for action in actions
                },
                "description": "Manage services"
            }
        }
        for service_id, service_config in choices[prefix[0]]["children"]["config"]["children"].items():
            service_config["children"] = {
                "show": {
                    "children": {},
                    "description": "Show the (reduced) configuration"
                },
                "dump": {
                    "children": {},
                    "description": "Show the (full) configuration"
                },
                "raw": {
                    "children": {},
                    "description": "Show the unexpanded configuration"
                },
                "keys": {
                    "children": {},
                    "description": "List the available keys"
                },
                "get": {
                    "children": {},
                    "description": "Get a configuration option (key)"
                },
                "expand": {
                    "children": {},
                    "description": "Expand a configuration option (key)"
                },
                "set": {
                    "children": {},
                    "description": "Set a configuration option (key value)"
                },
                "restart-all": {
                    "children": {},
                    "description": "Restart all services"
                },
                "stop-all": {
                    "children": {},
                    "description": "Stop all services"
                },
                "start-all": {
                    "children": {},
                    "description": "Start all services"
                }
            }
        return choices

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
            config:  Interact with the service's configuration
                show: Prints the configuration, omitting large objects
                dump: Prints the full configuration
                keys: Prints the available keys of the configuration
                get:  Gets a single configuration option
                expand:  Permanently expands a single configuration option
                set:  Sets a single configuration option (is parsed as JSON)
            start-all:  Start a all services
            stop-all:  Stop all services
            restart-all:  Restart all services
            
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

    def _traverse_config_dict(self, configuration: Dict, keys: Union[str, List[str]], set_value: bool = False, value: Any = None) -> Any:
        if isinstance(keys, str):
            keys = keys.split(".")
        if len(keys) == 0:
            if set_value:
                print("Cannot set empty key")
            return configuration
        key = keys.pop(0)
        if key in configuration:
            if len(keys) > 0:
                return self._traverse_config_dict(configuration[key], keys, set_value, value)
            if set_value:
                configuration[key] = value
            return configuration[key]
        elif len(keys) == 0 and set_value:
            configuration[key] = value
            return configuration
        else:
            print(f"Invalid key: {key}")
        return None

    def _handle_basic_command(self, command: List[str], prefix: List[str]) -> bool:
        sub_command = command[0]
        if sub_command == "refresh":
            self._update_services(force=True)
            return True

        if sub_command in ["start", "stop", "poll", "restart", "kill", "info", "config"]:
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
            if sub_command == "config":
                if len(command) < 3:
                    print("Invalid command")
                    return True
                config_command = command[2]
                if config_command in ["show", "dump", "keys", "raw"]:
                    if config_command == "raw":
                        print(json.dumps(service.get_configuration(), indent=4))
                        return True
                    if config_command in ["show", "dump"]:
                        expanded = service.expand_configuration().copy()
                        if config_command == "show":
                            for key in ["power_grid", "network", "datapoints"]:
                                if key in expanded:
                                    expanded[key] = "@hidden"
                        print(json.dumps(expanded, indent=4))
                        return True
                    if config_command == "keys":
                        print("Configuration keys")
                        for key in service.get_configuration().keys():
                            print(key)
                if config_command == "get":
                    if len(command) < 4 or len(command[3]) == 0:
                        print("Specify configuration key")
                        return True
                    key = command[3]
                    print(f"== Configuration for {key} ==")
                    print(f"==== Raw configuration ====")
                    raw_config = self._traverse_config_dict(service.get_configuration(), key)
                    print(json.dumps(raw_config, indent=4))
                    print(f"==== Expanded configuration ====")
                    expanded_config = self._traverse_config_dict(service.expand_configuration(), key)
                    print(json.dumps(expanded_config, indent=4))

                if config_command == "expand":
                    if len(command) < 4 or len(command[3]) == 0:
                        print("Specify configuration key")
                        return True
                    key = command[3]
                    print(f"Expanding configuration {key}")
                    expanded_config_value = self._traverse_config_dict(service.expand_configuration(), key)
                    configuration = service.get_configuration().copy()
                    configuration = self._traverse_config_dict(configuration, key, True, expanded_config_value)
                    service.set_configuration(configuration)
                    return True
                if config_command == "set":
                    if len(command) < 5:
                        print("Specify configuration key and value")
                        return True
                    key = command[3]
                    value = json.loads(command[4])
                    configuration = service.get_configuration().copy()
                    configuration = self._traverse_config_dict(configuration, key, True, value)
                    print(f"Setting configuration {key} = {repr(value)}")
                    service.set_configuration(configuration)
                    return True
        if sub_command in ["restart-all", "start-all", "stop-all"]:
            self._update_services()
            if sub_command == "start-all":
                print("Starting all services")
            elif sub_command == "stop-all":
                print("Stopping all services")
            elif sub_command == "restart-all":
                print("Restarting all services")

            for service_id, service in self._services.items():
                print(f"{service_id}", end="  ", flush=True)
                if sub_command == "restart-all":
                    service.restart()
                elif sub_command == "start-all":
                    service.start()
                elif sub_command == "stop-all":
                    service.stop()
            print("")
        return True
