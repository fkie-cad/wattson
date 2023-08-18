import ipaddress
from typing import Optional, List, TYPE_CHECKING, Dict

from wattson.cosimulation.exceptions import NetworkException
from wattson.cosimulation.exceptions.node_creation_failed_exception import NodeCreationFailedException
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_router import RemoteNetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_switch import RemoteNetworkSwitch
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator
from wattson.services.wattson_remote_service import WattsonRemoteService


if TYPE_CHECKING:
    from wattson.cosimulation.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class NodeCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)

        self.remote_network_emulator = RemoteNetworkEmulator.get_instance(wattson_client=self.cli.wattson_client)

        self.cli.register_command("node", self)
        self.cli.register_command("host", self)
        self.cli.register_command("switch", self)
        self.cli.register_command("router", self)
        self.cli.register_command("mtu", self)
        self.cli.register_command("rtu", self)

        self._nodes: Optional[Dict] = None

        self._available_commands = ["list",
                                    "info",
                                    "stop", "start", "restart",
                                    "log", "screen", "term",
                                    "pcap-start", "pcap-stop",
                                    "add-role", "delete-role", "list-roles"]
        self._command_paths = [
            [
                {"cmd": "list-roles", "description": "List all roles."},
                {"cmd": "$node", "description": "$node_info"},
            ],
            [
                {"cmd": "delete-role", "description": "Delete role from a node"},
                {"cmd": "$node", "description": "$node_info"},
            ],
            [
                {"cmd": "add-role", "description": "Add role to node"},
                {"cmd": "$node", "description": "$node_info"},
            ],
            [
                {"cmd": "list", "description": "List all $pl_prefix"}
            ],
            [
                {"cmd": "info", "description": "$prefix info"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "start", "description": "Start a $prefix"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "stop", "description": "Stop a $prefix"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "start-service", "description": "Start a $prefix service"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$service", "description": "$service_info"}
            ],
            [
                {"cmd": "stop-service", "description": "Stop a $prefix service"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$service", "description": "$service_info"}
            ],
            [
                {"cmd": "restart-service", "description": "Restart a $prefix service"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$service", "description": "$service_info"}
            ],
            [
                {"cmd": "screen", "description": "Open a screen"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "term", "description": "Open a terminal"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "add", "description": "Add a $prefix"}
            ],
            [
                {"cmd": "delete", "description": "Delete a $prefix"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "connect", "description": "Create link between $pl_prefix"},
                {"cmd": "$node", "description": "[From] $node_info"},
                {"cmd": "$node", "description": "[To] $node_info"}
            ],
            [
                {"cmd": "log", "description": "View $prefix log"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$service", "description": "$service_info"}
            ],
            [
                {"cmd": "pcap-start", "description": "Start a PCAP"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$interface", "description": "$interface_info"}
            ],
            [
                {"cmd": "pcap-stop", "description": "Stop a PCAP"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$interface", "description": "$interface_info"}
            ],
            [
                {"cmd": "update-default-route", "description": "Update the default route"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "open-browser", "description": "Open a browser"},
                {"cmd": "$node", "description": "$node_info"}
            ],
            [
                {"cmd": "interface", "description": "Manage interface"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$interface", "description": "$interface_info"},
                {"cmd": "set-ip", "description": "Set the IP address of this interface"},
                {"cmd": "@ip", "description": "The IP address to set"}
            ]
        ]

    def get_nodes(self) -> List[RemoteNetworkNode]:
        return self.remote_network_emulator.get_nodes()

    def get_node(self, entity_id) -> Optional[RemoteNetworkNode]:
        node = None
        try:
            node = self.remote_network_emulator.get_node(node=entity_id)
        finally:
            return node

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 1:
            if command[0] == "list":
                nodes = [node for node in self.get_nodes() if self.matches_type(prefix, node)]
                for node in nodes:
                    print(f"{node.entity_id}: {node.display_name} -- {', '.join(node.get_roles())}")
                return True
            elif command[0] == "list-roles":
                for node in self.get_nodes():
                    print(f"{node.entity_id}: {node.get_roles()}")
                return True
        context = None
        if len(command) >= 2:
            action = command[0]
            entity_id = command[1]
            if len(command) > 2:
                context = command[2]
        else:
            self.cli.invalid_command(prefix + command)
            return True
        node = self.get_node(entity_id)
        if node is None or not self.matches_type(prefix, node):
            if action != "add":
                # Add does not require an existing entity
                print(f"No {prefix[0]} {entity_id} found")
                return True
        if action == "add-role":
            node.add_role(context)
            self.cli._completer.setup()
            return True
        elif action == "delete-role":
            node.delete_role(context)
            self.cli._completer.setup()
            return True
        elif action == "info":
            self.print_node_info(node)
        elif action == "start":
            print(f"Starting {node.entity_id} ({node.display_name})")
            node.start()
        elif action == "stop":
            print(f"Stopping {node.entity_id} ({node.display_name})")
            node.stop()
        elif action == "start-service":
            if context is None:
                print("Starting all services")
                node.start_services()
            else:
                service = node.get_service(service_id=int(context))
                print(f"Starting service {service.id} ({service.name})")
                service.start()
        elif action == "stop-service":
            if context is None:
                print("Stopping all services")
                node.stop_services()
            else:
                service = node.get_service(service_id=int(context))
                print(f"Stopping service {service.id} ({service.name})")
                service.stop()
        elif action == "restart-service":
            if context is None:
                print("Restarting all services")
                node.stop_services()
                node.start_services()
            else:
                service = node.get_service(service_id=int(context))
                print(f"Restarting service {service.id} ({service.name})")
                service.restart()
        elif action == "pcap-start" or action == "pcap-stop":
            if context is None:
                interface = None
            else:
                interface = node.get_interface(context).entity_id
                if interface is None:
                    print("Interface not found")
                    return True
            if action == "pcap-start":
                if interface is None:
                    print("Starting PCAP for all interfaces")
                else:
                    print(f"Starting PCAP at interface {interface}")
                interface = node.get_interface(context)
                node.start_pcap(interface=interface)
            elif action == "pcap-stop":
                if interface is None:
                    print("Stopping PCAP for all interfaces")
                else:
                    print(f"Stopping PCAP at interface {interface}")
                interface = node.get_interface(context)
                node.stop_pcap(interface=interface)
        elif action == "add":
            supported_adding = ["host", "router", "switch"]
            if self.get_singular(prefix) not in supported_adding:
                print(f"Only {', '.join(supported_adding)} supported for adding")
                return True
            if entity_id is None:
                print(f"ID for new {self.get_singular(prefix[0])} required")
                return True
            self._add_node(prefix[0], entity_id)
            return True
        elif action == "delete":
            self.remote_network_emulator.remove_node(node)
            print(f"Removed {node.entity_id}")
        elif action == "connect":
            if entity_id is None:
                print("Node A has to be specified")
                return True
            if context is None:
                print("Node B has to be specified")
                return True
            self._connect_nodes(prefix, entity_id, context)
        elif action == "term":
            if node.open_terminal():
                print("Terminal opened")
            else:
                print(f"Cannot open terminal for node {node.entity_id}")
            return True
        elif action == "open-browser":
            response = self.remote_network_emulator.query(WattsonNetworkQuery(
                query_type=WattsonNetworkQueryType.NODE_ACTION,
                query_data={
                    "entity_id": node.entity_id,
                    "action": "start-browser"
                }
            ))
            if not response.is_successful():
                error = response.data.get("error", "")
                print(f"Could not open browser: {error=}")
            else:
                print("Browser opened")
            return True
        elif action == "update-default-route":
            if not isinstance(node, RemoteNetworkHost):
                print("Cannot set default route for this node type")
                return True
            if node.update_default_route():
                print("Default route updated")
            else:
                print("Could not update default route")
            return True
        elif action == "interface":
            if len(command) < 4:
                print("Invalid command")
                return True
            interface = node.get_interface(context)
            sub_action = command[3]
            if sub_action == "set-ip":
                if len(command) < 5:
                    ip = None
                else:
                    ip = command[4]
                    ip = ipaddress.IPv4Address(ip)
                if interface.set_ip_address(ip_address=ip):
                    print(f"IP set to {ip}")
                else:
                    print("Could not set IP address")
                return True
        else:
            self.cli.invalid_command(command)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        nodes = self.get_nodes()

        def _expand_placeholder(_placeholder: str, _node=None, _context=None) -> list:
            if "$prefix" in _placeholder:
                return _expand_placeholder(_placeholder.replace("$prefix", self.get_singular(prefix)), _node, _context)
            if "$pl_prefix" in _placeholder:
                return _expand_placeholder(_placeholder.replace("$pl_prefix", self.get_plural(prefix)), _node, _context)
            if "$node_info" in _placeholder:
                if not isinstance(_node, RemoteNetworkNode):
                    raise ValueError("Node not defined")
                return _expand_placeholder(_placeholder.replace("$node_info", _node.display_name), _node, _context)
            if _placeholder == "$service_info":
                if not isinstance(_context, WattsonRemoteService):
                    raise ValueError("Context is no WattsonService")
                return _expand_placeholder(_placeholder.replace("$service_info", _context.name), _node, _context)
            if _placeholder == "$interface_info":
                if not isinstance(_context, RemoteNetworkInterface):
                    raise ValueError("Context is no WattsonNetworkInterface")
                return _expand_placeholder(_placeholder.replace("$interface_info", _context.display_name), _node, _context)
            if _placeholder == "$node":
                return [n for n in nodes if self.matches_type(prefix, n)]
            if _placeholder == "$service":
                if not isinstance(_node, RemoteNetworkNode):
                    raise ValueError("Node is not defined")
                return list(_node.get_services().values())
            if _placeholder == "$interface":
                if not isinstance(_node, RemoteNetworkNode):
                    raise ValueError("Node is not defined")
                return _node.get_interfaces()
            return [_placeholder]

        def _expand_auto_completion(_path, _node=None) -> dict:
            if len(_path) == 0:
                return {}

            entry = _path[0]
            _auto_completion = {}
            cmd = entry["cmd"]
            description = entry["description"]

            _expanded = _expand_placeholder(cmd, _node)
            for _e in _expanded:
                _ref_node = _node if not isinstance(_e, RemoteNetworkNode) else _e
                _description = _expand_placeholder(description, _ref_node, _e)[0]
                _children = _expand_auto_completion(_path[1:], _node=_ref_node)
                _cmd = _e
                if isinstance(_e, (RemoteNetworkNode, RemoteNetworkInterface)):
                    _cmd = _e.entity_id
                if isinstance(_e, WattsonRemoteService):
                    _cmd = _e.id
                _auto_completion[str(_cmd)] = {
                    "children": _children,
                    "description": _description
                }
            return _auto_completion

        command_completion_dict = {}
        for path in self._command_paths:
            completion_dict = _expand_auto_completion(path)
            command_completion_dict.update(completion_dict)

        return {
            prefix[0]: {
                "children": command_completion_dict,
                "description": f"Manage network {self.get_plural(prefix)}"
            }
        }

    @staticmethod
    def print_node_info(node: RemoteNetworkNode):
        print(f"Node {node.entity_id}")
        print(f"  Name         {node.get_hostname()}")
        print(f"  System       {node.system_id}")
        print(f"  Role         {node.get_role()}")
        print(f"  Roles         {', '.join(node.get_roles())}")
        print(f"  Interfaces   {len(node.get_interfaces())}")
        for i, interface in enumerate(node.get_interfaces()):
            interface_infos = [str(i)]
            if interface.get_system_name() is not None:
                interface_infos.append(interface.get_system_name())
            if interface.has_ip():
                ip_str = f"{str(interface.get_ip_address()).rjust(15)}/{str(interface.get_subnet_prefix_length()).ljust(2)}"
                interface_infos.append(ip_str)
            if interface.get_mac_address() is not None:
                mac_str = str(interface.get_mac_address()).ljust(17)
                interface_infos.append(mac_str)
            if interface.is_management:
                interface_infos.append("(Management)")
            print(f"        {' // '.join(interface_infos)}")

        print(f"  Services     {len(node.get_services())}")
        for _, service in node.get_services().items():
            print("-------------------------------------")
            for key, value in service.get_info().items():
                print(f"        {key}: {value}")

    def get_singular(self, prefix) -> str:
        return self.get_node_info(prefix)["singular"]

    def get_plural(self, prefix) -> str:
        return self.get_node_info(prefix)["plural"]

    def matches_type(self, prefix, entity) -> bool:
        return self.get_node_info(prefix)["check"](entity)

    @staticmethod
    def get_node_info(prefix):
        if isinstance(prefix, list):
            prefix = prefix[0]
        return {
            "switch": {
                "singular": "switch",
                "plural": "switches",
                "check": lambda x: isinstance(x, RemoteNetworkSwitch),
                "class": RemoteNetworkSwitch,
                "type": "switch"
            },
            "node": {
                "singular": "node",
                "plural": "nodes",
                "check": lambda x: isinstance(x, RemoteNetworkNode),
                "class": RemoteNetworkNode,
                "type": "node"
            },
            "host": {
                "singular": "host",
                "plural": "hosts",
                "check": lambda x: isinstance(x, RemoteNetworkHost) and not isinstance(x, RemoteNetworkRouter),
                "class": RemoteNetworkHost,
                "type": "host"
            },
            "router": {
                "singular": "router",
                "plural": "routers",
                "check": lambda x: isinstance(x, RemoteNetworkRouter),
                "class": RemoteNetworkRouter,
                "type": "router"
            },
            "firewall": {
                "singular": "firewall",
                "plural": "firewalls",
                "check": lambda x: x.supports_firewall(),
                "class": RemoteNetworkNode,
                "type": "host"
            },
            "rtu": {
                "singular": "RTU",
                "plural": "RTUs",
                "check": lambda x: isinstance(x, RemoteNetworkHost) and x.has_role("rtu"),
                "class": RemoteNetworkHost,
                "type": "host"
            },
            "mtu": {
                "singular": "MTU",
                "plural": "MTUs",
                "check": lambda x: isinstance(x, RemoteNetworkHost) and x.has_role("mtu"),
                "class": RemoteNetworkHost,
                "type": "host"
            }
        }.get(prefix)

    def _add_node(self, prefix, entity_id):
        node = self.get_node(entity_id=entity_id)
        if node is not None:
            print(f"Node with ID {entity_id} already exists")
            return False
        info = self.get_node_info(prefix)
        node_type_class = info["class"]
        try:
            node = self.remote_network_emulator.create_node(
                entity_id=entity_id,
                node_class=node_type_class
            )
            print(f"Created node {entity_id} ({node.system_id} // {node.display_name})")
            return True
        except NodeCreationFailedException as e:
            print("Failed to create node")
            print(f"{e=}")
            return False

    def _connect_nodes(self, prefix, entity_id, context):
        node_a = self.get_node(entity_id=entity_id)
        node_b = self.get_node(entity_id=context)
        if node_a is None:
            print(f"Node A ({entity_id}) does not exist")
            return False
        if node_b is None:
            print(f"Node B ({context}) does not exist")
            return False
        if not self.matches_type(prefix, node_a):
            print(f"Node A ({entity_id}) is no {self.get_singular(prefix)}")
        if not self.matches_type(prefix, node_b):
            print(f"Node B ({context}) is no {self.get_singular(prefix)}")

        try:
            interface_a, link, interface_b = self.remote_network_emulator.connect_nodes(
                node_a=node_a,
                node_b=node_b,
                interface_a_options={},
                interface_b_options={},
                link_options={}
            )
            print(f"Connected nodes {node_a.entity_id} <-> {node_b.entity_id}")
            print(f"  Interface A: {interface_a.entity_id}")
            print(f"  Interface B: {interface_b.entity_id}")
            print(f"  Link: {link.entity_id}")
            return True
        except NetworkException as e:
            print("Failed to connect nodes")
            print(f"  {e=}")
            return False

    def description(self, prefix: List[str]) -> str:
        return f"Manage {self.get_plural(prefix)}"

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return f"""
        Manage {self.get_plural(prefix)}
        Usage: '{prefix[0]} <command> <entity_id>'
        Available commands:
        {'  '.join(sorted(self._available_commands))}        
        """
