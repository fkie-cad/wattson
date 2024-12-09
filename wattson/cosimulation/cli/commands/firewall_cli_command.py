from typing import Optional

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_router import RemoteNetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_switch import RemoteNetworkSwitch
from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator
from wattson.cosimulation.simulators.network.roles.ip_tables_firewall import IPTablesFirewall
from wattson.services.wattson_remote_service import WattsonRemoteService


class FirewallCliCommand(CliCommandHandler):
    def description(self, prefix: list[str]) -> str:
        return "Commands for adding/removing/modifying iptables firewall rules."

    def __init__(self, cli):
        super().__init__(cli)
        self.remote_network_emulator = RemoteNetworkEmulator(wattson_client=self.cli.wattson_client)
        self.cli.register_command("firewall", self)
        self.available_commands = ["list", "add-rule", "delete-rule", "enable-rule", "disable-rule", "block-traffic", "block-tcp-traffic"]
        self._command_paths = [
            [
                {"cmd": "block-tcp-traffic", "description": "Node 0 will drop all tcp packets sent by Node 1."},
                {"cmd": "$firewall", "description": "$node_info"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$interface", "description": "$interface_info"}
            ],
            [
                {"cmd": "block-traffic", "description": "Node 0 will drop all packets sent by Node 1."},
                {"cmd": "$firewall", "description": "$node_info"},
                {"cmd": "$node", "description": "$node_info"},
                {"cmd": "$interface", "description": "$interface_info"}
            ],
            [
                {"cmd": "list", "description": "List all rules of a firewall."},
                {"cmd": "$firewall", "description": "$node_info"}
            ],
            [
                {"cmd": "list-active", "description": "List active rules of a firewall"},
                {"cmd": "$firewall", "description": "$node_info"}
            ],
            [
                {"cmd": "add-rule", "description": "Add rule to a firewall."},
                {"cmd": "$firewall", "description": "$node_info"},
            ],
            [
                {"cmd": "delete-rule", "description": "Delete rule from a firewall."},
                {"cmd": "$firewall", "description": "$node_info"},
            ],
            [
                {"cmd": "enable-rule", "description": "Enable a rule on a firewall."},
                {"cmd": "$firewall", "description": "$node_info"},
            ],
            [
                {"cmd": "disable-rule", "description": "Disable a rule on a firewall."},
                {"cmd": "$firewall", "description": "$node_info"},
            ]
        ]
    def get_node(self, entity_id) -> Optional[RemoteNetworkNode]:
        node = None
        try:
            node = self.remote_network_emulator.get_node(node=entity_id)
        finally:
            return node

    def handle_command(self, command: list[str], prefix: list[str]) -> bool:
        action = command[0]
        entity_id = command[1]
        node = self.get_node(entity_id)
        context = command[2] if len(command) == 3 else None
        if action == "list":
            firewall = IPTablesFirewall(node)
            print(firewall.list_rules())
            return True
        elif action == "list-active":
            firewall = IPTablesFirewall(node)
            print(firewall.list_active_rules())
            return True
        elif action == "add-rule":
            firewall = IPTablesFirewall(node)
            firewall.add_rule(context)
            return True
        elif action == "delete-rule":
            firewall = IPTablesFirewall(node)
            firewall.remove_rule(int(context))
            return True
        elif action == "enable-rule":
            firewall = IPTablesFirewall(node)
            firewall.enable_rule(int(context))
            return True
        elif action == "disable-rule":
            firewall = IPTablesFirewall(node)
            firewall.disable_rule(int(context))
            return True
        elif action == "block-traffic":
            firewall = IPTablesFirewall(node)
            other_node = self.get_node(command[2])
            ip = other_node.get_primary_ip_address_string(with_subnet_length=False)
            if ip is not None:
                firewall.block_traffic_from_address(ip)
            return True
        elif action == "block-tcp-traffic":
            firewall = IPTablesFirewall(node)
            other_node = self.get_node(command[2])
            ip = other_node.get_primary_ip_address_string(with_subnet_length=False)
            if ip is not None:
                firewall.block_tcp_traffic_from_address(ip)
            return True
        else:
            print(f"Command {action} not known.")
            return True

    def auto_complete_choices(self, prefix: list[str], level: Optional[int] = None) -> dict:
        nodes = self.get_nodes()

        def _expand_placeholder(_placeholder: str, _node=None, _context=None) -> list:

            if _placeholder == "$interface_info":
                if not isinstance(_context, RemoteNetworkInterface):
                    raise ValueError("Context is no WattsonNetworkInterface")
                return _expand_placeholder(_placeholder.replace("$interface_info", _context.display_name), _node, _context)
            if _placeholder == "$interface":
                if not isinstance(_node, RemoteNetworkNode):
                    raise ValueError("Node is not defined")
                return _node.get_interfaces()
            if "$node_info" in _placeholder:
                if not isinstance(_node, RemoteNetworkNode):
                    raise ValueError("Node not defined")
                return _expand_placeholder(_placeholder.replace("$node_info", _node.display_name), _node, _context)
            if _placeholder == "$firewall":
                return [n for n in nodes if self.matches_type(prefix, n)]
            if _placeholder == "$node":
                return [n for n in nodes if self.matches_type("node", n)]
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
                "check": lambda x: x.has_role("firewall"),
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

    def get_nodes(self) -> list[RemoteNetworkNode]:
        return self.remote_network_emulator.get_nodes()

    def get_singular(self, prefix) -> str:
        return self.get_node_info(prefix)["singular"]

    def get_plural(self, prefix) -> str:
        return self.get_node_info(prefix)["plural"]
