import types
from typing import Optional
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode
from wattson.services.wattson_service_interface import WattsonServiceInterface
from wattson.cosimulation.simulators.network.roles.ip_tables_firewall_rules import IPTablesFirewallRules


class IPTablesFirewall:
    def __init__(self, node):
        self._node = node
        self.support_enabled = None

    def supports_firewall(self) -> bool:
        if self.support_enabled is None:
            self.support_enabled = self._node.exec("which iptables")[0] == 0
        return self.support_enabled and self._node.has_role("firewall")
    
    def add_rule(self, rule, enable: bool = False) -> bool:
        if self.supports_firewall():
            rule = IPTablesFirewallRules(rule)
            rules = self._node.get_config().get("rules", [])
            rules.append(rule)
            if not self._node.update_config({"rules": rules}):
                return False
            rule_id = len(rules) - 1
            if enable:
                return self.enable_rule(rule_id)
            return True
        return False

    def rule_index_exists(self, index: int) -> bool:
        return 0 <= index < len(self._node.get_config().get("rules", []))

    def remove_rule(self, index) -> bool:
        if self.supports_firewall():
            if self.rule_index_exists(index):
                old_rules = self._node.get_config().get("rules", [])
                del old_rules[int(index)]
                return self._node.update_config({
                    "rules": old_rules
                })
        return False

    def enable_rule(self, index) -> bool:
        if self.supports_firewall():
            if self.rule_index_exists(index):
                rule = self._node.get_config()["rules"][int(index)]
                code, lines = self._node.exec(rule["rule"])
                if code != 0:
                    error = '\n'.join(lines)
                    self._node.logger.error(f"Error enabling rule: {error}")
                    return False
                return True
        return False

    def disable_rule(self, index) -> bool:
        if self.supports_firewall():
            if self.rule_index_exists(index):
                rule_obj = self._node.get_config()["rules"][int(index)]
                disable_rule = f"iptables -D {rule_obj['specification']}"
                code, lines = self._node.exec(disable_rule)
                self._node.synchronize(force=True)
                if code != 0:
                    error = '\n'.join(lines)
                    self._node.logger.error(f"Error disabling rule: {error}")
                    return False
                return True
        return False

    def list_active_rules(self):
        if self.supports_firewall():
            command = "iptables -L --line-numbers"
            code, lines = self._node.exec(command)
            return "\n".join(lines)

    def list_rules(self):
        if not self.supports_firewall():
            return f"{self._node.entity_id} does not support firewall."
        text = ""
        if "rules" not in self._node.get_config():
            return "No rules field in config"
        for i, r in enumerate(self._node.get_config()["rules"]):
            text += f"{i}: {r}\n"
        return text

    def block_traffic_from_address(self, address) -> bool:
        rule = f"iptables -I INPUT -s {address[:-3]} -j DROP"
        success = self.add_rule(rule, enable=True)
        rule = f"iptables -I FORWARD -s {address[:-3]} -j DROP"
        success &= self.add_rule(rule, enable=True)
        return success

    def block_tcp_traffic_from_address(self, address) -> bool:
        rule = f"iptables -I INPUT -p tcp -s {address} -j DROP"
        success = self.add_rule(rule, enable=True)
        rule = f"iptables -I FORWARD -p tcp -s {address} -j DROP"
        success &= self.add_rule(rule, enable=True)
        return success
