from wattson.services.wattson_service import WattsonService


class WattsonFirewallService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)

    def callable_methods(self) -> dict[str, dict]:
        return {
            "allow": {
                "parameters": {
                    "port": {"type": int, "description": "The port number."},
                    "source": {"type": str, "description": "The source address."},
                    "destination": {"type": str, "description": "The destination address."},
                    "append": {"type": bool, "description": "Whether or not the rule should be appended."},
                    "action": {"type": str, "description": "The action of the rule. Defaults to ACCEPT."},
                    "protocol": {"type": str, "description": "The protocol of the rule. Defaults to TCP."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Allow specified traffic."
            },
            "block": {
                "parameters": {
                    "port": {"type": int, "description": "The port number."},
                    "source": {"type": str, "description": "The source address."},
                    "destination": {"type": str, "description": "The destination address."},
                    "append": {"type": bool, "description": "Whether or not the rule should be appended."},
                    "action": {"type": str, "description": "The action of the rule. Defaults to REJECT."},
                    "protocol": {"type": str, "description": "The protocol of the rule. Defaults to TCP."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Block specified traffic."
            }
        }

    def call(self, method, **kwargs):
        parts = []
        if "port" in kwargs:
            parts.append("-p")
            parts.append(kwargs.get("protocol", "tcp"))
            parts.append("--destination-port")
            parts.append(kwargs["port"])
        if "source" in kwargs:
            parts.append("-s")
            parts.append(kwargs["source"])
        if "destination" in kwargs:
            parts.append("-d")
            parts.append(kwargs["destination"])

        if method == "allow":
            parts.append("-j")
            parts.append(kwargs.get("action", "ACCEPT"))
        elif method == "block":
            parts.append("-j")
            parts.append(kwargs.get("action", "REJECT"))
        else:
            return False
        rule_options = ' '.join([str(part) for part in parts])
        insertion_mode = "-A" if kwargs.get("append", False) else "-I"
        input_code, _ = self.network_node.exec(["iptables", insertion_mode, "INPUT", rule_options])
        forward_code, _ = self.network_node.exec(["iptables", insertion_mode, "FORWARD", rule_options])
        return input_code == forward_code == 0
