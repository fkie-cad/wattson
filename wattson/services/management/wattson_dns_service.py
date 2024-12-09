import subprocess
from typing import Optional, Callable, TYPE_CHECKING, Union

from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonDnsService(WattsonService):

    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.config = self._service_configuration.get("options", {})
        self.logger = get_logger("DNS Server", "DNS Server")
        # self.logger.setLevel("DEBUG")

    def get_stdout(self):
        return self.get_log_handle()

    def get_stderr(self):
        return subprocess.STDOUT

    def call(self, method, **kwargs) -> Union[bool, str]:
        if method == "add_dns_entry":
            hostname = kwargs.get("hostname")
            ip = kwargs.get("ip")
            domain = kwargs.get("domain")
            _type = kwargs.get("type")
            code, _ = self.network_node.exec(["python3", "scripts/add_entry.py", hostname, ip, domain, _type])
            return code == 0
        elif method == "remove_dns_entry":
            hostname = kwargs.get("hostname")
            ip = kwargs.get("ip")
            domain = kwargs.get("domain")
            _type = kwargs.get("type")
            code, _ = self.network_node.exec(["python3", "scripts/remove-entry.py", hostname, ip, domain, _type])
            return code == 0
        elif method == "modify_dns_entry":
            hostname = kwargs.get("hostname")
            ip = kwargs.get("ip")
            domain = kwargs.get("domain")
            _type = kwargs.get("type")
            target = kwargs.get("target")
            target_value = kwargs.get("target_value")
            code, _ = self.network_node.exec(["python3", "scripts/modify-entry.py", hostname, ip, domain, _type, target, target_value])
            return code == 0
        elif method == "list_dns_entries":
            domain = kwargs.get("domain")
            code, lines = self.network_node.exec(["python3", "scripts/list-entries.py", domain])
            text = "\n".join(lines)
            text = text.replace("\\t", "\t")
            return text
        return False

    def callable_methods(self) -> dict[str, dict]:
        return {
            "add_dns_entry": {
                "parameters": {
                    "hostname": {"type": str, "description": "The host name (FQDN) to include in the DNS"},
                    "ip": {"type": str, "description": "The desired IP address to associate with the hostname"},
                    "domain": {"type": str, "description": "The name of the zone."},
                    "type": {"type": str, "description": "The type of record. (A, MX, NS, DMARC, SPF)"}
                },
                "returns": {
                    "type": bool,
                    "description": "Whether the operation was successful or not"
                },
                "description": "Adds a new DNS entry for the server, associating the given host name with the given IP address"
            },
            "remove_dns_entry": {
                "parameters": {
                    "hostname": {"type": str, "description": "The host name (FQDN) of the DNS"},
                    "ip": {"type": str, "description": "The IP address associated with the hostname"},
                    "domain": {"type": str, "description": "The name of the zone."},
                    "type": {"type": str, "description": "The type of record. (A, MX, NS, DMARC, SPF)"}
                },
                "returns": {
                    "type": bool,
                    "description": "Whether the operation was successful or not"
                },
                "description": "Removes a DNS entry in the server, by the given host name with the given IP address"
            },
            "modify_dns_entry": {
                "parameters": {
                    "hostname": {"type": str, "description": "The host name (FQDN) of the DNS"},
                    "ip": {"type": str, "description": "The IP address associated with the hostname"},
                    "domain": {"type": str, "description": "The name of the zone the host is in."},
                    "type": {"type": str, "description": "The type of record. (A, MX, NS, DMARC, SPF)"},
                    "target": {"type": str,
                               "description": "The part of the entry that should be changed. (hostname, ip or domain)"},
                    "target_value": {"type": str, "description": "The new value for the target."}
                },
                "returns": {
                    "type": bool,
                    "description": "Whether the operation was successful or not"
                },
                "description": "Modify a DNS entry."
            },
            "list_dns_entries": {
                "parameters": {
                    "domain": {"type": str, "description": "The domain for which to list all dns entries."}
                },
                "returns": {
                    "type": str,
                    "description": "A string whose value is the content of the zone file."
                },
                "description": "List all DNS entries for the given domain."
            }
        }

    def get_start_command(self) -> list[str]:
        return [
            self.network_node.get_python_executable(),
            "/wattson/scripts/start.py",
            str(self.get_current_guest_configuration_file_path().absolute())
        ]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["service", "named", "stop"])
        return success
