import json
import shlex
import subprocess
from typing import Optional, Callable, List, TYPE_CHECKING

from wattson.cosimulation.exceptions import NetworkException
from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface
from wattson.cosimulation.simulators.network.components.interface.network_router import NetworkRouter
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonDhcpServerService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("DHCP Server", "DHCP Server")

    def ensure_artifacts(self):
        self.ensure_working_directory()
        if self.config_file is None:
            self.config_file = ArtifactRotate(self.working_directory.joinpath(f"{self.network_node.entity_id}-service-{self.id}-dhcpd.conf"))
            self._artifacts.append(self.config_file)
        super().ensure_artifacts()

    def write_configuration_file(self, configuration: dict, refresh_config: bool = False):
        self.ensure_artifacts()
        if not refresh_config and not self.config_file.is_empty():
            return
        with self.config_file.get_current().open("w") as f:
            dhcpd_config = self._get_dhcpd_config_content()
            f.write(dhcpd_config)

    def get_stdout(self):
        return self.get_log_handle()

    def get_stderr(self):
        return subprocess.STDOUT

    def get_start_command(self) -> List[str]:
        cmd = [
            self.network_node.get_python_executable(),
            "/wattson/scripts/start-dhcp-server.py",
            str(self.get_current_guest_configuration_file_path().absolute()),
        ]
        cmd.extend(self._get_dhcp_interfaces())
        return cmd

    def stop(self,
             wait_seconds: float = 5,
             auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None
             ) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback)
        self.network_node.exec("service isc-dhcp-server stop", shell=True)
        return success

    def _get_dhcpd_config_content(self):
        if self._service_configuration.get("derive_configuration", False):
            if not self._derive_configuration():
                return ""

        lines = [
            "default-lease-time 6000;",
            "max-lease-time 7200;",
            "authoritative;",
            ""
        ]

        for lease in self._service_configuration["leases"]:
            dns_options = ""
            router_options = ""
            if len(lease["dns_servers"]) > 0:
                dns_options = f"  option domain-name-servers {', '.join(lease['dns_servers'])};"
            if len(lease["routers"]) > 0:
                router_options = f"  option routers {', '.join(lease['routers'])};"

            lines.extend([
                f"subnet {lease['subnet']} netmask {lease['netmask']} " + "{",
                f"  range {' '.join([str(ip) for ip in lease['range']])};",
                router_options,
                dns_options,
                "}"
                ""
            ])

        return "\n".join(lines)

    def _get_dhcp_interfaces(self):
        return self._service_configuration.get("interfaces", [])

    def _derive_configuration(self):
        self._service_configuration["leases"] = []
        self._service_configuration["interfaces"] = []

        # Detect DNS
        dns_server: WattsonNetworkNode
        dns_server_ips = []
        for dns_server in self.network_node.network_emulator.find_nodes_by_role("dns-server"):
            dns_server_ips.append(dns_server.get_primary_ip_address_string(with_subnet_length=False))

        for interface in self.network_node.get_interfaces():
            if interface.is_management:
                continue
            if not interface.has_ip():
                continue
            lease = {}
            subnet = interface.get_subnet()
            routers = [router for router in self.network_node.network_emulator.find_nodes_in_subnet(subnet) if isinstance(router, WattsonNetworkRouter)]
            if len(routers) == 0:
                self.logger.error(f"No router found for subnet {str(subnet)} at interface {interface.interface_name}")
                continue

            router_ips = []
            for router in routers:
                for router_interface in router.get_interfaces_in_subnet(subnet):
                    router_ips.append(router_interface.ip_address_short_string)

            try:
                first_free_ip = self.network_node.network_emulator.get_unused_ip(subnet)
            except NetworkException:
                self.logger.error(f"Could not determine IP range as no free IP for subnet {str(subnet)}")
                return False

            first_used_ip = first_free_ip + 10
            if first_used_ip not in subnet:
                self.logger.error(f"Could not determine IP range as subnet is exhausted")
                return False
            last_used_ip = subnet[-2]
            if last_used_ip < first_used_ip:
                self.logger.error(f"Could not determine IP range as last ip is smaller than suggested first address")
                return False

            ip_range = [str(first_used_ip), str(last_used_ip)]

            lease["subnet"] = str(subnet.network_address)
            lease["netmask"] = str(subnet.netmask)
            lease["range"] = ip_range
            lease["routers"] = router_ips
            lease["dns_servers"] = dns_server_ips

            self._service_configuration["leases"].append(lease)
            self._service_configuration["interfaces"].append(interface.interface_name)

        return True
