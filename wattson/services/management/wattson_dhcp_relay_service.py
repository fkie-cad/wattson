import shlex
from typing import Optional, Callable, List

from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger


class WattsonDhcpRelayService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.service_configuration = service_configuration
        self.logger = get_logger("DHCP Relay", "DHCP Relay")

    def get_start_command(self) -> List[str]:
        return [self.network_node.get_python_executable(), "scripts/start-relay.py", self.service_configuration["server_ip"], self.service_configuration["downstream_intf"], self.service_configuration["upstream_intf"]]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback)
        self.network_node.exec("service isc-dhcp-relay stop", shell=True)
        return success
