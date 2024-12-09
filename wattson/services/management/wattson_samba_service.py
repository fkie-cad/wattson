import time

from wattson.services.wattson_service import WattsonService

from typing import Optional, Callable, TYPE_CHECKING, List

from wattson.util.performance.performance_decorator import performance_assert

if TYPE_CHECKING:
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonSambaService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.config = service_configuration
        self._last_status_call = 0
        self._last_status = None
        self._status_timeout = 30

    def start(self, refresh_config: bool = False):
        code_smb, _ = self.network_node.exec(["service", "smbd", "restart"])
        code_nmb, _ = self.network_node.exec(["service", "nmbd", "restart"])
        self._last_status_call = code_nmb == 0 == code_smb
        self._last_status_call = time.time()

    @performance_assert(1)
    def update_is_running(self):
        if not self.network_node.is_started:
            self._last_status = False
            return
        # self.network_node.logger.info(f"Updating Samba service status {self.name} // {self.id}")
        code_smbd, _ = self.network_node.exec(["service", "smbd", "status"])
        code_nmbd, _ = self.network_node.exec(["service", "nmbd", "status"])
        self._last_status = code_smbd == 0 == code_nmbd
        self._last_status_call = time.time()

    def update(self):
        self.update_is_running()

    @performance_assert(1)
    def is_running(self) -> bool:
        if self._last_status is not None:
            return self._last_status
        self.update_is_running()
        return self._last_status

    def kill(self) -> bool:
        return self.stop()

    def get_start_command(self) -> List[str]:
        return []
        # return ["/usr/sbin/sshd"]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        # self._last_status_call = 0
        code_nmb, _ = self.network_node.exec(["service", "nmbd", "stop"])
        code_smb, _ = self.network_node.exec(["service", "smbd", "stop"])
        self._last_status = not (code_smb == 0 == code_nmb)
        self._last_status_call = time.time()
        return code_smb == 0 == code_nmb
