import time
from typing import Optional, Callable, TYPE_CHECKING, List

from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.services.wattson_service_interface import WattsonServiceInterface
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonMultiService(WattsonService):
    """A wrapper to manage multiple individual services at once"""
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode', services: Optional[List[WattsonService]] = None):
        super().__init__(service_configuration=service_configuration, network_node=network_node)
        self._sub_services: List[WattsonService] = services if services is not None else []
        self.max_wait = service_configuration.get("max_wait", 10)

    def is_running(self) -> bool:
        for service in self._sub_services:
            if not service.is_running():
                return False
        return True

    def is_killed(self) -> bool:
        for service in self._sub_services:
            if service.is_killed():
                return True
        return False

    def start(self, refresh_config: bool = False) -> bool:
        self.ensure_artifacts()
        success = True
        for service in self._sub_services:
            success &= service.start(refresh_config=refresh_config)
            timeout = time.time() + self.max_wait
            while not service.is_running() and time.time() < timeout:
                # Busy wait
                time.sleep(0.05)
            if not service.is_running():
                self.network_node.logger.warning(f"Could not start sub-service {service.name}")
                success = False
        return success

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = True
        for service in self._sub_services:
            success &= service.stop(wait_seconds=wait_seconds,
                                    auto_kill=auto_kill)
        if async_callback is not None:
            async_callback(self)
        return success
