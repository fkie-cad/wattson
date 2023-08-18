import shutil
import subprocess
from typing import TYPE_CHECKING, Optional, List, Callable

from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonPcapService(WattsonService):
    def __init__(self, interface: 'NetworkInterface', service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.interface = interface
        self.log_file: Optional[ArtifactRotate] = None
        self.pcap_file: Optional[ArtifactRotate] = None
        self._log_handle = None

    def get_start_command(self) -> List[str]:
        res, output = self.network_node.exec(["which", "tcpdump"])
        if res != 0 or len(output) != 1:
            raise FileNotFoundError(f"tcpdump cannot be found on node {self.network_node.entity_id} ({self.network_node.display_name})")
        tcpdump_path = output[0]
        # TODO: Handle pcap path for Docker and VM nodes
        return [tcpdump_path, "-n", "-K",
                "-i", self.interface.get_system_name(),
                "-w", str(self.pcap_file.get_current().absolute())]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonService'], None]] = None) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback=async_callback)
        self._clear_log_handle()
        return success

    def ensure_working_directory(self):
        super().ensure_working_directory()
        if self.log_file is None:
            self.log_file = ArtifactRotate(self.dir.joinpath(f"{self.network_node.entity_id}-service-{self.id}.log"))
            self._artifacts.append(self.log_file)
        if self.pcap_file is None:
            self.pcap_file = ArtifactRotate(self.dir.joinpath(f"{self.network_node.entity_id}-service-{self.id}-{self.interface.system_id}.pcap"))
            self._artifacts.append(self.pcap_file)

    def _clear_log_handle(self):
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None

    def get_stdout(self):
        self._clear_log_handle()
        self._log_handle = self.log_file.get_current().open("w")
        return self._log_handle

    def get_stderr(self):
        return subprocess.STDOUT
