import shlex
import socket
import subprocess
import sys
from pathlib import Path
from typing import Optional, Callable, Dict, List, Tuple, Union

from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.configuration import ServiceConfiguration
from wattson.services.wattson_service import WattsonService


class WattsonFrRoutingService(WattsonService):
    tmp_dir_by_host: Dict[str, Path] = {}

    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self._artifacts_by_name: Dict[str, ArtifactRotate] = {}
        self.frr_artifacts: List[ArtifactRotate] = []
        self.routing_path: Optional[Path] = None
        self.tmp_root: Optional[Path] = None
        self._auto_mount_tmp: bool = service_configuration.get("auto_mount_tmp", True)

    def get_extra_arguments(self) -> Dict:
        return {
            # "cwd": str(self.dir.absolute())
        }

    def _clear_symlinks(self):
        if self.tmp_root.exists() and self.tmp_root.is_dir():
            self.network_node.unmount(self.tmp_root, force=True)

    def get_tmp_path(self, artifact: ArtifactRotate) -> Path:
        self.ensure_working_directory()
        return self.tmp_root.joinpath(artifact.get_current().name)

    def pre_start(self):
        for artifact in self.frr_artifacts:
            artifact.rotate()

    def start(self) -> bool:
        if self.is_running():
            return True
        self.pre_start()
        return super().start()

    def write_fr_config_file(self):
        pass

    def is_running(self) -> bool:
        if super().is_running():
            if self.get_socket_file() is None:
                return True
            return self.can_listen()
        return False

    def get_socket_file(self) -> Optional[Path]:
        return None

    def can_listen(self):
        if self.get_socket_file() is None:
            return False
        return self.network_node.socket_exists(self.get_socket_file())

    def write_configuration(self):
        super().write_configuration()
        self.write_fr_config_file()

    def get_artifact(self, filename: str, is_folder: bool = False) -> ArtifactRotate:
        self.ensure_working_directory()
        if filename in self._artifacts_by_name:
            return self._artifacts_by_name[filename]
        # artifact = ArtifactRotate(Path("/tmp").joinpath(f"wattson_{self.network_node.system_name}_{filename}"), is_folder=is_folder)
        artifact = ArtifactRotate(self.routing_path.joinpath(f"wattson_{self.network_node.system_name}_{filename}"), is_folder=is_folder)
        self._artifacts_by_name[filename] = artifact
        return artifact

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonService'], None]] = None) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback=async_callback)
        self._clear_symlinks()
        return success

    def get_stderr(self):
        return self.get_stdout()

    def get_stdout(self):
        return self.get_log_handle()

    def ensure_working_directory(self):
        super().ensure_working_directory()

        self.routing_path = self.dir.joinpath("routing")
        self.routing_path.mkdir(0o777, exist_ok=True)
        self.routing_path.chmod(0o777)
        mount_routing_path = self.guest_dir.joinpath("routing")

        tmp_name = f"wattson_{self.network_node.system_name}"
        self.tmp_root = WattsonFrRoutingService.tmp_dir_by_host.get(tmp_name)

        if self.tmp_root is None:
            self.tmp_root = Path("/tmp").joinpath(tmp_name)
            self.network_node.logger.info(f"Binding {mount_routing_path} as {self.tmp_root}")
            self.network_node.mount(mount_point=self.tmp_root,
                                    target=mount_routing_path,
                                    remove_file=True,
                                    remove_folder=True,
                                    bind=True)
            WattsonFrRoutingService.tmp_dir_by_host[tmp_name] = self.tmp_root

