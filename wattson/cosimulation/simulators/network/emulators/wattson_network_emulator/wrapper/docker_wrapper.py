import threading
import typing
from typing import TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.docker_namespace import DockerNamespace
from wattson.networking.namespaces.namespace import Namespace

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator


class DockerWrapper(NodeWrapper):
    def __init__(self, entity: NetworkEntity, emulator: 'WattsonNetworkEmulator'):
        super().__init__(entity, emulator)
        self._namespace_lock: threading.Lock = threading.Lock()
        self.docker.container_name = f"wattson.{self.docker.system_id}"
        self._namespace: Namespace = Namespace(f"w_{self.entity.entity_id}")

    @property
    def docker(self) -> WattsonNetworkDockerHost:
        return typing.cast(WattsonNetworkDockerHost, self.entity)

    def get_docker_pid(self) -> int:
        return self.docker.get_container_pid()

    def get_docker_namespace(self) -> DockerNamespace:
        return self.docker.get_namespace()

    def get_namespace(self) -> Namespace:
        with self._namespace_lock:
            if self._namespace.exists():
                return self._namespace
            if not self.is_container_running():
                raise RuntimeError("Cannot create namespace from non-existent container")
            self._namespace.from_pid(self.get_docker_pid())
            return self._namespace

    def is_container_running(self) -> bool:
        return self.docker.is_container_running()

    def create(self):
        # Check for image
        if not self.docker.is_valid_image():
            self.logger.error("Invalid docker image specified")
            return False
        if not self.docker.is_image_installed():
            if not self.docker.pull_image():
                self.logger.error(f"Could not pull image")
                return False
            self.logger.warning("Docker image is not installed - pulled it from registry")

        # Create Container
        if self.docker.create_container():
            self.docker.start_container()
            self.logger.debug(f"Created container {self.docker.container_name} from {self.docker.get_full_image()} (PID {self.docker.get_container_pid()})")
            return True
        self.logger.error(f"Could not create container {self.docker.container_name} from {self.docker.get_full_image()}")

        # Remove docker-specific interface

        return False

    def clean(self):
        if self.docker.container_exists():
            self.docker.remove_container(force=True)

        if self._namespace.exists():
            self._namespace.clean()
