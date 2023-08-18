from typing import Optional

from wattson.cosimulation.simulators.network.components.interface.network_docker_host import NetworkDockerHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost


class RemoteNetworkDockerHost(RemoteNetworkHost, NetworkDockerHost):

    def get_image_name(self) -> Optional[str]:
        return self._state.get("image_name")

    def get_image_tag(self) -> Optional[str]:
        return self._state.get("image_tag")

    def get_boot_command(self) -> str:
        """
        @return: The boot command to start the container with.
        """
        return self._state.get("boot_command")
