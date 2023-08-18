import abc
from typing import Optional

from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost


class NetworkDockerHost(NetworkHost, abc.ABC):
    @staticmethod
    def get_prefix():
        return "h"

    @abc.abstractmethod
    def get_image_name(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_image_tag(self) -> Optional[str]:
        ...

    def get_full_image(self) -> Optional[str]:
        return f"{self.get_image_name()}:{self.get_image_tag()}"

    @abc.abstractmethod
    def get_boot_command(self) -> Optional[str]:
        ...
