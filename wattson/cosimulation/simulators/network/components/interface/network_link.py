import abc
from typing import TYPE_CHECKING


from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.network_link_model import NetworkLinkModel

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.interface.network_interface import NetworkInterface


class NetworkLink(NetworkEntity, abc.ABC):
    @abc.abstractmethod
    def get_link_model(self) -> NetworkLinkModel:
        ...

    @abc.abstractmethod
    def get_interface_a(self) -> 'NetworkInterface':
        ...

    @abc.abstractmethod
    def get_interface_b(self) -> 'NetworkInterface':
        ...

    @abc.abstractmethod
    def get_link_state(self) -> dict:
        ...

    @abc.abstractmethod
    def is_up(self) -> bool:
        ...

    @abc.abstractmethod
    def up(self):
        """
        Set the link state to "up", i.e., enabling the link
        @return:
        """
        ...

    @abc.abstractmethod
    def down(self):
        """
        Set the link state to "down", i.e., disabling the link
        @return:
        """
        ...
