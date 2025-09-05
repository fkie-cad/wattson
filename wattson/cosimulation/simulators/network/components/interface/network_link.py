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

    def get_other_interface(self, interface: 'NetworkInterface') -> 'NetworkInterface':
        """
        Given a valid interface, returns the other interface connected forming this link.
        In case the given interface is not part of this link, interface A is returned.

        Args:
            interface ('NetworkInterface'):
                One interface that forms the link

        Returns:
            'NetworkInterface': The other interface forming the link.
        """
        if self.get_interface_a() == interface:
            return self.get_interface_b()
        return self.get_interface_a()

    @abc.abstractmethod
    def get_link_state(self) -> dict:
        ...

    @abc.abstractmethod
    def is_up(self) -> bool:
        ...

    @abc.abstractmethod
    def up(self):
        """Set the link state to "up", i.e., enabling the link"""
        ...

    @abc.abstractmethod
    def down(self):
        """Set the link state to "down", i.e., disabling the link"""
        ...
