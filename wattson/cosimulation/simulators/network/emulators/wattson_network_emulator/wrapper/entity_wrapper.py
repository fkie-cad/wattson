import abc
from typing import TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.networking.namespaces.namespace import Namespace
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator


class EntityWrapper(abc.ABC):
    def __init__(self, entity: NetworkEntity, emulator: 'WattsonNetworkEmulator'):
        self.entity = entity
        self.emulator: 'WattsonNetworkEmulator' = emulator
        self._virtual_machine_namespace = None
        self.logger = get_logger(f"{entity.entity_id}Wrapper", f"{entity.entity_id}Wrapper")

    @abc.abstractmethod
    def get_namespace(self) -> Namespace:
        ...

    def get_additional_namespace(self) -> Namespace:
        return self.get_namespace()

    def has_additional_namespace(self) -> bool:
        return self.get_additional_namespace() != self.get_namespace()

    def create(self) -> bool:
        return True

    def clean(self):
        pass
