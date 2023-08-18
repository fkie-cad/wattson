import dataclasses
import logging
from typing import Any, Optional, TYPE_CHECKING, ClassVar

from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.constants import DEFAULT_SEGMENT
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkEntity(NetworkEntity):
    id: str
    system_name: Optional[str] = None
    display_name: Optional[str] = None
    network_emulator: Optional['NetworkEmulator'] = None
    emulation_instance: Optional[Any] = None
    segment: str = DEFAULT_SEGMENT
    config: dict = dataclasses.field(default_factory=lambda: {})

    _is_started: bool = False
    logger: Optional[logging.Logger] = None

    class_id: ClassVar[int] = 0

    def __post_init__(self):
        self._numerical_id = None
        self._numerical_id = self.get_numerical_id()
        if self.system_name is None:
            self.system_name = self.generate_name()
        if self.id is None:
            self.id = self.system_name
        if self.display_name is None:
            self.display_name = self.generate_display_name()
        if self.logger is None:
            self.logger = get_logger(self.entity_id, self.entity_id)

    def generate_name(self):
        return f"{self.get_prefix()}{self.get_numerical_id()}"

    def generate_display_name(self) -> str:
        return f"{self.__class__.__name__.replace('WattsonNetwork', '')} {self.entity_id}"

    def get_prefix(self) -> str:
        return "e"

    @classmethod
    def get_class_id(cls):
        return cls.class_id

    @classmethod
    def set_class_id(cls, class_id):
        cls.class_id = class_id

    def get_numerical_id(self) -> int:
        if self._numerical_id is None:
            free_id = self.get_class_id()
            self.__class__.set_class_id(free_id + 1)
            self._numerical_id = free_id
        return self._numerical_id

    def start(self):
        if self._is_started:
            return
        self.start_emulation_instance()
        if self.network_emulator is not None:
            self.network_emulator.on_entity_start(self)
            self.network_emulator.on_topology_change(self, "entity_start")
        self._is_started = True

    def stop(self):
        if not self._is_started:
            return
        self._is_started = False
        self.stop_emulation_instance()
        if self.network_emulator is not None:
            self.network_emulator.on_entity_stop(self)
            self.network_emulator.on_topology_change(self, "entity_stop")

    def start_emulation_instance(self):
        if hasattr(self.emulation_instance, "start"):
            self.emulation_instance.start()
        if hasattr(self, "update_default_route"):
            self.update_default_route()

    def stop_emulation_instance(self):
        if hasattr(self.emulation_instance, "stop"):
            if hasattr(self.emulation_instance, "shell") and self.emulation_instance.shell is None:
                # Mininet Instance already exited
                return
            self.emulation_instance.stop()

    @property
    def is_started(self) -> bool:
        return self._is_started

    def get_namespace(self):
        return self.network_emulator.get_namespace(self)

    @property
    def entity_id(self) -> str:
        return str(id(self))

    @property
    def system_id(self) -> str:
        return self.system_name

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        """
        Creates a dictionary for synchronization with a RemoteNetworkEntity.
        @param force_state_synchronization: Whether to force a synchronization of the internal state with the actual state
        @return: A dictionary representation of this WattsonNetworkEntity for synchronization with a RemoteNetworkEntity.
        """
        return RemoteNetworkEntityRepresentation({
            "entity_id": self.entity_id,
            "system_id": self.system_id,
            "display_name": self.display_name,
            "id": self.id,
            "segment": self.segment,
            "config": self.config,
            "is_started": self._is_started,
            "class": self.__class__.__name__,
        })

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.entity_id == self.entity_id
        return False

    def __hash__(self):
        return hash(self.entity_id)
