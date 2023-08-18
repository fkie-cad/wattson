import abc
from typing import Optional


class NetworkEntity(abc.ABC):
    id: str
    system_name: Optional[str] = None

    @property
    @abc.abstractmethod
    def entity_id(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def system_id(self) -> str:
        ...

    @abc.abstractmethod
    def start(self) -> bool:
        ...

    @abc.abstractmethod
    def stop(self) -> bool:
        ...

    def restart(self) -> bool:
        return self.stop() and self.start()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.entity_id == self.entity_id
        return False
