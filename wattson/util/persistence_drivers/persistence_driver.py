import abc
from typing import List, Any, Dict, Optional


class PersistenceDriver(abc.ABC):
    def __init__(self, clear: bool = False):
        if clear:
            self.clear()

    @abc.abstractmethod
    def create_domain(self, domain: str, keys: List[str], indices: Optional[List[str]] = None):
        ...

    @abc.abstractmethod
    def store(self, domain: str, values: Dict[str, Any]):
        ...

    @abc.abstractmethod
    def delete(self, domain: str, search: Dict[str, Any]):
        ...

    @abc.abstractmethod
    def get_all(self, domain: str, order: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def search(self, domain: str, search: Dict[str, Any], order: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def get_one(self, domain: str, search: Dict[str, Any], order: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def clear(self):
        pass
