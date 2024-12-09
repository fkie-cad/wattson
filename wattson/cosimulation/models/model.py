import abc

from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation


class Model(abc.ABC):
    @abc.abstractmethod
    def get_model_type(self) -> str:
        ...

    @abc.abstractmethod
    def get_id(self) -> str:
        ...

    @staticmethod
    @abc.abstractmethod
    def load_from_dict(model_dict: dict) -> 'Model':
        ...

    @abc.abstractmethod
    def to_remote_representation(self) -> WattsonRemoteRepresentation:
        ...

    def __hash__(self):
        return id(self)
