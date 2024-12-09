import abc

from wattson.cosimulation.models.model import Model
from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation


class RemoteModel(WattsonRemoteObject, Model, abc.ABC):
    def synchronize(self, force: bool = False, block: bool = True):
        pass

    @staticmethod
    def load_from_dict(model_dict: dict) -> 'Model':
        pass

    @abc.abstractmethod
    def update_from_remote_representation(self, remote_representation: WattsonRemoteRepresentation):
        ...
