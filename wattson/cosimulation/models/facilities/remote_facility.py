from wattson.cosimulation.models.facilities.facility import Facility
from wattson.cosimulation.models.remote_model import RemoteModel
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation


class RemoteFacility(RemoteModel, Facility):
    def synchronize(self, force: bool = False, block: bool = True):
        pass

    def update_from_remote_representation(self, remote_representation: WattsonRemoteRepresentation):
        if remote_representation.get("model_class") != Facility:
            return False
        self._id = remote_representation.get("id")
        self._name = remote_representation.get("name")
        self._facility_type = remote_representation.get("facility_type")
        self._readable_name = remote_representation.get("readable_name")
