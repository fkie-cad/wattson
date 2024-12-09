from typing import Optional

from wattson.cosimulation.models.facilities.facility_remote_representation import FacilityRemoteRepresentation
from wattson.cosimulation.models.model import Model
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation


class Facility(Model):
    @staticmethod
    def load_from_dict(model_dict: dict) -> 'Model':
        return Facility(model_dict.get("id"), model_dict.get("name"), model_dict.get("readable_name"), model_dict.get("type"))

    def __init__(self, facility_id: Optional[str] = None, facility_name: Optional[str] = None, readable_name: Optional[str] = None,
                 facility_type: Optional[str] = None):
        self._id = facility_id
        self._name = facility_name
        self._readable_name = readable_name
        self._facility_type = facility_type

    def get_model_type(self) -> str:
        return "facility"

    def get_id(self) -> str:
        return self._id

    def get_name(self) -> str:
        return self._name

    def get_readable_name(self) -> str:
        return self._readable_name

    def get_facility_type(self) -> str:
        return self._facility_type

    def to_remote_representation(self) -> WattsonRemoteRepresentation:
        return FacilityRemoteRepresentation({
            "type": "model",
            "model_type": self.get_model_type(),
            "model_class": self.__class__,
            "id": self._id,
            "name": self._name,
            "readable_name": self._readable_name,
            "facility_type": self._facility_type
        })
