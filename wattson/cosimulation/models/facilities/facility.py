from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from cosimulation.models.model_manager import ModelManager
    from powerowl.layers.powergrid import PowerGridModel
    from powerowl.layers.powergrid.elements import GridElement

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

    def __hash__(self):
        return self._id.__hash__()

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

    def get_power_grid_neighbors(self, power_grid_model: 'PowerGridModel', model_manager: 'ModelManager') -> List['Facility']:
        neighbor_ids = set()
        for grid_element in self.get_facility_grid_elements(power_grid_model):
            # This follows edges
            for bus in grid_element.get_buses():
                neighbor_ids.add(str(bus.get_data("facility_id")))
        if self.get_id() in neighbor_ids:
            neighbor_ids.remove(self.get_id())
        neighbor_ids = [n for n in neighbor_ids if n is not None]
        neighbors = [model_manager.get_model(self.get_model_type(), neighbor_id) for neighbor_id in neighbor_ids]
        neighbors = [f for f in neighbors if isinstance(f, Facility)]
        return neighbors

    def get_facility_grid_elements(self, power_grid_model: 'PowerGridModel') -> List['GridElement']:
        grid_elements = []
        for grid_element in power_grid_model.get_elements():
            for bus in grid_element.get_buses():
                if str(bus.get_data("facility_id")) == self.get_id():
                    grid_elements.append(grid_element)
        return grid_elements
