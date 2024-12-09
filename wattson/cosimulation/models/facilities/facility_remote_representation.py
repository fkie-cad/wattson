from typing import TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient
    from wattson.cosimulation.models.facilities.remote_facility import RemoteFacility


class FacilityRemoteRepresentation(WattsonRemoteRepresentation):
    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> 'RemoteFacility':
        from wattson.cosimulation.models.facilities.remote_facility import RemoteFacility
        facility = RemoteFacility()
        facility._id = self.get("id")
        facility._name = self.get("name")
        facility._facility_type = self.get("facility_type")
        facility._readable_name = self.get("readable_name")
        return facility
