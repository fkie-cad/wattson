from typing import TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation

if TYPE_CHECKING:
    from wattson.services.wattson_remote_service import WattsonRemoteService
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class WattsonRemoteServiceRepresentation(WattsonRemoteRepresentation):
    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> 'WattsonRemoteService':
        from wattson.services.wattson_remote_service import WattsonRemoteService
        service = WattsonRemoteService(wattson_client=wattson_client, service_id=self["service_id"], auto_sync=False)
        service.sync_from_remote_representation(self)
        return service
