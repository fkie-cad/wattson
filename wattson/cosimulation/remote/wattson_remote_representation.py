from typing import TYPE_CHECKING, Any

from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class WattsonRemoteRepresentation(dict):
    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> WattsonRemoteObject:
        raise NotImplementedError()

    def resolve(self, wattson_client: 'WattsonClient'):
        for key, value in self.items():
            self[key] = self._resolve_remote_objects(value, wattson_client=wattson_client)

    def _resolve_remote_objects(self, remote_object, wattson_client: 'WattsonClient') -> Any:
        if isinstance(remote_object, WattsonRemoteRepresentation):
            return remote_object.to_wattson_remote_object(wattson_client=wattson_client)
        if isinstance(remote_object, dict):
            return {key: self._resolve_remote_objects(value, wattson_client=wattson_client)
                    for key, value in remote_object.items()}
        if isinstance(remote_object, list):
            return [self._resolve_remote_objects(item, wattson_client=wattson_client) for item in remote_object]
        return remote_object
