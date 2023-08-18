import json
import time
from typing import TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class RemoteNetworkEntity(WattsonRemoteObject, NetworkEntity):
    """
    A remote representation of a WattsonNetworkEntity
    """
    def __init__(self, entity_id: str, wattson_client: 'WattsonClient', auto_sync: bool = True):
        self._entity_id = entity_id
        self._wattson_client = wattson_client
        self.last_synchronization = 0
        self._synchronization_interval = 60
        self._state = {}
        logger_name = f"{self.__class__.__name__}.{self.entity_id}"
        self.logger = get_logger(logger_name, logger_name)
        if auto_sync:
            self.synchronize()

    @property
    def id(self):
        return self._state.get("id")

    def synchronize(self, force: bool = False, block: bool = True):
        if not force and time.time() - self.last_synchronization < self._synchronization_interval:
            return

        def handle_response(_response: WattsonResponse):
            if _response.is_successful():
                self.update_from_remote_representation(_response.data["entity"])
            else:
                error = _response.data.get("error")
                self.logger.error(f"Failed to synchronize with server {error=}")

        query = WattsonNetworkQuery(WattsonNetworkQueryType.GET_ENTITY, query_data={"entity_id": self.entity_id})
        if block:
            resp = self._wattson_client.query(query, block=True)
            handle_response(_response=resp)
        else:
            promise = self._wattson_client.async_query(query)
            promise.on_resolve(callback=handle_response)

    def update_from_remote_representation(self, remote_representation: RemoteNetworkEntityRepresentation) -> bool:
        self.last_synchronization = time.time()
        remote_representation.resolve(self._wattson_client)
        self._state = remote_representation
        return True

    @property
    def state(self) -> dict:
        return self._state

    @property
    def entity_id(self) -> str:
        return self._entity_id

    @property
    def system_id(self) -> str:
        return self.state.get("system_id", self.entity_id)

    @property
    def display_name(self) -> str:
        return self.state.get("display_name", self.entity_id)

    @property
    def is_started(self) -> bool:
        return self.state.get("is_started", False)

    def start(self):
        """
        Start the WattsonNetworkEntity
        @return:
        """
        # TODO: Start the actual node

    def stop(self):
        """
        Stop the WattsonNetworkEntity
        @return:
        """
        # TODO
