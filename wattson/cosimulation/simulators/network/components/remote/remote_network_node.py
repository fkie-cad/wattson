import time
import typing
from typing import Dict, Optional, List, Union

from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.services.wattson_remote_service import WattsonRemoteService

if typing.TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class RemoteNetworkNode(RemoteNetworkEntity, NetworkNode):
    def __init__(self, entity_id: str, wattson_client: 'WattsonClient', auto_sync: bool = True):
        super().__init__(entity_id, wattson_client, auto_sync)

    def exec(self, cmd: Union[list[str], str]):
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "entity_id": self.entity_id,
                "action": "exec",
                "value": cmd
            }
        )
        response = self._wattson_client.query(query)
        if response.is_successful():
            return response.data["code"], response.data["lines"]
        # check for error details
        self.logger.error(f"{response.data=}")
        self.logger.error(f"Couldn't execute command: {cmd}")
        return -1, []

    def start_services(self):
        """
        Start all services associated with this WattsonNetworkEntity
        @return:
        """
        self.synchronize()
        super().start_services()

    def stop_services(self):
        """
        Stop all services associated with this WattsonNetworkEntity
        @return:
        """
        self.synchronize()
        super().stop_services()

    def start(self):
        query = WattsonNetworkQuery(WattsonNetworkQueryType.NODE_ACTION,
                                    query_data={
                                        "entity_id": self.entity_id,
                                        "action": "start"
                                    })
        response = self._wattson_client.query(query)
        self.synchronize()
        return response.is_successful()

    def stop(self):
        query = WattsonNetworkQuery(WattsonNetworkQueryType.NODE_ACTION,
                                    query_data={
                                        "entity_id": self.entity_id,
                                        "action": "stop"
                                    })
        response = self._wattson_client.query(query)
        self.synchronize()
        return response.is_successful()

    def has_services(self) -> bool:
        self.synchronize()
        return super().has_services()

    def add_interface(self, interface: 'RemoteNetworkInterface'):
        self.logger.warning("Cannot add interface to RemoteNetworkNode")
        pass

    def get_interfaces(self) -> List['RemoteNetworkInterface']:
        self.synchronize()
        return self.state.get("interfaces")

    def get_config(self) -> dict:
        return self.state.get("config", {})

    def update_config(self, config):
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.UPDATE_NODE_CONFIGURATION,
            query_data={
                "entity_id": self.entity_id,
                "config": config
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            e = response.data.get("error")
            self.logger.error(f"Could not update config. {e=}")
            return
        self.state["config"] = response.data["config"]

    def get_roles(self) -> List[str]:
        self.synchronize()
        return self.state.get("roles", [])

    def delete_role(self, role: str):
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "action": "delete-role",
                "entity_id": self.entity_id,
                "role": role
            }
        )
        self._wattson_client.query(query)
        self.synchronize(force=True)

    def has_role(self, role: str):

        return role in self.get_roles()

    def add_role(self, role: str):
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "action": "add-role",
                "entity_id": self.entity_id,
                "role": role
            }
        )
        self._wattson_client.query(query)
        self.synchronize(force=True)


    def add_service(self, service: WattsonRemoteService):
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.ADD_SERVICE,
            query_data={
                "entity_id": self.entity_id,
                "configuration": service.configuration,
                "deployment_class": service.deployment_class_name,
                "service_type": "python"
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Cannot add service: {error=}")
            return False
        service_data = response.data.get("service")
        service._id = service_data.get("service_id")
        self.state.setdefault("services", {})[service.id] = service
        return service.sync_from_remote_representation(service_data)

    def get_services(self) -> Dict[int, WattsonRemoteService]:
        self.synchronize()
        return self.state.get("services")

    def get_service(self, service_id: int) -> WattsonRemoteService:
        return typing.cast(WattsonRemoteService, super().get_service(service_id))

    def start_pcap(self, interface: Optional['RemoteNetworkInterface'] = None) -> List['WattsonRemoteService']:
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "entity_id": self.entity_id,
                "action": "start_pcap",
                "interface": interface.entity_id if interface is not None else None
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", None)
            self.logger.warning(f"Failed to start_pcap: {error=}")
        service_ids = response.data.get("services", [])
        self.synchronize(force=True)
        return [self.get_service(service_id) for service_id in service_ids]

    def stop_pcap(self, interface: Optional['RemoteNetworkInterface'] = None):
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "entity_id": self.entity_id,
                "action": "stop_pcap",
                "interface": interface.entity_id if interface is not None else None
            }
        )
        response = self._wattson_client.query(query)
        self.synchronize(force=True)
        if not response.is_successful():
            error = response.data.get("error", None)
            self.logger.warning(f"Failed to stop_pcap: {error=}")

    def open_terminal(self) -> bool:
        """
        Attempts to open a terminal / konsole for this node (on the simulation server)
        @return: Whether a terminal could be opened.
        """
        query = WattsonNetworkQuery(
            WattsonNetworkQueryType.NODE_ACTION,
            query_data={
                "entity_id": self.entity_id,
                "action": "open_terminal"
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", None)
            self.logger.warning(f"Failed to open terminal: {error=}")
            return False
        return True
