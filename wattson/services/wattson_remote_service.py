from pathlib import Path
from typing import Optional, Callable, List, Dict, TYPE_CHECKING, Union, Type

import time
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.exceptions import ServiceException
from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.services.configuration import ServiceConfiguration
from wattson.services.service_priority import ServicePriority
from wattson.services.wattson_remote_service_representation import WattsonRemoteServiceRepresentation
from wattson.services.wattson_service import WattsonService
from wattson.services.wattson_service_interface import WattsonServiceInterface

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class WattsonRemoteService(WattsonRemoteObject, WattsonServiceInterface):
    """A remote representation for a WattsonService"""
    def __init__(self, wattson_client: 'WattsonClient', service_id: Optional[int] = None, auto_sync: bool = True):
        self._configuration: Optional[ServiceConfiguration] = None
        self._expanded_configuration: Optional[ServiceConfiguration] = None
        self._deployment_class_name: Optional[str] = None

        self._id = service_id
        self.wattson_client = wattson_client
        self._state = {}
        self._sync_time: float = 0
        self._sync_interval = 10
        if self.id is not None and auto_sync:
            self.synchronize(True)

    def call(self, method, **kwargs):
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "call",
                "params": {"method": method, "kwargs": kwargs}
            }
        )
        self.wattson_client.query(query)
        if query.response.is_successful():
            self.sync_from_remote_representation(query.response.data.get("service"))
        return query.response.is_successful()

    def callable_methods(self) -> dict[str, dict]:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "callable_methods",
                "params": {}
            }
        )
        self.wattson_client.query(query)
        if query.response.is_successful():
            self.sync_from_remote_representation(query.response.data.get("service"))
        return query.response.data

    @property
    def configuration(self) -> Optional[ServiceConfiguration]:
        return self._configuration

    @property
    def expanded_configuration(self) -> Optional[ServiceConfiguration]:
        return self._expanded_configuration
    
    @property
    def deployment_class_name(self) -> Optional[str]:
        return self._deployment_class_name

    @property
    def id(self):
        return self._id

    @property
    def name(self) -> str:
        return self._state.get("name", self.__class__.__name__)

    def connected(self) -> bool:
        """
        Whether this service is connected to a WattsonService (i.e., whether the associated WattsonService exists)


        Returns:
            bool: True iff the associated WattsonService exists.
        """
        return self.synchronize()

    def synchronize(self, force: bool = False, block: bool = True):
        """
        Synchronizes the service state with the actual service.

        Args:
            force (bool, optional):
                
                (Default value = False)
            block (bool, optional):
                
                (Default value = True)
        """
        if not force and time.time() - self._sync_time < self._sync_interval:
            return True
        query = WattsonNetworkQuery(query_type=WattsonNetworkQueryType.GET_SERVICE, query_data={"service_id": self.id})
        response = self.wattson_client.query(query)
        if response.is_successful():
            return self.sync_from_remote_representation(response.data["service"])
        return False

    def _on_service_state_change(self, notification: WattsonNotification):
        """
        Callback for Notifications on service state changes.

        Args:
            notification (WattsonNotification):
                
        """
        if notification.notification_topic == WattsonNetworkNotificationTopic.SERVICE_EVENT:
            service_info = notification.notification_data.get("service", {})
            if service_info.get("service_id") == self.id:
                self.sync_from_remote_representation(service_info)

    def get_start_command(self) -> List[str]:
        return self._state.get("command")

    def get_priority(self) -> ServicePriority:
        self.synchronize()
        return self._state.get("priority")

    def start(self, refresh_config: bool = False) -> bool:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "start",
                "params": {
                    "refresh_config": refresh_config
                }
            }
        )
        self.wattson_client.query(query)
        if query.response.is_successful():
            self.sync_from_remote_representation(query.response.data.get("service"))
        return query.response.is_successful()

    def stop(self, wait_seconds: float = 5, auto_kill: bool = True, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "stop",
                "params": {
                    "wait_seconds": wait_seconds,
                    "auto_kill": auto_kill
                }
            }
        )
        if async_callback is None:
            self.wattson_client.query(query)
            if query.response.is_successful():
                self.sync_from_remote_representation(query.response.data.get("service"))
            return query.response.is_successful()
        else:
            def callback(response):
                if response.is_successful():
                    self.sync_from_remote_representation(response.data.get("service"))
                async_callback(self)

            promise = self.wattson_client.async_query(query)
            promise.on_resolve(callback)
            return True

    def restart(self, refresh_config: bool = False) -> bool:
        return self.stop() and self.start(refresh_config=refresh_config)

    def kill(self) -> bool:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "kill",
                "params": {}
            }
        )
        response = self.wattson_client.query(query)
        return response.is_successful()

    def is_running(self) -> bool:
        self.synchronize()
        return self._state.get("is_running")

    def is_killed(self) -> bool:
        self.synchronize()
        return self._state.get("is_killed")

    def get_pid(self) -> Optional[int]:
        self.synchronize()
        return self._state.get("pid")

    def poll(self) -> Optional[int]:
        pass

    def wait(self, timeout: Optional[float] = None) -> int:
        pass

    def sync_from_remote_representation(self, service_info: WattsonRemoteServiceRepresentation) -> bool:
        """
        Synchronize internal state from API information.

        Args:
            service_info (WattsonRemoteServiceRepresentation):
                API information of the WattsonService
        """
        if service_info is None:
            return False
        if service_info["service_id"] != self.id:
            raise ServiceException(f"Service ID mismatch: Expected {self.id}, got {service_info['service_id']}")
        self._state = service_info
        self._sync_time = time.time()
        priority_representation: WattsonRemoteRepresentation = service_info["priority"]
        self._state["priority"] = priority_representation.to_wattson_remote_object(wattson_client=self.wattson_client)
        self._configuration = ServiceConfiguration(service_info["configuration"])
        self._expanded_configuration = ServiceConfiguration(service_info["expanded_configuration"])
        return True

    def get_info(self) -> Dict:
        self.synchronize()
        return {
            "Service-ID": self.id,
            "Name": self.name,
            "Running": self.is_running(),
            "Command": " ".join(self.get_start_command()),
            "Priority": self.get_priority().get_global()
        }

    def expand_configuration(self) -> ServiceConfiguration:
        return self._state.get("expanded_configuration", ServiceConfiguration())

    def get_configuration(self) -> ServiceConfiguration:
        return self._state.get("configuration", ServiceConfiguration())

    def set_configuration(self, configuration: Union[Dict, ServiceConfiguration]) -> bool:
        self._state["configuration"] = configuration
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.SERVICE_ACTION,
            query_data={
                "service_id": self.id,
                "action": "update_service_configuration",
                "params": {
                    "configuration": configuration
                }
            }
        )
        response = self.wattson_client.query(query)
        if response.is_successful():
            self.sync_from_remote_representation(response.data["service"])
            return True
        return False

    def from_service_configuration(self, deployment_class_path: str, configuration: ServiceConfiguration) -> bool:
        self._deployment_class_name = deployment_class_path
        self._configuration = configuration
        return True

    def get_artifact_paths(self) -> Dict[str, Path]:
        self.synchronize()
        return self._state.get("artifact_paths", {})

    def get_original_class(self) -> Type[WattsonService]:
        return self._state.get("original_class", WattsonService)
