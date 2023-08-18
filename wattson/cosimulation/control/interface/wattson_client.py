import math
import queue
import threading
import time
from typing import Optional, Any, Callable, Dict, Union, List, TYPE_CHECKING

import zmq

from wattson.cosimulation.control.constants import SIM_CONTROL_PORT, SIM_CONTROL_PUBLISH_PORT, SIM_CONTROL_ID
from wattson.cosimulation.control.interface.publish_client import PublishClient
from wattson.cosimulation.control.messages.failed_query_response import FailedQueryResponse
from wattson.cosimulation.control.messages.wattson_async_response import WattsonAsyncResponse
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.control.messages.wattson_response_promise import WattsonResponsePromise
from wattson.cosimulation.exceptions import WattsonClientException
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_factory import RemoteNetworkEntityFactory
from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.services.wattson_remote_service import WattsonRemoteService
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
    from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
    from wattson.time.wattson_time import WattsonTime
    from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator


class WattsonClient(threading.Thread):
    def __init__(self,
                 query_server_socket_string: str = f"tcp://127.0.0.1:{SIM_CONTROL_PORT}",
                 publish_server_socket_string: str = f"tcp://127.0.0.1:{SIM_CONTROL_PUBLISH_PORT}",
                 client_name: str = "generic-client",
                 namespace: Optional[Union[str, Namespace]] = None,
                 wait_for_namespace: bool = False):
        super().__init__()
        self.logger = get_logger(self.__class__.__name__, self.__class__.__name__)
        self._query_socket_string = query_server_socket_string
        self._publish_socket_string = publish_server_socket_string
        self._termination_requested = threading.Event()
        self._started_event = threading.Event()
        self._start_lock = threading.Lock()
        if isinstance(namespace, str):
            if namespace == "auto":
                namespace = Namespace(f"w_{SIM_CONTROL_ID}")
            else:
                namespace = None
        self._wait_for_namespace = wait_for_namespace

        if isinstance(namespace, Namespace):
            if not namespace.exists() and not wait_for_namespace:
                raise WattsonClientException("Requested namespace does not exist")
        self._namespace = namespace
        self._publish_client = PublishClient(socket_string=self._publish_socket_string, namespace=self._namespace,
                                             on_receive_notification_callback=self.on_receive_notification)
        self._query_queue = queue.Queue()
        self._queue_timeout = 1
        self._registered: bool = False
        self._subscriptions = {}
        self._client_name = client_name
        self._client_id: Optional[str] = None

        self._events: Dict[str, threading.Event] = {}
        self._event_lock = threading.RLock()

        # REMOTE OBJECTS
        self._remote_services: Dict[int, WattsonRemoteService] = {}

        self._async_queries: Dict[int, WattsonQuery] = {}

        self.subscribe(WattsonNotificationTopic.EVENTS, self._handle_event_notification)
        self.subscribe(WattsonNotificationTopic.ASYNC_QUERY_RESOLVE, self._handle_async_query)

    @property
    def client_id(self) -> Optional[str]:
        return self._client_id

    @property
    def name(self) -> str:
        return self._client_name

    def start(self) -> None:
        with self._start_lock:
            if self._started_event.is_set():
                return
            # Optionally wait for namespace to exist
            if self._wait_for_namespace:
                while not self._namespace.exists() and not self._termination_requested.is_set():
                    time.sleep(0.5)
                if self._termination_requested.is_set():
                    return
            self._started_event.set()
            self._termination_requested.clear()
            self._publish_client.start()
            super().start()

    def stop(self, timeout: Optional[float] = None):
        self._started_event.clear()
        self._termination_requested.set()
        self._publish_client.stop(timeout=timeout)
        if self.is_alive():
            self.join(timeout=timeout)
        self._registered = False
        self._client_id = None

    def register(self, client_name: Optional[str] = None) -> bool:
        """
        Registers this client with the server based on the given ID, indicating its connection and availability.
        Returns True, iff the registration is acknowledged.
        """
        if self.is_registered:
            self.logger.info(f"Already registered as {self.client_id}. Requesting new ID.")

        if client_name is not None:
            self._client_name = client_name
        if self._client_name is None:
            raise ValueError("No client_name specified, cannot register to server")
        query = WattsonQuery(query_type=WattsonQueryType.REGISTRATION, query_data={"client_name": self._client_name})
        resp = self.query(query)
        self._registered = resp.is_successful()
        if not self._registered:
            self.logger.error(f"Could not register with {query.query_type=} {query.query_data=} - {resp.data=}")
        else:
            self._client_id = resp.data.get("client_id")
        return self.is_registered

    def require_connection(self, timeout_seconds: Optional[float] = None) -> bool:
        """
        Waits for the connection to the server to be established by repeatedly sending ECHO requests.
        When a timeout is given, the attempt to connect is stopped after this interval.
        When the connection is not established after the timeout, a TimeoutError is raised.
        Otherwise, the function returns with no return value or Exception.

        This function regularly checks for termination requests and stops its blocking behavior accordingly.
        """
        interval_seconds = 1
        if timeout_seconds is not None:
            seconds = math.ceil(timeout_seconds)
            interval_seconds = timeout_seconds / seconds
        start_time = time.time()
        while not self._termination_requested.is_set():
            echo_query = WattsonQuery(WattsonQueryType.ECHO)
            promise: WattsonResponsePromise = self.async_query(echo_query)
            if promise.resolve(interval_seconds):
                return True
            if timeout_seconds is not None:
                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout_seconds:
                    # Outer Timeout exceeded, break loop and raise TimeoutError
                    break
        if not self._termination_requested.is_set():
            raise TimeoutError(f"Connection not established after {timeout_seconds} seconds")
        return False

    @property
    def is_started(self) -> bool:
        return self._started_event.is_set()

    @property
    def is_registered(self) -> bool:
        return self._registered

    def run(self) -> None:
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as context:
            with context.socket(zmq.REQ) as socket:
                self.logger.info(f"Connecting to {self._query_socket_string}")
                with socket.connect(self._query_socket_string) as socket_context:
                    while not self._termination_requested.is_set():
                        try:
                            request = self._query_queue.get(block=True, timeout=self._queue_timeout)
                        except queue.Empty:
                            continue
                        query: WattsonQuery = request["query"]
                        event: threading.Event = request["event"]
                        try:
                            socket.send_pyobj(query)
                            # Poll socket for answer, but be interruptable by the termination event
                            while not socket.poll(0.5):
                                if self._termination_requested.is_set():
                                    query.add_response(WattsonResponse(successful=False, data={"error": "Socket Timeout due to shutdown"}))
                                    event.set()
                                    # Exit
                                    return
                            resp = socket.recv_pyobj()
                            if isinstance(resp, WattsonAsyncResponse):
                                # Build promise from AsyncResponse
                                promise = WattsonResponsePromise(query=query, resolve_event=event)
                                self._async_queries[resp.reference_id] = query
                                query.add_response(promise)
                            else:
                                query.add_response(resp)
                                event.set()
                        except Exception as e:
                            self.logger.error(f"{e=}")
                        finally:
                            continue

    def async_query(self, query: WattsonQuery) -> WattsonResponsePromise:
        """
        Performs a query, but explicitly does not block.
        Shorthand with correct type hint for query(query, block=False)
        """
        response = self.query(query, block=False)
        if isinstance(response, WattsonResponsePromise):
            return response
        raise TypeError(f"Expected a WattsonResponsePromise, got {response.__class__.__name__} instead")

    def query(self, query: WattsonQuery, block: bool = True) -> WattsonResponse:
        """
        Issues a query to the simulation control server, returning the response.
        :param query:
        :param block: Whether to block until the response arrives
        :return: The WattsonResponse returned by the server. This can also be a promise.
        """
        self.start()
        event = threading.Event()
        query.client_id = self._client_id
        request = {
            "event": event,
            "query": query
        }
        self._query_queue.put(request)
        if block:
            event.wait()
        else:
            return WattsonResponsePromise(query, event)

        if not query.has_response():
            return FailedQueryResponse()
        return query.response

    def primitive_query(self, query_type: str, query_data: Any, block: bool = True) -> WattsonResponse:
        """
        Builds and issues a basic WattsonQuery with a given query type and a given data record (e.g., a dict)
        :param query_type:
        :param query_data:
        :param block:
        :return:
        """
        query = WattsonQuery()
        query.query_type = query_type
        query.query_data = query_data
        return self.query(query, block=block)

    def _handle_async_query(self, notification: WattsonNotification):
        if notification.notification_topic == WattsonNotificationTopic.ASYNC_QUERY_RESOLVE:
            reference_id = notification.notification_data.get("reference_id")
            response = notification.notification_data.get("response")
            query = self._async_queries.get(reference_id)
            if query is None:
                self.logger.warning(f"Unknown async query response {reference_id=} resolved")
                return
            # Remove entry / mark as resolved
            self._async_queries.pop(reference_id)
            promise = query.response
            if not isinstance(promise, WattsonResponsePromise):
                self.logger.warning("Resolved async query response does not have WattsonResponsePromise associated")
                return
            query.add_response(response)
            promise.trigger_resolve()

    def get_wattson_time(self, enable_synchronization: bool = False) -> 'WattsonTime':
        from wattson.time.wattson_time import WattsonTime
        query = WattsonQuery(WattsonQueryType.GET_TIME)
        response = self.query(query)
        if response.is_successful():
            wattson_time = response.data.get("wattson_time")
            if isinstance(wattson_time, WattsonTime):
                if enable_synchronization:
                    wattson_time.enable_synchronization(wattson_client=self)
                return wattson_time
        raise WattsonClientException("Could not receive wattson time from WattsonServer")

    def set_wattson_time(self, wattson_time: 'WattsonTime') -> bool:
        query = WattsonQuery(WattsonQueryType.SET_TIME, query_data={"wattson_time": wattson_time.copy(safe=True)})
        response = self.query(query)
        return response.is_successful()

    """
    SUBSCRIPTIONS
    """
    def on_receive_notification(self, notification: WattsonNotification):
        """
        Handles the reception of notifications by the server.
        """
        if not ("*" in notification.recipients or self._client_id in notification.recipients):
            # Notification not handled by this client
            return
        topic = notification.notification_topic
        for callback in self._subscriptions.get(topic, []):
            callback(notification)
        for callback in self._subscriptions.get("*", []):
            callback(notification)

    def subscribe(self, topic: str, callback: Callable[[WattsonNotification], None]):
        """
        Subscribes this client to a notification topic.
        For each notification matching this topic, the given callback is called.
        The topic "*" catches all topics.
        """
        self._subscriptions.setdefault(topic, []).append(callback)

    def unsubscribe_topic(self, topic: str):
        """
        Removes all subscription callbacks for the given notification topic.
        """
        self._subscriptions[topic] = []

    def unsubscribe_all(self):
        """
        Removes all subscriptions for all topics for this client.
        """
        self._subscriptions = {}

    def notify(self, notification: WattsonNotification) -> bool:
        response = self.query(query=WattsonQuery(query_type=WattsonQueryType.SEND_NOTIFICATION, query_data={"notification": notification}))
        return response.is_successful()

    """
    EVENTS
    """
    def _get_event(self, event_name: str, block: bool = False):
        with self._event_lock:
            if event_name not in self._events:
                event = threading.Event()
                self._events[event_name] = event
                query = WattsonQuery(query_type=WattsonQueryType.GET_EVENT_STATE, query_data={"event_name": event_name})
                if block:
                    response = self.query(query)
                    self._handle_event_state_response(response)
                else:
                    promise = self.async_query(query)
                    promise.on_resolve(self._handle_event_state_response)
                return event
            return self._events[event_name]

    def _handle_event_state_response(self, response: WattsonResponse):
        """
        Handles the response returned by the WattsonServer for a GET_EVENT_STATE query
        @param response: The response
        @return:
        """
        if response.is_successful():
            event_name = response.data.get("event_name")
            event_occurred = response.data.get("event_occurred")
            event = self._get_event(event_name)
            if event_occurred:
                event.set()
            else:
                event.clear()

    def _handle_event_notification(self, notification: WattsonNotification):
        if notification.notification_topic == WattsonNotificationTopic.EVENTS:
            action = notification.notification_data.get("action")
            event_name = notification.notification_data.get("event_name")
            if event_name is None:
                return None
            if action == "set":
                self._get_event(notification.notification_data["event_name"]).set()
            elif action == "clear":
                self._get_event(notification.notification_data["event_name"]).clear()

    def event_wait(self, event_name: str, timeout: Optional[float] = None) -> bool:
        return self._get_event(event_name=event_name).wait(timeout=timeout)

    def event_is_known(self, event_name: str) -> bool:
        return event_name in self._events

    def event_is_set(self, event_name: str) -> bool:
        return self._get_event(event_name=event_name, block=True).is_set()

    def event_set(self, event_name: str):
        event = self._get_event(event_name)
        event.set()
        query = WattsonQuery(query_type=WattsonQueryType.SET_EVENT, query_data={"event_name": event_name})
        promise = self.async_query(query)
        promise.raise_exception_on_fail()

    def event_clear(self, event_name: str):
        event = self._get_event(event_name)
        event.clear()
        query = WattsonQuery(query_type=WattsonQueryType.CLEAR_EVENT, query_data={"event_name": event_name})
        promise = self.async_query(query)
        promise.raise_exception_on_fail()

    def get_notification_history(self, topic: Optional[str]) -> List[WattsonNotification]:
        response = self.query(WattsonQuery(query_type=WattsonQueryType.GET_NOTIFICATION_HISTORY, query_data={"topic": topic}))
        if not response.is_successful():
            self.logger.error("Could not get notification history")
            return []
        return response.data.get("notifications", [])

    """
    CONVENIENCE METHODS
    """
    def request_shutdown(self) -> bool:
        resp = self.query(WattsonQuery(query_type=WattsonQueryType.REQUEST_SHUTDOWN))
        return resp.is_successful()

    def get_remote_network_emulator(self) -> 'RemoteNetworkEmulator':
        from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator
        return RemoteNetworkEmulator.get_instance(self)

    def node_action(self, entity_id: str, action: str, block: bool = True) -> Union[bool, WattsonResponsePromise]:
        """
        Handles commands related to nodes such as starting or stopping their associated services.
        @param entity_id: The entity ID of the node
        @param action: The action to perform.
        @param block: Whether to block while waiting for a response.
        @return: A boolean indicating the queries success if block is True, else a WattsonResponsePromise to resolve later on
        """
        if action not in ["start", "stop", "restart"]:
            raise WattsonClientException(f"Unsupported action {action=} requested")

        from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
        from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType

        query = WattsonNetworkQuery(query_type=WattsonNetworkQueryType.NODE_ACTION,
                                    query_data={
                                        "action": action,
                                        "entity_id": entity_id
                                    })
        if block:
            return self.query(query, block=True).is_successful()
        return self.async_query(query)

    def get_remote_network_node(self, entity_id: str) -> 'RemoteNetworkNode':
        return self.get_remote_network_emulator().get_node(entity_id)

    def get_remote_network_link(self, entity_id: str) -> 'RemoteNetworkLink':
        return self.get_remote_network_emulator().get_link(entity_id)

    def get_remote_network_interface(self, node: Union[str, 'RemoteNetworkNode'], interface_id: str) -> 'RemoteNetworkInterface':
        return self.get_remote_network_emulator().get_interface(node, interface_id)

    def get_remote_network_interface_by_id(self, entity_id: str) -> 'RemoteNetworkInterface':
        return self.get_remote_network_emulator().get_interface_by_id(entity_id)

    def get_remote_service(self, service_id: int) -> WattsonRemoteService:
        """
        Returns a remote representation of a WattsonService existing in the simulation.
        @param service_id: The ID of the service to return.
        @return: The WattsonRemoteService instance.
        """
        return self._remote_services.setdefault(service_id, WattsonRemoteService(wattson_client=self, service_id=service_id))

    def get_remote_network_links(self) -> List['RemoteNetworkLink']:
        return self.get_remote_network_emulator().get_links()

    def get_remote_network_nodes(self) -> List['RemoteNetworkNode']:
        return self.get_remote_network_emulator().get_nodes()
