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
from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
from wattson.services.wattson_remote_service import WattsonRemoteService
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
    from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
    from wattson.time.wattson_time import WattsonTime
    from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator


class WattsonClient(threading.Thread):
    """
    WattsonClient is a threading-based Python client for connecting to a Wattson Co-Simulation environment.
    It provides mechanisms for handling queries, publishing notifications, and managing communication with the Wattson server.
    
    Example:
        .. code-block:: python
    
            wattson_client = WattsonClient(client_name="test_application", namespace="auto", wait_for_namespace=True)
            wattson_client.register() (Default value = None)
            response = wattson_client.query(
                query=WattsonQuery(query_type=WattsonQueryType.ECHO)),
                block=True
            )
            print(response.is_successful())

    """

    def __init__(self,
                 query_server_socket_string: str = f"tcp://127.0.0.1:{SIM_CONTROL_PORT}",
                 publish_server_socket_string: str = f"tcp://127.0.0.1:{SIM_CONTROL_PUBLISH_PORT}",
                 client_name: str = "generic-client",
                 namespace: Optional[Union[str, Namespace]] = None,
                 wait_for_namespace: bool = False,
                 wattson_socket_ip: Optional[str] = None):
        """
        Creates a new WattsonClient instance to connect to a running Wattson Co-Simulation.

        Args:
            query_server_socket_string (str, optional):
                The socket to connect to for query handling. Defaults to localhost.
            publish_server_socket_string (str, optional):
                The socket to connect to for notification handling. Defaults to localhost.
            client_name (str, optional):
                A name of the client. (Default value = "generic-client")
            namespace (Optional[Union[str, Namespace]], optional):
                If given, the client moves itself to a Networking Namespace.
                This allows you to use the client from outside the simulation (i.e., without direct access to the management network).
                It can either be the name of the networking namespace or the corresponding Namespace object.
                **Use ``auto`` to move the client to Wattson's primary namespace - this is most likely what you want to do** (Default value =
                None)
            wait_for_namespace (bool, optional):
                If ``True``, the client waits for the namespace to exist.
                This is useful if the WattsonClient is instantiated during the startup of the Co-Simulation. (Default value = False)
            wattson_socket_ip (Optional[str], optional):
                Instead of providing individual socket strings, the IP of the server can be passed.
                **This overrides both socket_string parameters!**
                (Default value = None)
        """

        super().__init__(daemon=True)
        self.logger = get_logger(self.__class__.__name__, self.__class__.__name__)
        # self.logger.setLevel(logging.DEBUG)
        self._query_socket_string = query_server_socket_string
        self._publish_socket_string = publish_server_socket_string

        if wattson_socket_ip is not None:
            self._query_socket_string = f"tcp://{wattson_socket_ip}:{SIM_CONTROL_PORT}"
            self._publish_socket_string = f"tcp://{wattson_socket_ip}:{SIM_CONTROL_PUBLISH_PORT}"

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
        self._pre_resolved_queries: Dict[int, WattsonNotification] = {}

        self.subscribe(WattsonNotificationTopic.EVENTS, self._handle_event_notification)
        self.subscribe(WattsonNotificationTopic.ASYNC_QUERY_RESOLVE, self._handle_async_query)

    @property
    def client_id(self) -> Optional[str]:
        """
        The system-wide unique identifier of this client.
        This is given by the WattsonServer.

        """
        return self._client_id

    @property
    def name(self) -> str:
        """
        The user-defined name of this client.

        """
        return self._client_name

    def start(self, timeout: Optional[float] = None) -> None:
        with self._start_lock:
            if self._started_event.is_set():
                return
            # Optionally wait for namespace to exist
            if self._wait_for_namespace:                
                waiting_start = time.time()
                while not self._namespace.exists() and not self._termination_requested.is_set():
                    time.sleep(0.5)
                    if timeout is not None and time.time() - waiting_start > timeout:
                        raise TimeoutError("Waiting for the namespace took too long")
                if self._termination_requested.is_set():
                    return
            self._started_event.set()
            self._termination_requested.clear()
            self._publish_client.start()
            super().start()
        self.require_connection(timeout_seconds=timeout)
        self.register()

    def stop(self, timeout: Optional[float] = None):
        self._started_event.clear()
        self._termination_requested.set()
        self._publish_client.stop(timeout=timeout)
        if self.is_alive():
            self.logger.debug(f"Waiting for termination")
            self.join(timeout=timeout)
        self._registered = False
        self._client_id = None
        self.logger.debug(f"Stopped")

    def register(self, client_name: Optional[str] = None, force_new_id: bool = False) -> bool:
        """
        Registers this client with the server based on the given ID, indicating its connection and availability.
        Returns True, iff the registration is acknowledged.

        Args:
            client_name (Optional[str], optional):
                If given, the current name of this client will be changed. (Default value = None)
            force_new_id (bool, optional):
                If True, the client explicitly requests a new ID (even if already registered). (Default value = False)

        Returns:
            bool: True iff the registration was successful.
        """
        if self.is_registered:
            if not force_new_id:
                return True
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
        self.logger.info(f"Registered as {self._client_id}")
        self._publish_client.set_registration(self._client_name)
        return self.is_registered

    def require_connection(self, timeout_seconds: Optional[float] = None) -> bool:
        """
        Waits for the connection to the server to be established by repeatedly sending ECHO requests.
        When a timeout is given, the attempt to connect is stopped after this interval.
        When the connection is not established after the timeout, a TimeoutError is raised.
        Otherwise, the function returns with no return value or Exception.
        
        This function regularly checks for termination requests and stops its blocking behavior accordingly.

        Args:
            timeout_seconds (Optional[float], optional):
                (Default value = None)
        """
        termination_interval = 1
        echo_interval = 20
        if timeout_seconds is not None:
            timeout_seconds = math.ceil(timeout_seconds)
            termination_interval = min(termination_interval, timeout_seconds)
            echo_interval = min(termination_interval, timeout_seconds)
        interval_seconds = 5
        """if timeout_seconds is not None:
            seconds = math.ceil(timeout_seconds)
            interval_seconds = timeout_seconds / seconds
        """
        start_time = time.time()
        while not self._termination_requested.is_set():
            echo_query = WattsonQuery(WattsonQueryType.ECHO)
            echo_time = time.time()
            promise: WattsonResponsePromise = self.async_query(echo_query)
            while not self._termination_requested.is_set():
                if promise.resolve(termination_interval):
                    return True
                if time.time() - echo_time >= echo_interval:
                    # Query Timeout exceeded - break
                    break
                if timeout_seconds is not None:
                    elapsed_time = time.time() - start_time
                    if elapsed_time >= timeout_seconds:
                        # Outer Timeout exceeded, break loop and raise TimeoutError
                        raise TimeoutError(f"Connection not established after {timeout_seconds} seconds")
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
                        query.client_id = self.client_id
                        try:
                            socket.send_pyobj(query)
                            # Poll socket for answer, but be interruptable by the termination event
                            while not socket.poll(1):
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
                                self._check_is_pre_resolved(resp.reference_id)
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

        Args:
            query (WattsonQuery):
                
        """
        response = self.query(query, block=False)
        if isinstance(response, WattsonResponsePromise):
            return response
        raise TypeError(f"Expected a WattsonResponsePromise, got {response.__class__.__name__} instead")

    def query(self, query: WattsonQuery, block: bool = True) -> WattsonResponse:
        """
        Handles a query by placing it into a queue for processing and optionally blocking until a response is obtained.

        Args:
            query (WattsonQuery):
                An instance of WattsonQuery containing the query to be processed.
            block (bool, optional):
                A boolean indicating whether the method should wait for the query response or return immediately.
                (Default value = True)

        Returns:
            If block is True, returns a WattsonResponse containing the result of the processed query.
                If the query fails or the client shuts down during processing, returns a FailedQueryResponse.
                If block is False, returns a WattsonResponsePromise that allows tracking the query's completion status asynchronously.
        """
        self.start()
        event = threading.Event()
        query.client_id = self._client_id
        # self.logger.info(f"{self.client_id}: {query.query_type}")
        request = {
            "event": event,
            "query": query
        }
        self._query_queue.put(request)
        if block:
            # Allow interrupting when client stops
            while not event.wait(1):
                if self._termination_requested.is_set():
                    self.logger.warning(f"Client shutdown during query handling")
                    return FailedQueryResponse()
        else:
            return WattsonResponsePromise(query, event)

        if not query.has_response():
            return FailedQueryResponse()
        return query.response

    def primitive_query(self, query_type: str, query_data: Any, block: bool = True) -> WattsonResponse:
        """
        Executes a primitive query by creating a WattsonQuery object, setting its type and data, and passing it to the query handler.

        Args:
            query_type (str):
                The type of the query to be executed.
            query_data (Any):
                The data or payload associated with the query.
            block (bool, optional):
                A flag indicating whether the query should be executed in blocking mode. Defaults to True.

        Returns:
            WattsonResponse: The response generated after executing the query.
        """
        query = WattsonQuery()
        query.query_type = query_type
        query.query_data = query_data
        return self.query(query, block=block)

    def _handle_async_query(self, notification: WattsonNotification):
        if notification.notification_topic == WattsonNotificationTopic.ASYNC_QUERY_RESOLVE:
            reference_map = notification.notification_data.get("reference_map")
            if self.client_id not in reference_map:
                self.logger.warning("Got async query response with unknown client id")
                return
            reference_id = reference_map[self.client_id]
            response = notification.notification_data.get("response")
            query = self._async_queries.get(reference_id)
            if query is None:
                self._pre_resolved_queries[reference_id] = notification
                self.logger.warning(f"{self.client_id} - Unknown async query response {reference_id=} resolved - marking as pre-resolved")
                response: WattsonResponse
                # self.logger.warning(f"{response.is_successful()=}, {repr(response.data)[:200]}")
                return
            # Remove entry / mark as resolved
            self._async_queries.pop(reference_id)
            promise = query.response
            if not isinstance(promise, WattsonResponsePromise):
                self.logger.warning("Resolved async query response does not have WattsonResponsePromise associated")
                return

            query.add_response(response)
            promise.trigger_resolve()

    def _check_is_pre_resolved(self, reference_id: int):
        if reference_id in self._pre_resolved_queries:
            notification = self._pre_resolved_queries.pop(reference_id)
            self.logger.info(f"Resolving pre-resolved query {reference_id=}")
            self._handle_async_query(notification)

    def get_wattson_time(self, enable_synchronization: bool = False) -> 'WattsonTime':
        """
        Retrieves the current WattsonTime from the WattsonServer.

        Args:
            enable_synchronization (bool, optional):
                If set to True, the returned WattsonTime instance will enable synchronization with the central WattsonTime from the server.
                (Default value = False)

        Returns:
            A WattsonTime instance representing the current time from the WattsonServer.

        Raises:
            WattsonClientException:            If the time could not be retrieved successfully from the WattsonServer.

        """
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
        """
        Sets the Wattson time by creating and sending a WattsonQuery of type SET_TIME.

        Args:
            wattson_time ('WattsonTime'):
                A WattsonTime object representing the time to be set.
                It is copied safely before being included in the query.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        query = WattsonQuery(WattsonQueryType.SET_TIME, query_data={"wattson_time": wattson_time.copy(safe=True)})
        response = self.query(query)
        return response.is_successful()

    """
    SUBSCRIPTIONS
    """
    def on_receive_notification(self, notification: WattsonNotification):
        """
        Handles the receipt of a notification and invokes appropriate callbacks based on the subscription topic.

        Args:
            notification (WattsonNotification):
                An instance of WattsonNotification containing the notification's details.
                The notification includes a list of recipients and a notification topic.
                If the message contains a wildcard recipient "*" or matches the client's ID, the notification is processed.
                Otherwise, it is ignored.
        """
        if "*" not in notification.recipients and self._client_id not in notification.recipients:
            # Notification not handled by this client
            return
        topic = notification.notification_topic
        for callback in self._subscriptions.get(topic, []):
            callback(notification)
        for callback in self._subscriptions.get("*", []):
            callback(notification)

    def subscribe(self, topic: str, callback: Callable[[WattsonNotification], None]):
        """
        Subscribes a callback function to a specific topic. When a notification for the topic is received,
        the callback function will be executed.

        Args:
            topic (str):
                The name of the topic to subscribe to.
            callback (Callable[[WattsonNotification], None]):
                The function to be executed when a notification for the subscribed topic is received. Receives a WattsonNotification as its argument.
        """
        self._subscriptions.setdefault(topic, []).append(callback)

    def unsubscribe_topic(self, topic: str):
        """
        

        Args:
            topic (str):
                The topic to unsubscribe from, provided as a string. This will clear all subscriptions associated with the specified topic.
        """
        self._subscriptions[topic] = []

    def unsubscribe_all(self):
        """Removes all subscriptions for all topics for this client."""
        self._subscriptions = {}

    def notify(self, notification: WattsonNotification) -> bool:
        """
        Sends the specified notification object to the server to be forwarded to all subscribed clients.

        Args:
            notification (WattsonNotification):
                The notification object to be sent.

        Returns:
            bool: True if the notification was sent successfully, False otherwise.
        """
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

        Args:
            response (WattsonResponse):
                The response
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
        """
        Waits for an event with the specified name to be set or until the timeout is reached.

        Args:
            event_name (str):
                Name of the event to wait for.
            timeout (Optional[float], optional):
                Maximum time in seconds to wait for the event. If None, waits indefinitely.
                (Default value = None)

        Returns:
            bool: True if the event is set, False if the timeout is reached before the event is set.
        """
        return self._get_event(event_name=event_name).wait(timeout=timeout)

    def event_is_known(self, event_name: str) -> bool:
        """
        Checks if the given event name is in the list of known events.

        Args:
            event_name (str):
                The name of the event to check.

        Returns:
            bool: True if the event name is known, otherwise False.
        """
        return event_name in self._events

    def event_is_set(self, event_name: str) -> bool:
        """
        Checks if the specified event is set.

        Args:
            event_name (str):
                The name of the event to check.

        Returns:
            A boolean indicating whether the event is set.
        """
        return self._get_event(event_name=event_name, block=True).is_set()

    def event_set(self, event_name: str):
        """
        Sets an event by its name and initiates an asynchronous query to confirm the event's status.

        Args:
            event_name (str):
                The name of the event to set.
        """
        event = self._get_event(event_name)
        event.set()
        query = WattsonQuery(query_type=WattsonQueryType.SET_EVENT, query_data={"event_name": event_name})
        promise = self.async_query(query)
        promise.raise_exception_on_fail()

    def event_clear(self, event_name: str):
        """
        Clears the specified event and sends an asynchronous query to clear it.

        Args:
            event_name (str):
                The name of the event to clear.
        """
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
        """
        Requests a co-simulation shutdown.
        
        This method sends a shutdown request query to the Wattson CoSimulationController and returns whether the request was successful.


        Returns:
            bool: True if the shutdown request was successful, False otherwise.
        """
        resp = self.query(WattsonQuery(query_type=WattsonQueryType.REQUEST_SHUTDOWN))
        return resp.is_successful()

    def get_remote_network_emulator(self) -> 'RemoteNetworkEmulator':
        from wattson.cosimulation.simulators.network.remote_network_emulator import RemoteNetworkEmulator
        return RemoteNetworkEmulator.get_instance(self)

    def node_action(self, entity_id: str, action: str, block: bool = True) -> Union[bool, WattsonResponsePromise]:
        """
        Handles commands related to nodes such as starting or stopping their associated services.

        Args:
            entity_id (str):
                The entity ID of the node
            action (str):
                The action to perform.
            block (bool, optional):
                Whether to block while waiting for a response. (Default value = True)

        Returns:
            Union[bool,WattsonResponsePromise]: A boolean indicating the queries success if block is True, else a WattsonResponsePromise
                to resolve later on
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

        Args:
            service_id (int):
                The ID of the service to return.

        Returns:
            WattsonRemoteService: The WattsonRemoteService instance.
        """
        return self._remote_services.setdefault(service_id, WattsonRemoteService(wattson_client=self, service_id=service_id))

    def get_remote_network_links(self) -> List['RemoteNetworkLink']:
        return self.get_remote_network_emulator().get_links()

    def get_remote_network_nodes(self) -> List['RemoteNetworkNode']:
        return self.get_remote_network_emulator().get_nodes()

    def has_simulator(self, simulator_type) -> bool:
        query = WattsonQuery(query_type=WattsonQueryType.HAS_SIMULATOR, query_data={"simulator_type": simulator_type})
        response = self.query(query)
        if not response.is_successful():
            self.logger.error("HasSimulator query not successful")
            return False
        return response.data.get("has_simulator", False)

    def get_simulators(self) -> List[str]:
        query = WattsonQuery(query_type=WattsonQueryType.GET_SIMULATORS)
        response = self.query(query)
        if not response.is_successful():
            self.logger.error("GetSimulators query not successful")
            return []
        return response.data.get("simulators", [])
