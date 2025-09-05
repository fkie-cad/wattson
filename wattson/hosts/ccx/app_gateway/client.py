import queue
import threading
import uuid
from typing import Optional, Dict, Callable, Any, List, Type, Union

import zmq
from wattson.cosimulation.control.constants import SIM_CONTROL_ID

from wattson.hosts.ccx.app_gateway.constants import DEFAULT_GATEWAY_NOTIFICATION_SOCKET_STRING, DEFAULT_GATEWAY_QUERY_SOCKET_STRING, APP_GATEWAY_NOTIFICATION_PORT, APP_GATEWAY_QUERY_PORT
from wattson.hosts.ccx.app_gateway.handlers.notification_handler import NotificationHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_async_response import AppGatewayAsyncResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response_promise import AppGatewayResponsePromise
from wattson.hosts.ccx.app_gateway.notification_client import AppGatewayNotificationClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.networking.namespaces.namespace import Namespace
from wattson.util import get_logger


class AppGatewayClient(threading.Thread):
    def __init__(self, notification_socket_string: Optional[str] = None, query_socket_string: Optional[str] = None, ip_address: Optional[str] = None,
                 client_name: Optional[str] = None, namespace: Optional[Union[Namespace, str]] = None):
        super().__init__()
        self._notification_socket_string = notification_socket_string
        self._query_socket_string = query_socket_string

        if client_name is None:
            client_name = f"generic-client"

        if self._notification_socket_string is None:
            if ip_address is None:
                self._notification_socket_string = DEFAULT_GATEWAY_NOTIFICATION_SOCKET_STRING
            else:
                self._notification_socket_string = f"tcp://{ip_address}:{APP_GATEWAY_NOTIFICATION_PORT}"
        if self._query_socket_string is None:
            if ip_address is None:
                self._query_socket_string = DEFAULT_GATEWAY_QUERY_SOCKET_STRING
            else:
                self._query_socket_string = f"tcp://{ip_address}:{APP_GATEWAY_QUERY_PORT}"

        self._client_name = f"{client_name}-{uuid.uuid4()}"
        self._client_id: Optional[str] = None
        self._registered: bool = False

        self._notification_handlers: List[NotificationHandler] = []
        self._pre_resolved_queries = {}

        self._namespace: Optional[Namespace] = None
        if namespace is not None:
            if isinstance(namespace, str) and namespace == "auto":
                namespace = Namespace(f"w_{SIM_CONTROL_ID}")
            self._namespace = namespace

        self.logger = get_logger("AppGateway.Client")

        self._started_event: threading.Event = threading.Event()
        self._termination_requested = threading.Event()

        self._query_queue = queue.Queue()
        self._queue_timeout = 1
        self._async_queries: Dict[int, AppGatewayQuery] = {}

        self._callbacks = {}

        self._publisher: AppGatewayNotificationClient = AppGatewayNotificationClient(
            self._notification_socket_string,
            on_receive_notification=self.handle_notification,
            namespace=self._namespace
        )

    @property
    def client_name(self) -> str:
        return self._client_name

    @property
    def client_id(self) -> Optional[str]:
        return self._client_id

    def register_default_notification_handlers(self):
        from wattson.hosts.ccx.app_gateway.handlers.default_notification_handler import DefaultNotificationHandler
        self.register_notification_handler_class(DefaultNotificationHandler)

    def register_notification_handler(self, handler: NotificationHandler, priority: Optional[int] = None):
        self._notification_handlers.append(handler)
        if priority is not None:
            handler.set_priority(priority)
        self._notification_handlers.sort(key=lambda h: h.priority, reverse=True)

    def register_notification_handler_class(self, handler_class: Type[NotificationHandler], priority: Optional[int] = None):
        handler = handler_class(self)
        self.register_notification_handler(handler, priority=priority)

    def handle_notification(self, notification: AppGatewayNotification):
        if notification.notification_type == AppGatewayMessageType.RESOLVE_ASYNC_RESPONSE:
            data = notification.notification_data
            recipient = data["recipient"]
            if recipient != self.client_id:
                self.logger.debug(f"Resolving async query for {recipient} cancelled (I am {self.client_id})")
                return
            response = data["response"]
            reference_id = data["reference_id"]
            query = self._async_queries.get(reference_id)
            if query is None:
                self._pre_resolved_queries[reference_id] = notification
                self.logger.warning(f"Unknown async query response {reference_id=} resolved by client {self.client_id} (for client {recipient})")
                return
            # Remove entry / mark as resolved
            self._async_queries.pop(reference_id)
            promise = query.response
            if not isinstance(promise, AppGatewayResponsePromise):
                self.logger.warning("Resolved async query response does not have WattsonResponsePromise associated")
                return
            query.add_response(response)
            promise.trigger_resolve()
            return

        for handler in self._notification_handlers:
            try:
                if handler.handle(notification):
                    return
            except Exception as e:
                self.logger.error(e)
        self.logger.debug(f"Unhandled notification of type {notification.notification_type}")

    def _check_is_pre_resolved(self, reference_id: int):
        if reference_id in self._pre_resolved_queries:
            notification = self._pre_resolved_queries.pop(reference_id)
            self.logger.info(f"Resolving pre-resolved query {reference_id=}")
            self.handle_notification(notification)

    def on(self, event: str, callback: Callable):
        self._callbacks.setdefault(event, []).append(callback)
        return callback

    def off(self, event: str, callback: Optional[Callable] = None):
        if event not in self._callbacks:
            return
        if callback is None:
            self._callbacks[event] = []
        elif callback in self._callbacks[event]:
            self._callbacks[event].remove(callback)

    def trigger(self, event: str, *arguments) -> Any:
        if event not in self._callbacks:
            return None
        ret_value = None
        for callback in self._callbacks[event]:
            ret_value = callback(*arguments)
        return ret_value

    def start(self) -> None:
        if self._started_event.is_set():
            return

        # Automatically add handlers if None is present
        if len(self._notification_handlers) == 0:
            self.register_default_notification_handlers()

        self._termination_requested.clear()
        self._started_event.set()
        self._publisher.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        self._publisher.stop(timeout=timeout)
        if self.is_alive():
            self.join(timeout)
        self._registered = False
        self._client_id = None

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
                        query: AppGatewayQuery = request["query"]
                        query.client_id = self.client_id
                        event: threading.Event = request["event"]
                        try:
                            socket.send_pyobj(query)
                            # Poll socket for answer, but be interruptable by the termination event
                            while not socket.poll(0.5):
                                if self._termination_requested.is_set():
                                    query.respond(successful=False, data={"error": "Socket Timeout due to shutdown"})
                                    event.set()
                                    # Exit
                                    return
                            response: AppGatewayResponse = socket.recv_pyobj()
                            if isinstance(response, AppGatewayAsyncResponse):
                                # Build promise from AsyncResponse
                                promise = AppGatewayResponsePromise(query=query, resolve_event=event)
                                self._async_queries[response.reference_id] = query
                                query.add_response(promise)
                                self._check_is_pre_resolved(reference_id=response.reference_id)
                            else:
                                query.add_response(response)
                                event.set()
                        except Exception as e:
                            self.logger.error(f"{e=}")
                        finally:
                            continue

    def query(self, query: AppGatewayQuery, block: bool = True, block_timeout: Optional[float] = None) -> AppGatewayResponse:
        event = threading.Event()
        request = {
            "query": query,
            "event": event
        }
        self._query_queue.put(request)
        if block:
            if not event.wait(timeout=block_timeout):
                raise TimeoutError(f"Query did not resolve after {block_timeout} seconds")
            if query.has_response():
                return query.response
            return AppGatewayResponse(successful=False, data={"error": "Timeout"})
        else:
            return AppGatewayResponsePromise(query=query, resolve_event=event)

    def register(self) -> bool:
        if self._registered:
            self.logger.info(f"Already registered. Requesting new Client ID")
        response = self.query(AppGatewayQuery(AppGatewayMessageType.REGISTRATION, query_data={"client_name": self.client_name}))
        if not response.is_successful():
            self.logger.error("Registration failed")
            return False
        else:
            self._client_id = response.data["client_id"]
            self.logger.info(f"Registered with Client ID {self.client_id}")
            return True

    def wait_for_connection(self, server_ids: Optional[List[str]] = None, timeout: Optional[float] = None) -> bool:
        global_connection_status = None
        trigger_event = threading.Event()

        def update_connection_state(protocol, server_key, server_ip, server_port, connection_status: Optional[CCXConnectionStatus]):
            if global_connection_status is None:
                return
            if server_key is not None:
                if server_key in global_connection_status:
                    global_connection_status[server_key]["connection_status"] = connection_status
                else:
                    self.logger.warning(f"Unknown server added to connection: {server_key} ({server_ip}:{server_port})")
            for key, status_dict in global_connection_status.items():
                status = status_dict["connection_status"]
                if status not in [CCXConnectionStatus.CONNECTED, CCXConnectionStatus.ESTABLISHED]:
                    if server_ids is not None and key not in server_ids:
                        # Server ID is not of interest
                        continue
                    # Not connected, don't trigger event
                    self.logger.info(f"Server {key} not yet connected ({status})")
                    return
            # Everything of interest is connected, trigger event
            trigger_event.set()

        self.on("connection_change", update_connection_state)
        global_connection_status = self.get_connection_status()
        update_connection_state(None, None, None, None, None)
        trigger_event.wait(timeout=timeout)
        self.off("connection_change", update_connection_state)
        return trigger_event.is_set()

    """
    Default Queries
    """
    def read_data_point_query(self, data_point_identifier: str, protocol_data: dict):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.READ_DATA_POINT_COMMAND,
            query_data={
                "data_point_identifier": data_point_identifier,
                "protocol_data": protocol_data
            }
        )
        return self.query(query, block=False)

    def set_data_point_query(self, data_point_identifier: str, value: Any, protocol_data: dict):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.SET_DATA_POINT_COMMAND,
            query_data={
                "data_point_identifier": data_point_identifier,
                "value": value,
                "protocol_data": protocol_data
            }
        )
        return self.query(query, block=False)

    def request_data_points(self):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.REQUEST_DATA_POINTS
        )
        response = self.query(query, block=True)
        return response.data.get("data_points", {})

    def request_grid_value_mapping(self):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.REQUEST_GRID_VALUE_MAPPING
        )
        response = self.query(query, block=True)
        return response.data.get("grid_value_mapping", {})

    def get_connection_status(self):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.GET_NODE_STATUS,
            query_data={}
        )
        response = self.query(query, block=True)
        if response.is_successful():
            return response.data.get("connection_status", {})
        return {}

    def trigger_interrogation(self, server_key: str):
        query = AppGatewayQuery(
            query_type=AppGatewayMessageType.TRIGGER_INTERROGATION,
            query_data={
                "server_key": server_key
            }
        )
        return self.query(query, block=True)
