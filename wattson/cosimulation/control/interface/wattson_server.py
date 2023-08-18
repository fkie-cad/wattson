import os
import sys
import threading
import traceback
from typing import Optional, TYPE_CHECKING, List, Union, Type, Any, Set, Callable, Dict

import zmq

from wattson.cosimulation.control.interface.publish_server import PublishServer
from wattson.cosimulation.control.interface.wattson_query_handler import WattsonQueryHandler
from wattson.cosimulation.control.messages.failed_query_response import FailedQueryResponse
from wattson.cosimulation.control.messages.wattson_async_response import WattsonAsyncResponse
from wattson.cosimulation.control.messages.wattson_multi_query import WattsonMultiQuery
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_multi_response import WattsonMultiResponse
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.control.messages.unhandled_query_response import UnhandledQueryResponse
from wattson.cosimulation.exceptions import InvalidSimulationControlQueryException
from wattson.services.configuration import ServiceConfiguration
from wattson.services.configuration.configuration_expander import ConfigurationExpander
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace
from wattson.time.wattson_time import WattsonTime

if TYPE_CHECKING:
    from wattson.cosimulation.control.co_simulation_controller import CoSimulationController


class WattsonServer(threading.Thread, WattsonQueryHandler):
    """
    Handles queries issued by clients for interacting with the co-simulation.
    """
    def __init__(self,
                 co_simulation_controller: 'CoSimulationController',
                 query_socket_string: str,
                 publish_socket_string: str,
                 namespace: Optional[Namespace] = None,
                 **kwargs: Any):
        super().__init__()
        self._co_simulation_controller = co_simulation_controller
        self._simulators = self._co_simulation_controller.get_simulators()
        self._namespace = namespace
        self._query_socket_str = query_socket_string
        self._publish_socket_str = publish_socket_string
        self._publisher: Optional[PublishServer] = None
        self._poll_timeout = 1000
        self._termination_requested = threading.Event()
        self._on_client_registration_callback: Optional[Callable[[str], None]] = None
        self._wattson_time: WattsonTime = kwargs.get("wattson_time", WattsonTime())
        self._ready_event = threading.Event()

        self._config = {
            "required_clients": [],     # List of client (IDs) to be connected before starting the simulation
            "connection_timeout_seconds": 30,   # Timeout in seconds to wait for all clients to connect
        }
        self._config.update(kwargs)

        self._client_id: int = 0
        self._clients: Set[str] = set()

        self._events: Dict[str, threading.Event] = {}

        self.logger = get_logger("SimControlServer", "SimControlServer")

    @property
    def query_socket_string(self) -> str:
        return self._query_socket_str

    @property
    def publish_socket_string(self) -> str:
        return self._publish_socket_str

    @property
    def wattson_time(self) -> WattsonTime:
        return self._wattson_time

    def is_ready(self) -> bool:
        if self._publisher is None:
            return False
        return self._ready_event.is_set() and self._publisher.is_ready()

    def wait_until_ready(self):
        self._ready_event.wait()
        if self._publisher is None:
            return False
        self._publisher.wait_until_ready()

    def start(self) -> None:
        self._termination_requested.clear()
        self._publisher = PublishServer(simulation_control_server=self, socket_string=self._publish_socket_str,
                                        namespace=self._namespace)
        self._publisher.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        self._publisher.stop(timeout=timeout)
        try:
            self.join(timeout=timeout)
        except RuntimeError:
            pass

    def run(self) -> None:
        main_namespace = None
        if self._namespace is not None:
            main_namespace = Namespace("w_main")
            if not main_namespace.exists():
                main_namespace.from_pid(os.getpid())
            self._namespace.thread_attach()
        self.logger.info(f"Binding to {self._query_socket_str} for queries")
        async_reference_id = 0

        with zmq.Context() as context:
            with context.socket(zmq.REP) as socket:
                socket.bind(self._query_socket_str)
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    if not socket.poll(timeout=self._poll_timeout):
                        continue
                    query: WattsonQuery = socket.recv_pyobj()
                    if main_namespace is not None:
                        response = main_namespace.call(self._handle_query, arguments=(query,))
                    else:
                        response = self._handle_query(query)
                    # Async queries get a unique ID
                    callback = response.get_post_send_callback()
                    response.clear_post_send_callback()
                    try:
                        if isinstance(response, WattsonAsyncResponse):
                            response.reference_id = async_reference_id
                            response.client_id = query.client_id
                            response.wattson_server = self
                            send_response = response.copy_for_sending()
                            socket.send_pyobj(send_response)
                            response.resolvable.set()
                            async_reference_id += 1
                        else:
                            socket.send_pyobj(response)
                    except AttributeError as e:
                        self.logger.error(f"Failed to reply to {query.query_type=}, {repr(query.query_data)}")
                        self.logger.error(f"{e=}")
                        socket.send_pyobj(FailedQueryResponse())
                    except Exception as e:
                        self.logger.error(f"Failed to send response for {query.__class__.__name__} // {query.query_type}")
                        self.logger.error(f"{e=}")
                        self.logger.error(traceback.print_exception(*sys.exc_info()))
                        socket.send_pyobj(FailedQueryResponse())
                    if callback is not None:
                        callback()

    def set_on_client_registration_callback(self, callback: Callable[[str], None]):
        self._on_client_registration_callback = callback

    def on_client_registration(self, client_id: str):
        if self._on_client_registration_callback is not None:
            self._on_client_registration_callback(client_id)

    def has_client(self, client_id: str):
        return client_id in self._clients

    def get_clients(self):
        return self._clients.copy()

    def broadcast(self, simulation_notification: WattsonNotification):
        """
        Sends a notification to all connected clients
        :param simulation_notification: The notification to send
        :return:
        """
        return self._publisher.broadcast(simulation_notification)

    def multicast(self, simulation_notification: WattsonNotification, recipients: List[str]):
        """
        Sends a notification to the clients in the recipients list.
        :param simulation_notification: The notification to send
        :param recipients: The list of recipient IDs to send the notification to.
        :return:
        """
        return self._publisher.multicast(simulation_notification, recipients=recipients)

    def unicast(self, simulation_notification: WattsonNotification, recipient: str):
        """
        Sends a notification to the specified client.
        :param simulation_notification: The notification to send
        :param recipient: The ID of the desired recipient
        :return:
        """
        return self._publisher.unicast(simulation_notification, recipient=recipient)

    def resolve_async_response(self, async_response: WattsonAsyncResponse, response: WattsonResponse):
        """
        Sends a (delayed) response to a former WattsonQuery.
        :param async_response: The async response object to resolve.
        :param response: The (resolved) response object.
        :return:
        """
        client_id = async_response.client_id
        reference_id = async_response.reference_id
        self.unicast(
            WattsonNotification(
                WattsonNotificationTopic.ASYNC_QUERY_RESOLVE,
                notification_data={
                    "reference_id": reference_id,
                    "response": response
                }),
            recipient=client_id
        )

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        query_class = self.get_simulation_query_type(query)
        return query_class is WattsonQuery

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        if not self.handles_simulation_query_type(query):
            raise InvalidSimulationControlQueryException(f"WattsonServer does not handle {query.__class__.__name__}")
        if not isinstance(query.query_type, WattsonQueryType):
            return None

        if query.query_type == WattsonQueryType.GET_NOTIFICATION_HISTORY:
            query.mark_as_handled()
            topic = query.query_data.get("topic", None)
            notifications = self._publisher.get_history(topic=topic)
            return WattsonResponse(successful=True, data={"notifications": notifications})

        if query.query_type == WattsonQueryType.SEND_NOTIFICATION:
            query.mark_as_handled()
            notification = query.query_data.get("notification", None)
            if not isinstance(notification, WattsonNotification):
                return WattsonResponse(successful=False, data={"error": "Not a valid notification"})
            self._publisher.notify(notification)

        if query.query_type == WattsonQueryType.REGISTRATION:
            # Handle Client Registration
            # If a new client registers, notify all clients via broadcast and further
            #     trigger the local callback if set.
            client_name = query.query_data.get("client_name")
            client_id = query.query_data.get("client_id")
            query.mark_as_handled()
            if client_name is None:
                return WattsonResponse(False)
            if client_id is not None:
                if client_id not in self._clients:
                    self.logger.warning(f"Client {client_name} tried to register with unknown ID {client_id}")
                    return WattsonResponse(False)
                # Already registered
                return WattsonResponse(True, data={"client_id": client_id})
            client_id = f"{client_name}_{self._client_id}"
            self._client_id += 1
            self._clients.add(client_id)
            self.logger.debug(f"Registering client {client_name}")
            self.on_client_registration(client_id=client_id)
            self.broadcast(WattsonNotification(
                notification_topic=WattsonNotificationTopic.REGISTRATION,
                notification_data={
                    "client_name": client_name,
                    "client_id": client_id,
                    "clients": list(self._clients)
                }
            ))
            return WattsonResponse(True, data={"client_id": client_id})

        if query.query_type == WattsonQueryType.ECHO:
            # ECHO Query just receives an ACK
            query.mark_as_handled()
            return WattsonResponse(True)

        if query.query_type == WattsonQueryType.GET_TIME:
            # Return the server's WattsonTime object
            query.mark_as_handled()
            return WattsonResponse(True, data={"wattson_time": self.wattson_time.copy(safe=True)})

        if query.query_type == WattsonQueryType.SET_TIME:
            query.mark_as_handled()
            wattson_time = query.query_data.get("wattson_time")
            if not isinstance(wattson_time, WattsonTime):
                return WattsonResponse(successful=False, data={"error": "No WattsonTime object given"})
            self._wattson_time.sync_from(wattson_time)
            self.logger.info(f"Setting WattsonTime to {repr(wattson_time)}")
            self.broadcast(WattsonNotification(
                notification_topic=WattsonNotificationTopic.WATTSON_TIME,
                notification_data={"wattson_time": self.wattson_time.copy(safe=True)}
            ))
            return WattsonResponse(True)

        if query.query_type == WattsonQueryType.SET_CONFIGURATION:
            query.mark_as_handled()
            success = True
            for key, value in query.query_data.items():
                if not isinstance(key, str):
                    success = False
                    continue
                self._co_simulation_controller.configuration_store.register_configuration(key, value)
            return WattsonResponse(successful=success)

        if query.query_type == WattsonQueryType.GET_CONFIGURATION:
            query.mark_as_handled()
            success = True
            response_data = {}
            for key in query.query_data.get("keys", []):
                if not isinstance(key, str):
                    success = False
                    break
                value = self._co_simulation_controller.configuration_store.get(key)
                response_data[key] = value
            return WattsonResponse(successful=success, data=response_data)

        if query.query_type == WattsonQueryType.RESOLVE_CONFIGURATION:
            query.mark_as_handled()
            success = True
            node_id = query.query_data.get("node_id")
            if node_id is not None:
                node = self._co_simulation_controller.network_emulator.get_node(node_id)
            else:
                nodes = self._co_simulation_controller.network_emulator.get_nodes()
                if len(nodes) == 0:
                    return WattsonResponse(successful=False, data={"error": "Cannot resolve configuration without network nodes"})
                node = nodes[0]

            expander = ConfigurationExpander(self._co_simulation_controller.configuration_store)
            configuration = ServiceConfiguration()
            for path in query.query_data.get("paths", []):
                if not isinstance(path, str):
                    return WattsonResponse(successful=False, data={"error": f"Invalid path: {path}"})
                self.logger.info(f"Adding {path=} for resolving")
                configuration[path] = path
            try:
                resolved = expander.expand_node_configuration(node, configuration)
                response_data = dict()
                for key, value in resolved.items():
                    response_data[key] = value
                return WattsonResponse(successful=success, data=response_data)
            except Exception as e:
                return WattsonResponse(successful=False, data={"error": repr(e)})

        """
        EVENTS
        """
        if query.query_type == WattsonQueryType.GET_EVENT_STATE:
            event_name = query.query_data["event_name"]
            event = self._get_event(event_name)
            query.mark_as_handled()
            return WattsonResponse(successful=True, data={"event_name": event_name, "event_occurred": event.is_set()})

        if query.query_type == WattsonQueryType.SET_EVENT:
            event_name = query.query_data["event_name"]
            self.set_event(event_name)
            query.mark_as_handled()
            return WattsonResponse(True)

        if query.query_type == WattsonQueryType.CLEAR_EVENT:
            event_name = query.query_data["event_name"]
            self.clear_event(event_name)
            query.mark_as_handled()
            return WattsonResponse(True)
        """ END EVENTS """

        if query.query_type == WattsonQueryType.REQUEST_SHUTDOWN:
            query.mark_as_handled()
            response = WattsonResponse(True)
            response.add_post_send_callback(lambda: self._co_simulation_controller.stop())
            return response

        return None

    def _handle_query(self, simulation_control_query: WattsonQuery) -> WattsonResponse:
        """
        Handles a query sent by a client.
        :param simulation_control_query: The query to handle
        :return:
        """
        if isinstance(simulation_control_query, WattsonMultiQuery):
            # Handle queries containing multiple sub queries
            response = WattsonMultiResponse()
            for query in simulation_control_query.queries:
                response.add_response(self._handle_query(query))
                if query.is_handled():
                    simulation_control_query.mark_as_handled()
            return response

        handlers: List[WattsonQueryHandler] = [self, self._co_simulation_controller] + self._simulators
        response = None
        try:
            for handler in handlers:
                if not simulation_control_query.can_be_handled():
                    break
                if handler.handles_simulation_query_type(simulation_control_query):
                    response = handler.handle_simulation_control_query(simulation_control_query)
        except Exception as e:
            self.logger.error(traceback.print_exception(*sys.exc_info()))
            response = UnhandledQueryResponse(successful=False, data={"error": repr(e)})
        if response is None:
            # Create UnhandledQueryResponse
            return UnhandledQueryResponse()
        return response

    """
    EVENTS
    """
    def _get_event(self, event_name: str) -> threading.Event:
        if event_name not in self._events:
            self._events[event_name] = threading.Event()
        return self._events[event_name]

    def set_event(self, event_name: str):
        event = self._get_event(event_name)
        event.set()
        self.broadcast(WattsonNotification(
            notification_topic=WattsonNotificationTopic.EVENTS,
            notification_data={
                "action": "set",
                "event_name": event_name
            }
        ))

    def clear_event(self, event_name: str):
        event = self._get_event(event_name)
        event.clear()
        self.broadcast(WattsonNotification(
            notification_topic=WattsonNotificationTopic.EVENTS,
            notification_data={
                "action": "clear",
                "event_name": event_name
            }
        ))

    def event_is_set(self, event_name: str) -> bool:
        return self._get_event(event_name).is_set()
