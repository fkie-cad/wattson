import os
import queue
import sys
import threading
import time
import traceback
from typing import Optional, TYPE_CHECKING, List, Union, Type, Any, Set, Callable, Dict

import zmq
import pyprctl

from wattson.cosimulation.control.interface.publish_server import PublishServer
from wattson.cosimulation.control.interface.time_limit import TimeLimit
from wattson.cosimulation.control.interface.wattson_query_handler import WattsonQueryHandler
from wattson.cosimulation.control.messages.failed_query_response import FailedQueryResponse
from wattson.cosimulation.control.messages.wattson_async_group_response import WattsonAsyncGroupResponse
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
from wattson.cosimulation.exceptions.timeout_exception import TimeoutException
from wattson.services.configuration import ServiceConfiguration
from wattson.services.configuration.configuration_expander import ConfigurationExpander
from wattson.util import get_logger
from wattson.networking.namespaces.namespace import Namespace
from wattson.time.wattson_time import WattsonTime
from wattson.util.performance.performance_decorator import performance_assert

if TYPE_CHECKING:
    from wattson.cosimulation.control.co_simulation_controller import CoSimulationController


class WattsonServer(threading.Thread, WattsonQueryHandler):
    """Handles queries issued by clients for interacting with the co-simulation."""
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
        self._poll_timeout_ms = 1000
        self._query_timeout_seconds = None  # 5
        self._termination_requested = threading.Event()
        self._on_client_registration_callback: Optional[Callable[[str], None]] = None
        self._wattson_time: WattsonTime = kwargs.get("wattson_time", WattsonTime())
        self._ready_event = threading.Event()

        self._idle_watchdog_thread: Optional[threading.Thread] = None
        self._idle_watchdog_alarm_interval: float = 5
        self._idle_poll_duration: float = 0.3
        self._idle_watchdog_no_alarm_event: threading.Event = threading.Event()
        self._idle_watchdog_had_alarm: bool = False

        self._query_watchdog_long_query_interval: float = 5
        self._query_watchdog_thread: Optional[threading.Thread] = None
        self._query_watchdog_had_alarm: bool = False
        self._query_watchdog_query_start: Optional[float] = None
        self._query_watchdog_active_query: Optional[WattsonQuery] = None
        self._query_watchdog_query_done_event: threading.Event = threading.Event()

        self._query_statistics = {
            "total_queries": 0,
            "topic_counts": {},
            "client_counts": {},
            "query_timestamps": [],
            "query_timespan": 10,
            "queries_in_timespan": 0
        }
        self._query_statistics_queue = queue.Queue()
        self._query_statistics_thread: Optional[threading.Thread] = None

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

    def _idle_watchdog(self):
        logger = self.logger.getChild("IdleWatchdog")
        while not self._termination_requested.is_set():
            self._termination_requested.wait(1)
            if not self._idle_watchdog_no_alarm_event.wait(self._idle_watchdog_alarm_interval):
                if not self._idle_watchdog_had_alarm:
                    logger.warning(f"Can't keep up - no idle round since {self._idle_watchdog_alarm_interval} seconds")
                self._idle_watchdog_had_alarm = True
            else:
                # Alarm cleared
                if self._idle_watchdog_had_alarm:
                    self._idle_watchdog_had_alarm = False
                    logger.info(f"Idle round detected - backlog has been worked on")
            self._idle_watchdog_no_alarm_event.clear()

    def _query_watchdog(self):
        logger = self.logger.getChild("QueryWatchdog")
        while not self._termination_requested.is_set():
            self._termination_requested.wait(1)
            query_start_time = self._query_watchdog_query_start
            if query_start_time is not None:
                if time.time() - query_start_time > self._query_watchdog_long_query_interval:
                    if not self._idle_watchdog_had_alarm:
                        logger.warning(f"Long running query detected!")
                        if self._query_watchdog_active_query is not None:
                            query = self._query_watchdog_active_query
                            logger.warning(f"{query.query_type}")
                            logger.warning(f"{str(repr(query.query_data))[:200]}")
                        self._query_watchdog_had_alarm = True
                    while not self._query_watchdog_query_done_event.wait(1):
                        logger.warning(f"Long running query still blocking!")
                    self._query_watchdog_query_done_event.clear()
                    logger.info("Long running query resolved - no longer blocking")

    def _statistic_watchdog(self):
        logger = self.logger.getChild("QueryStatistics")
        last_info = 0
        while not self._termination_requested.is_set():
            try:
                query: WattsonQuery = self._query_statistics_queue.get(block=True, timeout=1)
                query_type = query.query_type
                client_id = query.client_id
                if client_id is not None:
                    self._query_statistics["client_counts"][client_id] = self._query_statistics["client_counts"].get(client_id, 0) + 1
                self._query_statistics["topic_counts"][query_type] = self._query_statistics["topic_counts"].get(query_type, 0) + 1
                self._query_statistics["query_timestamps"].append(time.time())
                self._query_statistics["total_queries"] += 1
            except queue.Empty:
                pass
            # Update sliding query count
            ## Remove old timestamps
            timeout = time.time() - self._query_statistics["query_timespan"]
            self._query_statistics["query_timestamps"] = [entry for entry in self._query_statistics["query_timestamps"] if entry > timeout]
            self._query_statistics["queries_in_timespan"] = len(self._query_statistics["query_timestamps"])

    def _log_query_statistic(self, query: WattsonQuery):
        self._query_statistics_queue.put(query)

    def start(self) -> None:
        self._termination_requested.clear()
        self._publisher = PublishServer(simulation_control_server=self, socket_string=self._publish_socket_str,
                                        namespace=self._namespace, **self._config)
        self._publisher.start()
        self._idle_watchdog_thread = threading.Thread(target=self._idle_watchdog, daemon=True)
        self._idle_watchdog_thread.start()
        self._query_watchdog_thread = threading.Thread(target=self._query_watchdog, daemon=True)
        self._query_watchdog_thread.start()
        self._query_statistics_thread = threading.Thread(target=self._statistic_watchdog, daemon=True)
        self._query_statistics_thread.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._termination_requested.set()
        self._publisher.stop(timeout=timeout)
        self._idle_watchdog_no_alarm_event.set()
        self._query_watchdog_query_done_event.set()
        if self._idle_watchdog_thread is not None:
            if self._idle_watchdog_thread.is_alive():
                self._idle_watchdog_thread.join(timeout=timeout)
        if self._query_watchdog_thread is not None:
            if self._query_watchdog_thread.is_alive():
                self._query_watchdog_thread.join(timeout=timeout)
        if self._query_statistics_thread is not None:
            if self._query_statistics_thread.is_alive():
                self._query_statistics_thread.join(timeout=timeout)
        try:
            self.join(timeout=timeout)
        except RuntimeError:
            pass

    def run(self) -> None:
        pyprctl.set_name("W/Srv")
        main_namespace = None
        if self._namespace is not None:
            main_namespace = Namespace("w_main")
            if not main_namespace.exists():
                main_namespace.from_pid(os.getpid())
            self._namespace.thread_attach()
        self.logger.info(f"Binding to {self._query_socket_str} for queries")
        async_reference_id = 0

        count = 0
        send_time_sum = 0

        with zmq.Context() as context:
            with context.socket(zmq.REP) as socket:
                socket.bind(self._query_socket_str)
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    poll_start = time.time()
                    if not socket.poll(timeout=self._poll_timeout_ms):
                        # We have an idle round
                        self._idle_watchdog_no_alarm_event.set()
                        continue
                    poll_end = time.time()
                    poll_time = poll_end - poll_start
                    if poll_time > self._idle_poll_duration:
                        # At least we were idle for a certain amount of time
                        self._idle_watchdog_no_alarm_event.set()

                    self._query_watchdog_query_start = time.time()

                    query: WattsonQuery = socket.recv_pyobj()

                    self._log_query_statistic(query)
                    self._query_watchdog_active_query = query

                    if query.requires_native_namespace() and main_namespace is not None:
                        response = main_namespace.call(self._handle_query_wrapper, arguments=(query, self._query_timeout_seconds))
                    else:
                        response = self._handle_query_wrapper(query, self._query_timeout_seconds)

                    # Async queries get a unique ID
                    callback = response.get_post_send_callback()
                    response.clear_post_send_callback()
                    try:
                        if isinstance(response, WattsonAsyncResponse):
                            # Single or Group Asynchronous Response
                            response.register_reference(query.client_id, async_reference_id)
                            if isinstance(response, WattsonAsyncGroupResponse):
                                self.logger.debug(f"Sending GroupResponse {response.group_key} for {len(response.reference_map)} clients")
                                # A Group response has asynchronous functionalities - hence, it is blocked while inserting a new client and reference.
                                response.unblock()
                            response.wattson_server = self
                            send_response = response.copy_for_sending(query.client_id)
                            socket.send_pyobj(send_response)
                            response.resolvable.set()
                            async_reference_id += 1
                        else:
                            # Synchronous Response
                            start_time = time.perf_counter()
                            socket.send_pyobj(response)
                            send_time_sum += time.perf_counter() - start_time
                            count += 1
                            """
                            if count % 100 == 0:
                                self.logger.info(f"Sent {count} queries in {send_time_sum:.2f} seconds -> Avg {send_time_sum / count:.2f}")
                                send_time_sum = 0
                                count = 0
                            """
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

                    self._query_watchdog_active_query = None
                    self._query_watchdog_query_start = None
                    self._query_watchdog_query_done_event.set()

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

        Args:
            simulation_notification (WattsonNotification):
                The notification to send
        """
        return self._publisher.broadcast(simulation_notification)

    def multicast(self, simulation_notification: WattsonNotification, recipients: List[str]):
        """
        Sends a notification to the clients in the recipients list.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send
            recipients (List[str]):
                The list of recipient IDs to send the notification to.
        """
        return self._publisher.multicast(simulation_notification, recipients=recipients)

    def unicast(self, simulation_notification: WattsonNotification, recipient: str):
        """
        Sends a notification to the specified client.

        Args:
            simulation_notification (WattsonNotification):
                The notification to send
            recipient (str):
                The ID of the desired recipient
        """
        return self._publisher.unicast(simulation_notification, recipient=recipient)

    def resolve_async_response(self, async_response: WattsonAsyncResponse, response: WattsonResponse):
        """
        Sends a (delayed) response to a former WattsonQuery.

        Args:
            async_response (WattsonAsyncResponse):
                The async response object to resolve.
            response (WattsonResponse):
                The (resolved) response object.
        """
        notification = WattsonNotification(
            WattsonNotificationTopic.ASYNC_QUERY_RESOLVE,
            notification_data={
                "reference_map": async_response.get_reference_map(),
                "response": response
            }
        )
        if isinstance(async_response, WattsonAsyncGroupResponse):
            # Multicast
            recipients = list(async_response.reference_map.keys())
            self.multicast(notification, recipients=recipients)
        else:
            # Unicast
            client_id = async_response.client_id
            self.unicast(notification, recipient=client_id)

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

    def _handle_query_wrapper(self, query: WattsonQuery, timeout: Optional[float] = 5) -> WattsonResponse:
        try:
            with TimeLimit(timeout) as time_limit:
                t = time.perf_counter()
                response = time_limit.run(self._handle_query, query)
                duration = time.perf_counter() - t
                if duration > 1:
                    self.logger.warning(f"Query handling of {query.__class__.__name__} ({query.query_type}) took {duration} s")
                return response
        except TimeoutException:
            self.logger.error(f"Handling query {query.__class__.__name__} ({query.query_type}) timed out after {timeout}s"
                              f" - canceled to prevent deadlock. This should not happen!")
            return FailedQueryResponse(data={"error": "Query handler timed out"})

    @performance_assert(1)
    def _handle_query(self, simulation_control_query: WattsonQuery) -> WattsonResponse:
        """
        Handles a query sent by a client.

        Args:
            simulation_control_query (WattsonQuery):
                The query to handle
        """
        if isinstance(simulation_control_query, WattsonMultiQuery):
            # Handle queries containing multiple sub queries
            response = WattsonMultiResponse()
            for query in simulation_control_query.queries:
                response.add_response(self._handle_query(query))
                if query.is_handled():
                    simulation_control_query.mark_as_handled()
            return response

        handlers: List[WattsonQueryHandler] = [self, self._co_simulation_controller] + self._simulators + [self._co_simulation_controller.get_model_manager()]
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
