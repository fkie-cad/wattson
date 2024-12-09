import threading
from typing import Optional, TYPE_CHECKING, Dict, Any, List, Type

import zmq

from wattson.hosts.ccx.app_gateway.constants import APP_GATEWAY_QUERY_PORT, APP_GATEWAY_NOTIFICATION_PORT
from wattson.hosts.ccx.app_gateway.handlers.query_handler import QueryHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_async_response import AppGatewayAsyncResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse
from wattson.hosts.ccx.app_gateway.notification_server import AppGatewayNotificationServer
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class AppGatewayServer(threading.Thread):
    def __init__(self, control_center_exchange_gateway: 'ControlCenterExchangeGateway', notification_socket_string: Optional[str] = None, query_socket_string: Optional[str] = None):
        super().__init__()
        self.ccx = control_center_exchange_gateway
        self._notification_socket_string = notification_socket_string
        self._query_socket_string = query_socket_string

        if self._notification_socket_string is None:
            self._notification_socket_string = f"tcp://0.0.0.0:{APP_GATEWAY_NOTIFICATION_PORT}"
        if self._query_socket_string is None:
            self._query_socket_string = f"tcp://0.0.0.0:{APP_GATEWAY_QUERY_PORT}"

        self.logger = get_logger("AppGateway.Server")

        self._query_handlers: List[QueryHandler] = []
        self.clients = {}
        self._client_id = 0

        self._started_event: threading.Event = threading.Event()
        self._ready_event: threading.Event = threading.Event()
        self._notification_server: AppGatewayNotificationServer = AppGatewayNotificationServer(self._notification_socket_string)
        self._poll_timeout = 1000
        self._termination_requested = threading.Event()

    @property
    def next_client_id(self) -> int:
        self._client_id += 1
        return self._client_id

    @property
    def command_socket_string(self) -> str:
        return self._query_socket_string

    @property
    def publish_socket_string(self) -> str:
        return self._notification_socket_string

    def register_default_query_handlers(self):
        from wattson.hosts.ccx.app_gateway.handlers.iec104_query_handler import Iec104QueryHandler
        from wattson.hosts.ccx.app_gateway.handlers.default_query_handler import DefaultQueryHandler
        self.register_query_handler_class(DefaultQueryHandler, 0)
        self.register_query_handler_class(Iec104QueryHandler, 5)

    def register_query_handler(self, query_handler: QueryHandler, priority: Optional[int] = None):
        self._query_handlers.append(query_handler)
        if priority is not None:
            query_handler.set_priority(priority)
        self._query_handlers.sort(key=lambda h: h.priority, reverse=True)

    def register_query_handler_class(self, query_handler_class: Type[QueryHandler], priority: Optional[int] = None):
        self.register_query_handler(query_handler_class(self), priority=priority)

    def start(self) -> None:
        if self._started_event.is_set():
            return
        # Automatically add handlers if None is present
        if len(self._query_handlers) == 0:
            self.register_default_query_handlers()

        self._termination_requested.clear()
        self._ready_event.clear()
        self._started_event.set()
        self._notification_server.start()
        super().start()

    def stop(self, timeout: Optional[float] = None):
        self._notification_server.stop(timeout=timeout)
        self._termination_requested.set()
        if self.is_alive():
            self.join(timeout)

    def wait_until_ready(self):
        self._ready_event.wait()
        if self._notification_server is None:
            return False
        self._notification_server.wait_until_ready()

    def run(self) -> None:
        async_reference_id = 0

        with zmq.Context() as context:
            with context.socket(zmq.REP) as socket:
                self.logger.info(f"Binding to {self.command_socket_string}")
                try:
                    socket.bind(self.command_socket_string)
                except zmq.error.ZMQError as e:
                    self.logger.error(f"Could not bind to {self.command_socket_string}")
                    raise e
                self._ready_event.set()
                while not self._termination_requested.is_set():
                    if not socket.poll(timeout=self._poll_timeout):
                        continue
                    query: AppGatewayQuery = socket.recv_pyobj()
                    self.logger.info(f"Got command: {query.query_type}")
                    response = self._handle_query(query)

                    if isinstance(response, AppGatewayAsyncResponse):
                        response.reference_id = async_reference_id
                        response.client_id = query.client_id
                        response.app_gateway = self
                        send_response = response.copy_for_sending()
                        socket.send_pyobj(send_response)
                        response.resolvable.set()
                        async_reference_id += 1
                    else:
                        socket.send_pyobj(response)

    """
    Query handling
    """
    def _handle_query(self, query: AppGatewayQuery) -> AppGatewayResponse:
        for handler in self._query_handlers:
            try:
                response = handler.handle(query)
                if response is not None:
                    return response
            except Exception as e:
                self.logger.error(f"Failed to handle query: {e=}")
        return AppGatewayResponse(False, {"error": "Unhandled query"})

    def send_notification(self, notification: AppGatewayNotification):
        return self._notification_server.send_notification(notification)

    def notify(self, message_type: AppGatewayMessageType, data: dict):
        return self._notification_server.notify(message_type, data)

    def resolve_async_response(self, async_response: AppGatewayAsyncResponse, response: AppGatewayResponse):
        """
        Sends a (delayed) response to a former WattsonQuery.
        :param async_response: The async response object to resolve.
        :param response: The (resolved) response object.
        :return:
        """
        client_id = async_response.client_id
        reference_id = async_response.reference_id
        self.notify(
            AppGatewayMessageType.RESOLVE_ASYNC_RESPONSE,
            data={
                "recipient": client_id,
                "reference_id": reference_id,
                "response": response
            }
        )

    """
    Default events
    """
    def notify_on_connection_change(self, client: 'CCXProtocolClient', server_key: str, server_ip: str, server_port: int, connection_status: CCXConnectionStatus):
        self.notify(
            AppGatewayMessageType.CONNECTION_CHANGE,
            data={
                "protocol": client.get_protocol_name(),
                "server_ip": server_ip,
                "server_port": server_port,
                "connection_status": connection_status,
                "server_key": server_key
            }
        )

    def notify_on_receive_data_point(self, client: 'CCXProtocolClient', data_point_identifier: str, value: Any, protocol_data: Optional[Dict] = None):
        self.notify(
            AppGatewayMessageType.DATA_POINT_RECEIVED,
            data={
                "protocol": client.get_protocol_name(),
                "data_point_identifier": data_point_identifier,
                "value": value,
                "protocol_data": protocol_data if protocol_data is not None else {}
            }
        )

    def notify_on_data_point_command_sent(self, client: 'CCXProtocolClient', data_point_identifier: str, value: Any, protocol_data: Optional[Dict] = None):
        self.notify(
            AppGatewayMessageType.DATA_POINT_COMMAND_SENT,
            data={
                "protocol": client.get_protocol_name(),
                "data_point_identifier": data_point_identifier,
                "value": value,
                "protocol_data": protocol_data if protocol_data is not None else {}
            }
        )

    def notify_on_data_point_command_reply(self, client: 'CCXProtocolClient', data_point_identifier: str, successful: bool, value: Any,
                                           protocol_data: Optional[Dict] = None):
        self.notify(
            AppGatewayMessageType.DATA_POINT_COMMAND_REPLY,
            data={
                "protocol": client.get_protocol_name(),
                "data_point_identifier": data_point_identifier,
                "successful": successful,
                "value": value,
                "protocol_data": protocol_data if protocol_data is not None else {}
            }
        )

    def notify_on_receive_packet(self, client: 'CCXProtocolClient', server_key: str, server_ip: str, server_port: int, raw_packet_data: Any, raw_packet_data_info: Any):
        self.notify(
            AppGatewayMessageType.RAW_PACKET_RECEIVED,
            data={
                "protocol": client.get_protocol_name(),
                "server_key": server_key,
                "server_ip": server_ip,
                "server_port": server_port,
                "raw_packet_data": raw_packet_data if raw_packet_data is not None else {},
                "raw_packet_data_info": raw_packet_data_info if raw_packet_data_info is not None else {}
            }
        )

    def notify_on_send_packet(self, client: 'CCXProtocolClient', server_key: str, server_ip: str, server_port: int, raw_packet_data: Any, raw_packet_data_info: Any):
        self.notify(
            AppGatewayMessageType.RAW_PACKET_SENT,
            data={
                "protocol": client.get_protocol_name(),
                "server_key": server_key,
                "server_ip": server_ip,
                "server_port": server_port,
                "raw_packet_data": raw_packet_data if raw_packet_data is not None else {},
                "raw_packet_data_info": raw_packet_data_info if raw_packet_data_info is not None else {}
            }
        )

    def notify_on_client_event(self, client: 'CCXProtocolClient', event: dict):
        self.notify(
            AppGatewayMessageType.CLIENT_EVENT,
            data={
                "protocol": client.get_protocol_name(),
                "event": event
            }
        )
