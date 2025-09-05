import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Set

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_event import WattsonEvent
from wattson.hosts.ccx.app_gateway import AppGatewayServer
from wattson.hosts.ccx.app_gateway.notification_exporter import NotificationExporter
from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.logics.logic_return_action import LogicReturnAction
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.iec104.common import MTU_READY_EVENT
from wattson.util import get_logger, dynamic_load_class
from wattson.util.misc import deep_update


class ControlCenterExchangeGateway:
    """
    A more comprehensive version of an MTU that bundles multiple protocols,
    allows apps to receive measurements, issue commands, and listen to protocol-specific traffic,
    and implements logics that influence the behavior of the gateway.

    """

    def __init__(self, **kwargs):
        self.options = {
            "ip": None,
            "data_points": {},
            "servers": {},
            "allow_apps": True,
            "connect_delay": 0,
            "iec104": {
                "do_clock_sync": True,
                "do_general_interrogation": True
            },
            "wattson_client_config": None,
            "iec61850_mms": {
                "enable_single_server": False
            },
            "logics": [],
            "export": {
                "enabled": False,
                "file": Path("ccx_notifications.jsonl")
            }
        }

        self._notification_socket_string: Optional[str] = None
        self._query_socket_string: Optional[str] = None

        self.options = deep_update(self.options, kwargs)
        self.servers = self.options.get("servers", {})
        # Maps data point ids to values and latest updates
        self.data_point_cache = {}
        self.data_points = self.options.get("data_points", {})

        self.protocols: Set[CCXProtocol] = set()

        if self.options.get("iec61850_mms", {}).get("enable_single_server", False):
            for data_point in self.data_points.values():
                if data_point.get("protocol") == CCXProtocol.IEC61850_MMS:
                    self.servers = {102: self.servers[102]}
                    break

        # Maps grid value identifiers to data points
        self.grid_value_mapping: Dict[str, List[str]] = {}
        self.protocol_info = {}
        # Clients by protocol
        self.clients: Dict[str, CCXProtocolClient] = {}
        self.logger = get_logger("CCX")

        # Caching and information
        self._connection_status = {}
        self._once_connected_clients = set()

        # Assign data points to protocol
        for dp_id, dp in self.data_points.items():
            protocol = dp["protocol"]
            if protocol == CCXProtocol.IEC104:
                dp["server_key"] = dp["protocol_data"]["coa"]
            elif protocol == CCXProtocol.IEC61850_MMS:
                dp["server_key"] = dp["protocol_data"]["server"]
            else:
                self.logger.error(f"Unknown CCXProtocol: {protocol}")
            self.protocol_info.setdefault(protocol, {})[dp_id] = dp

        self.protocols = set(self.protocol_info.keys())

        # Assign data points to grid values
        for dp_id, dp in self.data_points.items():
            for context in ["sources", "targets"]:
                for provider in dp.get("providers", {}).get(context, []):
                    if provider.get("provider_type") == "POWER_GRID":
                        d = provider["provider_data"]
                        grid_value_identifier = f"{d['grid_element']}.{d['context']}.{d['attribute']}"
                        self.grid_value_mapping.setdefault(grid_value_identifier, []).append(dp_id)

        # Initialize clients
        self.init_protocol_clients()
        # Initialize client callbacks
        for protocol, client in self.clients.items():
            client.on("connection_change", self.on_connection_change)
            client.on("receive_data_point", self.on_receive_data_point)
            client.on("data_point_command_sent", self.on_data_point_command_sent)
            client.on("data_point_command_reply", self.on_data_point_command_reply)
            client.on("receive_packet", self.on_receive_packet)
            client.on("send_packet", self.on_send_packet)
            client.on("client_event", self.on_client_event)

        # Initialize AppGateway
        self.app_gateway: Optional[AppGatewayServer] = None
        if self.options.get("allow_apps"):
            self.app_gateway = AppGatewayServer(control_center_exchange_gateway=self,
                                                notification_socket_string=self._notification_socket_string,
                                                query_socket_string=self._query_socket_string)

        self.notification_exporter: Optional[NotificationExporter] = None
        if self.options.get("export", {}).get("enabled", False):
            export_path = Path(self.options.get("export", {}).get("file", "ccx-notifications.jsonl"))
            export_path.parent.mkdir(parents=True, exist_ok=True)
            self.notification_exporter = NotificationExporter(app_gateway=self.app_gateway, export_file=export_path)

        # Initialize Logics
        self.logics = []
        for logic in self.options.get("logics", []):
            self.logics.append(dynamic_load_class(logic["class"])(self, config_file=logic[
                "config_file"] if "config_file" in logic else None))

        # Initialize Wattson Client
        self.wattson_client: Optional[WattsonClient] = None
        self.wattson_client_config = self.options.get("wattson_client_config")
        if self.wattson_client_config is not None:
            self.wattson_client = WattsonClient(
                query_server_socket_string=self.wattson_client_config["query_socket"],
                publish_server_socket_string=self.wattson_client_config["publish_socket"],
                namespace=None,
                client_name=f"{self.options.get('entity_id', 'WattsonCCX')}_CCX"
            )

    def init_protocol_clients(self):
        # Initializes the clients for all required protocols
        self.logger.info("Initialize protocol clients")
        for protocol in self.protocol_info.keys():
            if protocol == CCXProtocol.IEC104:
                from wattson.hosts.ccx.clients.iec104 import Iec104CCXProtocolClient
                self.clients[CCXProtocol.IEC104] = Iec104CCXProtocolClient(self)
            elif protocol == CCXProtocol.IEC61850_MMS:
                from wattson.hosts.ccx.clients.iec61850mms import Iec61850MMSCCXProtocolClient
                self.clients[CCXProtocol.IEC61850_MMS] = Iec61850MMSCCXProtocolClient(
                    self,
                    enable_single_server=self.options.get("iec61850_mms", {}).get("enable_single_server", False)
                )
            else:
                raise NotImplementedError(f"No CCX implementation for protocol {protocol}")

    def get_client(self, protocol: CCXProtocol) -> Optional[CCXProtocolClient]:
        return self.clients.get(protocol)

    def start(self):
        self.logger.setLevel(logging.WARNING)
        # Start Wattson client
        if self.wattson_client is not None:
            self.wattson_client.start()
            self.wattson_client.require_connection()
            self.wattson_client.register()
            self.wattson_client.event_wait(WattsonEvent.START)
        # Start AppGateway
        if self.app_gateway is not None:
            self.app_gateway.start()
        # Start Exporter
        if self.notification_exporter is not None:
            self.notification_exporter.start()
        # Start protocol clients
        for protocol, client in self.clients.items():
            self.logger.info(f"Starting client for {protocol}")
            client.start()
        # Start Logics
        for logic in self.logics:
            logic.start()

    def stop(self):
        # Stop protocol clients
        for protocol, client in self.clients.items():
            self.logger.info(f"Stopping client for {protocol}")
            client.stop()
        # Stop AppGateway
        if self.app_gateway is not None:
            self.app_gateway.stop()
        # Stop Exporter
        if self.notification_exporter is not None:
            self.notification_exporter.stop()
        # Stop WattsonClient
        if self.wattson_client is not None:
            self.wattson_client.stop()
        # Stop CCX Logics
        for logic in self.logics:
            logic.stop()

    def get_connection_status(self):
        return self._connection_status

    def _update_ready_event(self, server_key: str):
        if self._once_connected_clients is None:
            return
        self._once_connected_clients.add(server_key)
        if len(self._once_connected_clients) >= len(self.servers):
            self._once_connected_clients = None
            if self.wattson_client is not None:
                self.wattson_client.event_set(MTU_READY_EVENT)

    """
    CALLBACKS FOR CLIENTS
    """

    def on_connection_change(self, client: CCXProtocolClient, server_key: str, server_ip: str, server_port: int,
                             connection_status: CCXConnectionStatus):
        self.logger.info(
            f"[{client.get_protocol_name()}] Connection changed: {server_key} ({server_ip}:{server_port}) now {connection_status}")

        previous_status = self._connection_status.get(server_key, {}).get("connection_status",
                                                                          CCXConnectionStatus.UNINITIALIZED)
        self._connection_status[server_key] = {
            "protocol": client.get_protocol_name(),
            "connection_status": connection_status,
            "server_key": server_key,
            "server_ip": server_ip,
            "server_port": server_port
        }
        event_type = "connection_lost"
        if connection_status in [CCXConnectionStatus.CONNECTED]:
            self._update_ready_event(server_key)
            event_type = "connection_established"
        self._notify_wattson_connection_state(server_key, event_type)

        if previous_status == connection_status:
            return
        action = self.apply_logics("on_connection_change", client, server_key, server_ip, server_port,
                                   connection_status)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_connection_change(client, server_key, server_ip, server_port, connection_status)

    def on_receive_data_point(self, client: CCXProtocolClient, data_point_identifier: str, value: Any,
                              protocol_data: Optional[Dict] = None):
        action = self.apply_logics("on_receive_data_point", client, data_point_identifier, value, protocol_data)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_receive_data_point(client, data_point_identifier, value, protocol_data)

    def on_data_point_command_sent(self, client: CCXProtocolClient, data_point_identifier: str, value: Any,
                                   protocol_data: Optional[Dict] = None):
        self.logger.info(f"[{client.get_protocol_name()}] Sent data point command: {data_point_identifier} -> {value}")
        action = self.apply_logics("on_data_point_command_sent", client, data_point_identifier, value, protocol_data)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_data_point_command_sent(client, data_point_identifier, value, protocol_data)

    def on_data_point_command_reply(self, client: CCXProtocolClient, data_point_identifier: str, successful: bool,
                                    value: Any, protocol_data: Optional[Dict] = None):
        self.logger.info(
            f"[{client.get_protocol_name()}] Data point command for {data_point_identifier} (-> {value}) {' was successful' if successful else 'failed'}")
        action = self.apply_logics("on_data_point_command_reply", client, data_point_identifier, successful, value,
                                   protocol_data)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_data_point_command_reply(client, data_point_identifier, successful, value,
                                                                protocol_data)

    def on_receive_packet(self, client: CCXProtocolClient, server_key: str, server_ip: str, server_port: int,
                          raw_packet_data: Any, raw_packet_data_info: Any):
        self.logger.info(f"[{client.get_protocol_name()}] Received packet: {server_key} ({server_ip}:{server_port})")
        action = self.apply_logics("on_receive_packet", client, server_key, server_ip, server_port, raw_packet_data,
                                   raw_packet_data_info)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_receive_packet(client, server_key, server_ip, server_port, raw_packet_data,
                                                      raw_packet_data_info)

    def on_send_packet(self, client: CCXProtocolClient, server_key: str, server_ip: str, server_port: int,
                       raw_packet_data: Any, raw_packet_data_info: Any):
        self.logger.info(f"[{client.get_protocol_name()}] Sent packet: {server_key} ({server_ip}:{server_port})")
        action = self.apply_logics("on_send_packet", client, server_key, server_ip, server_port, raw_packet_data,
                                   raw_packet_data_info)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_send_packet(client, server_key, server_ip, server_port, raw_packet_data,
                                                   raw_packet_data_info)

    def on_client_event(self, client: CCXProtocolClient, event: Dict):
        self.logger.info(f"[{client.get_protocol_name()}] Event: {repr(event)}")
        action = self.apply_logics("on_client_event", client, event)
        if self.app_gateway is not None and action in [LogicReturnAction.NONE, LogicReturnAction.CONTINUE]:
            self.app_gateway.notify_on_client_event(client, event)

    """
    LOGIC HANDLERS
    """

    def apply_logics(self, event_type: str, *args) -> LogicReturnAction:
        return_action = LogicReturnAction.NONE
        for logic in self.logics:
            action = logic.apply(event_type, args)
            if action in [LogicReturnAction.STOP_NOTIFICATION, LogicReturnAction.STOP_ALL]:
                return_action = LogicReturnAction.STOP_NOTIFICATION
            if action in [LogicReturnAction.STOP_LOGICS, LogicReturnAction.STOP_ALL]:
                break
        return return_action

    """
    UTILITY FUNCTIONS
    """

    def get_data_point(self, data_point_identifier: str) -> Optional[dict]:
        return self.data_points.get(data_point_identifier)

    def get_data_point_protocol(self, data_point: dict) -> CCXProtocol:
        protocol_name = data_point["protocol"]
        try:
            return CCXProtocol(protocol_name)
        except Exception as e:
            self.logger.debug(f"Unknown protocol {protocol_name} {e=}")
            return CCXProtocol.UNKNOWN

    def _notify_wattson_connection_state(self, server_id: str, event_type: str):
        if self.wattson_client is not None:
            from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
            from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import \
                WattsonNetworkNotificationTopic
            notification = WattsonNotification(
                notification_topic=WattsonNetworkNotificationTopic.NODE_CUSTOM_EVENT,
                notification_data={
                    "event_context": "mtu",
                    "event_type": event_type,
                    "rtu_entity_id": server_id
                }
            )
            notification.recipients = ["*"]
            self.wattson_client.notify(notification=notification)
