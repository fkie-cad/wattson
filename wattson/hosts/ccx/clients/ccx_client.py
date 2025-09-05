import abc
from typing import TYPE_CHECKING, Optional, Callable, Any, Dict

from wattson.hosts.ccx.app_gateway.data_objects.ccx_report import CCXReport
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.protocols import CCXProtocol

if TYPE_CHECKING:
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class CCXProtocolClient(abc.ABC):
    def __init__(self, ccx: 'ControlCenterExchangeGateway'):
        self.ccx = ccx
        self._callbacks = {}

    @abc.abstractmethod
    def start(self):
        ...

    @abc.abstractmethod
    def stop(self):
        ...

    @abc.abstractmethod
    def get_default_port(self):
        ...

    @abc.abstractmethod
    def get_protocol(self) -> CCXProtocol:
        ...

    def get_protocol_name(self) -> str:
        return str(self.get_protocol().value)

    def get_server(self, server_key: str):
        server = self.ccx.servers.get(server_key)
        if server is None:
            return None
        server["port"] = server.get("port", self.get_default_port())
        server["server_key"] = server_key
        return server

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

    def trigger_all(self, event: str, *arguments) -> Any:
        if event not in self._callbacks:
            return None
        ret_value = None
        for callback in self._callbacks[event]:
            ret_value = callback(*arguments)
        return ret_value

    def trigger_first(self, event: str, default_return: Any, *arguments) -> Any:
        if event in self._callbacks and len(self._callbacks[event]) > 0:
            return self._callbacks[event][0](*arguments)
        return default_return

    @abc.abstractmethod
    def send_data_point_command(self, data_point_identifier: str, value: Any, protocol_options: Optional[Dict] = None):
        ...

    """
    Default Callbacks
    """
    def trigger_on_connection_change(self, server_key: str, server_ip: str, server_port: int, connection_status: CCXConnectionStatus):
        self.trigger_all("connection_change", self, server_key, server_ip, server_port, connection_status)

    def trigger_on_receive_data_point(self, data_point_identifier: str, value: Any, protocol_data: Optional[Dict] = None):
        self.trigger_all("receive_data_point", self, data_point_identifier, value, protocol_data)

    def trigger_on_data_point_command_sent(self, data_point_identifier: str, value: Any, protocol_data: Optional[Dict] = None):
        self.trigger_all("data_point_command_sent", self, data_point_identifier, value, protocol_data)

    def trigger_on_data_point_command_reply(self, data_point_identifier: str, successful: bool, value: Any, protocol_data: Optional[Dict] = None):
        self.trigger_all("data_point_command_reply", self, data_point_identifier, successful, value, protocol_data)

    def trigger_on_receive_packet(self, server_key: str, server_ip: str, server_port: int, raw_packet_data: Any, raw_packet_data_info: Any):
        self.trigger_all("receive_packet", self, server_key, server_ip, server_port, raw_packet_data, raw_packet_data_info)

    def trigger_on_send_packet(self, server_key: str, server_ip: str, server_port: int, raw_packet_data: Any, raw_packet_data_info: Any):
        self.trigger_all("send_packet", self, server_key, server_ip, server_port, raw_packet_data, raw_packet_data_info)

    def trigger_on_client_event(self, event: Dict):
        self.trigger_all("client_event", self, event)

    def trigger_on_report(self, server_key: str, server_ip: str, server_port: int, report_identifier: str, report: CCXReport, protocol_data: Optional[Dict] = None):
        self.trigger_all("report", self, server_key, server_ip, server_port, report_identifier, report, protocol_data)
