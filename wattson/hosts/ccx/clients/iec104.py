import logging
from typing import TYPE_CHECKING, Any, Optional, Dict

from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.common.iec104message import IEC104Message

from wattson.iec104.implementations.c104.client import IEC104Client
from wattson.iec104.common.config import SERVER_DEFAULT_PORT as IEC104_SERVER_DEFAULT_PORT
from wattson.iec104.interface.apdus import APDU

if TYPE_CHECKING:
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class Iec104CCXProtocolClient(CCXProtocolClient):
    def __init__(self, ccx: 'ControlCenterExchangeGateway'):
        super().__init__(ccx)
        self.data_points: dict = self.ccx.protocol_info[CCXProtocol.IEC104]
        self.client = IEC104Client(
            datapoints=list(self.data_points.values()),
            on_receive_raw=self._on_receive_apdu,
            on_send_apdu=self._on_send_apdu,
            on_send_raw=self._on_send_apdu,
            on_receive_apdu=self._on_receive_apdu,
            on_receive_datapoint=self._on_receive_data_point,
            on_receive_control_callback=self._on_explicit_control_exit,
            on_explicit_control_exit=self._on_explicit_control_exit,
            on_connection_change=self._on_connection_change,
        )
        self.logger = self.ccx.logger.getChild("IEC104")
        self.logger.setLevel(logging.INFO)
        # Add servers
        self.server_key_by_coa = {}
        self.coa_by_server = {}
        self.data_point_identifier_by_coa_ioa = {}
        self.known_servers = set()
        for dp_id, dp in self.data_points.items():
            server_id = dp["server_key"]
            protocol_data = dp["protocol_data"]
            coa = protocol_data["coa"]
            ioa = protocol_data["ioa"]
            self.data_point_identifier_by_coa_ioa[f"{coa}.{ioa}"] = dp["identifier"]

            if server_id in self.known_servers:
                continue
            server = self.get_server(server_id)
            if server is None:
                # self.logger.warning(f"No server with ID/Key {server_id} found - cannot create client for {dp_id} ({repr(dp)})")
                # raise KeyError(f"No server with ID/Key {server_id} found")
                continue
            server_ip_address = server.get("ip")
            server_port = server.get("port")
            self.logger.info(f"Adding server {server_id} ({server_ip_address}, {server_port})")
            self.client.add_server(server_ip_address, coa, port=server_port)
            self.known_servers.add(server_id)
            self.coa_by_server[server_id] = int(coa)
            self.server_key_by_coa[int(coa)] = server_id

    def get_default_port(self):
        return IEC104_SERVER_DEFAULT_PORT

    def get_protocol(self) -> CCXProtocol:
        return CCXProtocol.IEC104

    def get_coa(self, server_key: str) -> Optional[int]:
        return self.coa_by_server.get(server_key)

    def start(self):
        self.client.start()

    def stop(self):
        self.client.stop()

    def send_data_point_command(self, data_point_identifier: str, value: Any, protocol_options: Optional[Dict] = None):
        pass

    def _get_server_by_coa(self, coa: int) -> Optional[Dict]:
        server_key = self.server_key_by_coa.get(coa)
        if server_key is None:
            return None
        return self.get_server(server_key)

    def _on_receive_apdu(self, apdu: APDU, coa: int, raw_callback: bool = False):
        # TODO: Remove raw_callback
        server = self._get_server_by_coa(coa)
        if server is None:
            self.logger.error(f"Cannot find server for COA {coa}")
            return
        server_key = server["server_key"]
        raw_data = {
            "apdu": apdu
        }
        raw_data_info = {
            "protocol": CCXProtocol.IEC104
        }
        server_ip = server.get("ip")
        server_port = server.get("port")
        self.trigger_on_receive_packet(server_key, server_ip, server_port, raw_data, raw_data_info)

    def _on_send_apdu(self, apdu: APDU, coa: int):
        server = self._get_server_by_coa(coa)
        if server is None:
            self.logger.error(f"Cannot find server for COA {coa}")
            return
        server_key = server["server_key"]
        raw_data = {
            "apdu": apdu
        }
        raw_data_info = {
            "protocol": CCXProtocol.IEC104
        }
        server_ip = server.get("ip")
        server_port = server.get("port")
        self.trigger_on_send_packet(server_key, server_ip, server_port, raw_data, raw_data_info)

    def _on_receive_data_point(self, data_point: IEC104Point, previous_point: IEC104Point, incoming_message: IEC104Message):
        data_point_identifier = self.data_point_identifier_by_coa_ioa.get(f"{data_point.coa}.{data_point.ioa}")
        if data_point_identifier is None:
            self.logger.error(f"Cannot find data point identifier for {data_point.coa} {data_point.ioa}")
            return

        protocol_data = {
            "coa": data_point.coa,
            "ioa": data_point.ioa,
            "type_id": data_point.type,
            "cause_of_transmission": incoming_message.cot.value
        }
        self.trigger_on_receive_data_point(data_point_identifier=data_point_identifier, value=data_point.value, protocol_data=protocol_data)

    def _on_explicit_control_exit(self, coa: int, data_point: IEC104Point, successful: bool, original_cot: int):
        data_point_identifier = self.data_point_identifier_by_coa_ioa.get(f"{data_point.coa}.{data_point.ioa}")
        if data_point_identifier is None:
            self.logger.error(f"Cannot find data point identifier for {data_point.coa} {data_point.ioa}")
            return
        protocol_data = {
            "cause_of_transmission": original_cot,
            "coa": data_point.coa,
            "ioa": data_point.ioa,
            "type_id": data_point.type
        }
        self.trigger_on_data_point_command_reply(data_point_identifier=data_point_identifier, successful=successful,
                                                 value=data_point.value, protocol_data=protocol_data)

    def _on_connection_change(self, coa: int, connected: bool, ip: str, port: int):
        server_id = self.server_key_by_coa.get(coa)
        if server_id is None:
            self.logger.error(f"Cannot find server for COA {coa}")
            return
        connection_status = CCXConnectionStatus.CONNECTED if connected else CCXConnectionStatus.DISCONNECTED
        self.trigger_on_connection_change(server_id, ip, port, connection_status)
