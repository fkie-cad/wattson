import logging
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, Optional, Dict

import iec61850_python

from powerowl.layers.network.configuration.data_point_type import DataPointType
from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from powerowl.layers.network.configuration.protocols.iec61850.mms_report_inclusion_options import MMSReportInclusionOptions
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from wattson.hosts.ccx.app_gateway.data_objects.ccx_mms_report import CCXMmsReport
from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.iec61850.common.iec61850_python_mappings import iec61850_python_mappings
from wattson.iec61850.iec61850_mms_array import IEC61850MMSArray
from wattson.iec61850.iec61850_mms_report import IEC61850MMSReport
from wattson.iec61850.iec61850_model import IEC61850Model
from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute

if TYPE_CHECKING:
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class Iec61850MMSCCXProtocolClient(CCXProtocolClient):
    def __init__(self, ccx: 'ControlCenterExchangeGateway', **kwargs):
        super().__init__(ccx)

        self.logger = self.ccx.logger.getChild("IEC61850")
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Start initializing Iec61850MMSCCXProtocolClient")

        self.data_points: dict = self.ccx.protocol_info[CCXProtocol.IEC61850_MMS]

        self.model_names = {}
        self.models_by_server_id: Dict[str, IEC61850Model] = {}
        self.server_id_by_model: Dict[int, str] = {}
        self.model: Optional[IEC61850Model] = None

        self.report_control_blocks_by_server_id = {}

        self.tick_rate_ms = kwargs.get("tick_rate_ms", 1000)

        self.client: iec61850_python.Client = iec61850_python.Client(self.tick_rate_ms)

        # Add servers
        self.server_key_by_ip_port = {}
        self.server_key_by_id = {}
        self.known_servers = set()
        self.connection_by_server_id = {}
        self.data_points_by_server_id = {}
        self.data_points_by_rcb_by_server = {}
        self.connections_with_rcbs = []

        self._data_attributes_by_data_point_identifier: Dict[str, IEC61850RemoteDataAttribute] = {}
        self._data_point_identifier_by_data_attribute: Dict[int, str] = {}

        for dp_id, dp in self.data_points.items():
            server_id = dp["server_key"]
            protocol_data = dp["protocol_data"]
            if server_id != 102 and kwargs.get("enable_single_server", False):
                continue
            model_name = protocol_data["model"]

            if server_id not in self.model_names:
                self.model_names[server_id] = model_name
            self.server_key_by_id[dp["identifier"]] = server_id

            if server_id in self.known_servers:
                self.data_points_by_server_id[server_id].append(dp)
                continue

            self.data_points_by_server_id[server_id] = [dp]
            self.logger.debug(f"Getting server with id {server_id}.")
            server = self.get_server(server_id)
            self.logger.debug(f"Got server: {server}")
            self.logger.debug(f"Servers: {self.ccx.servers}")
            if server is None:
                # self.logger.warning(f"No server with ID/Key {server_id} found - cannot create client for {dp_id} ({repr(dp)})")
                # raise KeyError(f"No server with ID/Key {server_id} found")
                continue

            server_ip_address = server.get("ip")
            server_port = server.get("port", self.get_default_port())
            self.server_key_by_ip_port[(server_ip_address, server_port)] = server_id
            self.logger.info(f"Adding server {server_id} ({server_ip_address}:{server_port})")
            connection: iec61850_python.Connection = self.client.add_connection(server_ip_address, server_port, None)
            connection.set_state_changed_handler(self._on_connection_indication)
            connection.set_report_callback(self.on_report_callback)
            self.connection_by_server_id[server_id] = connection

            self.known_servers.add(server_id)
            if server_id not in self.report_control_blocks_by_server_id:
                self.report_control_blocks_by_server_id[server_id] = {}
            if server_id not in self.data_points_by_rcb_by_server:
                self.data_points_by_rcb_by_server[server_id] = {}

    def get_server_by_connection(self, connection: iec61850_python.Connection) -> Optional:
        connection_ip = connection.get_remote_hostname()
        connection_port = connection.get_remote_port()
        server_id = self.server_key_by_ip_port.get((connection_ip, connection_port))
        return server_id

    def get_data_point_identifier(self, mms_attribute: IEC61850RemoteDataAttribute) -> Optional[str]:
        if id(mms_attribute) in self._data_point_identifier_by_data_attribute:
            return self._data_point_identifier_by_data_attribute[id(mms_attribute)]

        reference = mms_attribute.get_attribute_reference()
        server_id = self.server_id_by_model.get(id(mms_attribute.get_model()))
        if server_id is None:
            return None
        for data_point in self.data_points_by_server_id[server_id]:
            data_point_identifier = data_point["identifier"]
            data_point_path = data_point.get("protocol_data", {}).get("mms_path")
            if data_point_path == reference:
                self._data_attributes_by_data_point_identifier[data_point_identifier] = mms_attribute
                self._data_point_identifier_by_data_attribute[id(mms_attribute)] = data_point_identifier
                return data_point_identifier
        return None

    def get_mms_value_type_from_data_points(self, mms_attribute: IEC61850RemoteDataAttribute) -> Optional[iec61850_python.MmsType]:
        mms_type = None
        try:
            data_point = self.data_points[self.get_data_point_identifier(mms_attribute)]
            attribute_type_value = data_point.get("protocol_data", {}).get("type")
            attribute_type: iec61850_python.DataAttributeType = iec61850_python_mappings.attribute_type_mapping.get("IEC61850_" + attribute_type_value)
            if attribute_type is None:
                return None
            mms_type = IEC61850RemoteDataAttribute.attribute_type_to_value_type(attribute_type)
        except Exception:
            pass
        return mms_type

    def get_data_attribute_by_data_point_identifier(self, data_point_identifier: str) -> Optional[IEC61850RemoteDataAttribute]:
        return self._data_attributes_by_data_point_identifier.get(data_point_identifier)

    def get_model(self, server_id):
        if server_id not in self.models_by_server_id:
            model = IEC61850Model(self.get_model_name(server_id), server_id=server_id)
            self.models_by_server_id[server_id] = model
            self.server_id_by_model[id(model)] = server_id
        return self.models_by_server_id[server_id]

    def get_model_name(self, server_id) -> str:
        return self.model_names[server_id]

    @property
    def mms_data_points(self) -> dict:
        return {dp_id: dp for dp_id, dp in self.data_points.items() if dp.get("type") == DataPointType.DATA_POINT and dp.get("protocol") == CCXProtocol.IEC61850_MMS}

    def init_after_connect(self, connection: iec61850_python.Connection):
        time.sleep(2)
        self.logger.debug("Building model.")

        connection_ip = connection.get_remote_hostname()
        connection_port = connection.get_remote_port()

        server_id = self.server_key_by_ip_port.get((connection_ip, connection_port))
        if server_id is None:
            self.logger.error(f"Cannot find server for address {connection_ip}:{connection_port}")
            return

        model = self.get_model(server_id)
        self.logger.debug(f"Server id is {server_id}")

        try:
            model.build_from_connection(connection, self.logger.getChild(f"Server{server_id}Model"))

            self.logger.debug("Installing data attribute callbacks")
            for data_attribute in model.get_data_attributes():
                if isinstance(data_attribute, IEC61850RemoteDataAttribute):
                    # Fills bidirectional maps
                    mms_type = self.get_mms_value_type_from_data_points(data_attribute)
                    data_attribute.mms_type = mms_type
                    if mms_type is None:
                        # self.logger.error(f"Cannot find MMS Value Type for {data_attribute.get_attribute_reference()}")
                        continue
                    # Add callback
                    self.logger.debug(f"Adding callback to {data_attribute.get_mms_path()}")
                    data_attribute.add_on_update_callback(self.on_attribute_value_update)
            self.logger.info("Installed data attribute callbacks")

            for data_set in model.get_data_sets():
                # Read data set
                data_set.read_from_server()

        except Exception as e:
            self.logger.error(f"Error while building model")
            self.logger.error(traceback.format_exc())

    def get_protocol(self) -> CCXProtocol:
        return CCXProtocol.IEC61850_MMS

    def start(self, start_all: bool = True):
        # TODO: remove optional parameter start_all.
        self.logger.debug("Starting client and connecting to all servers.")
        self.client.start(start_all)

    def stop(self):
        self.client.stop()

    def get_default_port(self):
        return 102

    def send_data_point_command(self, data_point_identifier: str, value: Any, protocol_options: Optional[Dict] = None):
        pass

    def _on_report(self, connection: iec61850_python.Connection, report: iec61850_python.RemoteReport):
        self.logger.info(f"Report received.")

        # TODO: how the hell do we get the names of the  data attributes that are included in a report?

        # get dataSet of report
        # for attribute in dataSet:
        #  get name of attribute
        #  trigger AppGatewayMessageType.DATA_POINT_RECEIVED, i.e. trigger_on_receive_data_point

    def get_datapoint_of_server(self, server_id: str, data_point_identifier: str):
        server_key = self.server_key_by_id[data_point_identifier]
        if server_key != server_id:
            return None
        else:
            dp = next((x for x in self.data_points_by_server_id[server_id] if
                       x["identifier"] == data_point_identifier), None)
            return dp

    def _on_connection_indication(self, connection: iec61850_python.Connection,
                                  new_state: iec61850_python.IedConnectionState):
        self.logger.info(f"Connection indication received for {connection.get_remote_hostname()}:{connection.get_remote_port()}.  New state is {new_state}")

        server_id = self.get_server_by_connection(connection)
        connection_ip = connection.get_remote_hostname()
        connection_port = connection.get_remote_port()

        if server_id is None:
            self.logger.error(f"Cannot find server for address {connection_ip}:{connection_port}")
            return

        connection_status = CCXConnectionStatus.CONNECTED if new_state == iec61850_python.IedConnectionState.IED_STATE_CONNECTED else CCXConnectionStatus.DISCONNECTED
        self.logger.debug(f"Connection status is now {connection_status}")
        self.trigger_on_connection_change(server_id, connection_ip, connection_port, connection_status)

        if connection_status != CCXConnectionStatus.CONNECTED:
            return
        threading.Thread(target=self.init_after_connect, args=(connection,)).start()

    def on_report_callback(self, connection: iec61850_python.Connection, report: iec61850_python.RemoteReport):
        self.logger.info(f"Got report from {connection.get_remote_hostname()}")
        server_id = self.get_server_by_connection(connection)
        if server_id is None:
            self.logger.error(f"Cannot find server for address {connection.get_remote_hostname()}")
            return
        model = self.get_model(server_id)
        try:
            mms_report = IEC61850MMSReport(report, model)
        except Exception as e:
            self.logger.error(f"Cannot create MMS Report")
            self.logger.error(traceback.format_exc())
            return

        ccx_mms_report = CCXMmsReport(
            report_name=mms_report.report_control_block.name,
            report_reference=mms_report.report_control_block.get_mms_reference(),
            data_points={},
            data_attributes={},
            model=model
        )

        count = len(mms_report.get_report_entries())
        for mms_value, mms_attribute in mms_report.get_report_entries():
            if mms_attribute is None:
                self.logger.warning(f"Unknown mms attribute in report")
                continue
            self.logger.debug(f"{mms_attribute.get_mms_path()}: {mms_value.value}")
            if mms_value.is_data_access_error():
                self.logger.debug(f"Skipping {mms_attribute.get_mms_path()} update: DataAccessError")
                continue
            mms_attribute.set_value(mms_value.value)
            ccx_mms_report.data_points[self.get_data_point_identifier(mms_attribute)] = mms_value.value
            ccx_mms_report.data_attributes[mms_attribute.get_attribute_reference()] = mms_value.value

        self.trigger_on_report(
            str(server_id),
            connection.get_remote_hostname(),
            connection.get_remote_port(),
            mms_report.report_control_block.name,
            ccx_mms_report,
            None
        )
        # self.logger.debug(f"on_report_callback done")

    def on_attribute_value_update(self, data_attribute: IEC61850RemoteDataAttribute, old_value: Any, new_value: Any):
        data_point_identifier = self.get_data_point_identifier(data_attribute)
        if data_point_identifier is None:
            self.logger.error(f"Cannot find data_point identifier for {data_attribute.get_mms_path()} // {data_attribute.get_attribute_reference()}")
            return
        self.logger.info(f"Trigger on_receive_data_point: {data_point_identifier} = {new_value} ({data_attribute.get_mms_path()})")
        self.trigger_on_receive_data_point(data_point_identifier, new_value, protocol_data=data_attribute.get_protocol_data())
