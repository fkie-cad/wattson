import logging
from typing import Optional, Any, Dict

import iec61850_python
from wattson.hosts.ccx.app_gateway.data_objects.ccx_report import CCXReport

from wattson.hosts.ccx.app_gateway.handlers.query_handler import QueryHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_async_response import AppGatewayAsyncResponse
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_query import AppGatewayQuery
from wattson.hosts.ccx.app_gateway.messages.app_gateway_response import AppGatewayResponse
from wattson.hosts.ccx.clients.iec61850mms import Iec61850MMSCCXProtocolClient
from wattson.hosts.ccx.connection_status import CCXConnectionStatus
from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.iec61850.common.iec61850_helpers import *
from wattson.iec61850.common.mms_control_error import MmsControlError
from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute


class Iec61850QueryHandler(QueryHandler):
    def __init__(self, app_gateway: 'AppGatewayServer'):
        super().__init__(app_gateway)

        self.logger.setLevel(logging.DEBUG)

        self.client: Optional[Iec61850MMSCCXProtocolClient] = self.app_gateway.ccx.get_client(CCXProtocol.IEC61850_MMS)
        self.logger.debug("This is 61850 query handler.")
        self.logger.debug(f"Client is {self.client}")
        if self.client is not None and not isinstance(self.client, Iec61850MMSCCXProtocolClient):
            raise RuntimeError(f"Invalid class for {CCXProtocol.IEC61850_MMS}: {type(self.client)}")
        if self.client is not None:
            self.client.on("receive_data_point", self._on_receive_data_point)
            self.client.on("report", self._on_receive_report)
            self.client.on("connection_change", self._on_connection_change)

        self._active_data_point_commands = {}

    def _register_async_response(self, action_id: str, async_response: AppGatewayAsyncResponse):
        self._active_data_point_commands.setdefault(action_id, []).append(async_response)

    def _cancel_async_response(self, action_id: str, async_response: AppGatewayAsyncResponse):
        try:
            self._active_data_point_commands[action_id].remove(async_response)
        finally:
            return True

    def _on_receive_data_point(self, client: Iec61850MMSCCXProtocolClient, data_point_identifier: str, value: Any, protocol_data: Dict[str, Any]):
        action_id = f"read-{data_point_identifier}"
        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            self.logger.warning(f"Resolving with on_receive_data_point should not be used - {data_point_identifier}")
            return
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id} to {async_response.reference_id}")
            async_response.resolve(
                AppGatewayResponse(
                    successful=True,
                    data={
                        "data_point_identifier": data_point_identifier,
                        "value": value,
                        "protocol_data": protocol_data
                    }
                )
            )

    def _on_receive_report(self,
                           client: Iec61850MMSCCXProtocolClient,
                           server_key: Any, server_ip: str, server_port: int,
                           report_identifier: str, report: CCXReport,
                           protocol_data: Dict[str, Any]):

        # Resolves when a complete report is requested
        action_id = f"read-{report_identifier}"

        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving {action_id}: {async_response.reference_id}")

            async_response.resolve(AppGatewayResponse(
                successful=True,
                data={
                    "report_identifier": report_identifier,
                    "report": report.to_dict()
                }
            ))

    def _on_connection_change(self, client: Iec61850MMSCCXProtocolClient, server_key: str, server_ip: str, server_port: int,
                              connection_status: CCXConnectionStatus):
        self.logger.info(f"Triggered trigger_on_connection_change - {server_key} ({server_ip}:{server_port}) -> {connection_status.name}")
        connection: iec61850_python.Connection = self.client.connection_by_server_id[server_key]
        if connection_status != CCXConnectionStatus.CONNECTED:
            # TODO: Clear active commands only for the broken connection
            # self._active_data_point_commands = {}
            pass

    def get_iec_61850_data_point(self, data_point_identifier: str) -> Optional[dict]:
        data_point = self.app_gateway.ccx.get_data_point(data_point_identifier)
        if data_point is None:
            return None

        protocol = self.app_gateway.ccx.get_data_point_protocol(data_point)
        if protocol != CCXProtocol.IEC61850_MMS:
            return None

        return data_point

    def _resolve_async_read(self, success: bool, data_attribute: IEC61850RemoteDataAttribute, value, action_id):
        if action_id in self._active_data_point_commands and len(self._active_data_point_commands[action_id]) > 0:
            async_response: AppGatewayAsyncResponse = self._active_data_point_commands[action_id].pop(0)
            self.logger.debug(f"Resolving async read {action_id}: {async_response.reference_id} ({data_attribute.get_attribute_reference()} = {value})")
            async_response.resolve(AppGatewayResponse(
                successful=success,
                data={
                    "data_point_identifier": self.client.get_data_point_identifier(data_attribute),
                    "value": value,
                    "protocol_data": {
                        "data_attribute": data_attribute.get_attribute_reference(),
                        "protocol": CCXProtocol.IEC61850_MMS.value
                    }
                }
            ))
        else:
            self.logger.warning(f"Cannot resolve unknown async read: {action_id=}, {data_attribute.get_attribute_reference()}, {value}")

    def handle(self, query: AppGatewayQuery) -> Optional[AppGatewayResponse]:
        q_type = query.query_type
        q_data = query.query_data

        # Cannot handle queries without client.
        if self.client is None:
            return None

        """
        Read data point.
        """
        if q_type == AppGatewayMessageType.READ_DATA_POINT_COMMAND:
            data_point_identifier = q_data.get("data_point_identifier")

            data_attribute = self.client.get_data_attribute_by_data_point_identifier(data_point_identifier)
            if data_attribute is None:
                return None
            if not isinstance(data_attribute, IEC61850RemoteDataAttribute):
                return None

            data_point = self.get_iec_61850_data_point(data_point_identifier)
            if data_point is None:
                return None

            async_response = AppGatewayAsyncResponse()
            action_id = str(id(async_response))
            self._register_async_response(action_id, async_response)

            self.logger.info(f"Triggering read of {data_point_identifier} // {data_attribute.get_attribute_reference()}")

            if not data_attribute.async_read_value_from_server(callback=self._resolve_async_read, custom_id=action_id):
                self.logger.error(f"Error reading {data_point_identifier} // {data_attribute.get_attribute_reference()}")
                self._cancel_async_response(action_id, async_response)
                return AppGatewayResponse(successful=False)

            return async_response

        elif q_type in [AppGatewayMessageType.SET_DATA_POINT_COMMAND,
                        AppGatewayMessageType.WRITE_DATA_POINT_COMMAND,
                        AppGatewayMessageType.CONTROL_DATA_POINT_COMMAND]:
            data_point_identifier = q_data.get("data_point_identifier")
            value = q_data.get("value")

            derived_query_type = q_type

            self.logger.info(f"Requesting {q_type.name} for {data_point_identifier=} to {value}")

            data_attribute = self.client.get_data_attribute_by_data_point_identifier(data_point_identifier)
            if data_attribute is None:
                self.logger.error(f"Cannot find data_attribute for {data_point_identifier=}")
                return None
            if not isinstance(data_attribute, IEC61850RemoteDataAttribute):
                self.logger.error(f"DataAttribute for {data_point_identifier=} must be IEC61850RemoteDataAttribute")
                return None

            data = {
                "data_point_identifier": data_point_identifier,
                "value": value,
                "protocol_data": {
                    "data_attribute": data_attribute.get_attribute_reference(),
                    "protocol": CCXProtocol.IEC61850_MMS.value
                }
            }

            data_point = self.get_iec_61850_data_point(data_point_identifier)
            if data_point is None:
                return None

            if q_type == AppGatewayMessageType.SET_DATA_POINT_COMMAND:
                # Autodetect action (control or write)
                if data_attribute.can_operate():
                    derived_query_type = AppGatewayMessageType.CONTROL_DATA_POINT_COMMAND
                else:
                    derived_query_type = AppGatewayMessageType.WRITE_DATA_POINT_COMMAND

            async_response = AppGatewayAsyncResponse()
            action_id = str(id(async_response))
            self._register_async_response(action_id, async_response)

            if derived_query_type == AppGatewayMessageType.CONTROL_DATA_POINT_COMMAND:
                self.logger.info(f"Triggering control of {data_point_identifier} // {data_attribute.get_attribute_reference()} -> {value} ({action_id=})")

                data_object = data_attribute.get_controllable_object()
                if data_object is None:
                    self.logger.error(f"{data_point_identifier} // {data_attribute.get_attribute_reference()} is not controllable")
                    self._cancel_async_response(action_id, async_response)
                    return AppGatewayResponse(successful=False)
                control_object = data_object.get_control_object()

                def _callback(_success: bool, _data_object, _custom_id, _control_error: MmsControlError):
                    if not _success:
                        self.logger.error(f"Could not select and operate on {data_point_identifier}: {_control_error.name}")
                    async_response.resolve(AppGatewayResponse(
                        successful=_success,
                        data=data
                    ))

                mms_control_error = control_object.async_select_and_operate(value, _callback)
                if mms_control_error != MmsControlError.NO_ERROR:
                    self.logger.error(f"Error controlling {data_point_identifier} // {data_attribute.get_attribute_reference()} ({mms_control_error.name})")
                    self._cancel_async_response(action_id, async_response)
                    return AppGatewayResponse(successful=False)
            else:
                self.logger.info(f"Triggering write of {data_point_identifier} // {data_attribute.get_attribute_reference()} -> {value} ({action_id=})")

                def _callback(_success: bool, _data_attribute, _custom_id):
                    if not _success:
                        self.logger.error(f"Could not write to {data_point_identifier} // {data_attribute.get_attribute_reference()}")
                    async_response.resolve(AppGatewayResponse(
                        successful=_success,
                        data=data
                    ))

                if not data_attribute.async_write_value_to_server(value=value, callback=_callback, custom_id=action_id):
                    self.logger.error(f"Error writing {data_point_identifier} // {data_attribute.get_attribute_reference()}")
                    self._cancel_async_response(action_id, async_response)
                    return AppGatewayResponse(successful=False)

            return async_response
