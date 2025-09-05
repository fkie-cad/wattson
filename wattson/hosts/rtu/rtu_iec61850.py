import logging
import traceback
from functools import cmp_to_key
from typing import TYPE_CHECKING, Optional, Tuple, Any, List

import iec61850_python

from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from powerowl.layers.network.configuration.protocols.iec61850.mms_report_inclusion_options import MMSReportInclusionOptions
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName
from wattson.datapoints.interface import DataPointValue

from wattson.iec61850.common.iec61850_python_mappings import iec61850_python_mappings
from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue
from wattson.iec61850.iec61850_model import IEC61850Model

if TYPE_CHECKING:
    from wattson.hosts.rtu import RTU


class RtuIec61850:
    def __init__(self, rtu: 'RTU', **kwargs):
        self.server = None
        self.port = kwargs.get("port", 102)
        self.tick_rate_ms = kwargs.get("tick_rate_ms", 1000)
        self.rtu = rtu
        self.logger = self.rtu.logger.getChild("IEC61850")
        self.logger.setLevel(logging.DEBUG)
        self.max_open_connections = kwargs.get("max_open_connections", 1000)
        self.model: Optional[IEC61850Model] = None
        self.logger.info("Initialized RtuIec61850")

        self._data_points = []
        self._data_point_dict = {}

        self._data_point_subscriptions = []
        self._data_point_callbacks = []
        self._initial_attribute_values: List[Tuple[IEC61850DataAttribute, Any]] = []

    def setup_socket(self):
        self.logger.debug("Enter setup_socket")

        # Create the model.
        self._update_data_points()
        if len(self._data_points) == 0:
            raise AttributeError("No data points exist - this class should not have been instantiated at all")

        model_name: str = self._data_points[0]["protocol_data"]["model"]
        server_id = self._data_points[0]["protocol_data"]["server"]
        self.logger.debug("Setting up iec61850 model.")
        lib_model: iec61850_python.Model = iec61850_python.Model(model_name)

        self.model = IEC61850Model(lib_model, server_id)

        report_control_blocks: dict[str, iec61850_python.ReportControlBlock] = {}
        data_sets: dict[str, iec61850_python.DataSet] = {}
        attributes_for_write_access_handler = []
        objects_for_control_handler = set()
        initial_attribute_values = []

        for dp in self._data_points:
            data_point_identifier = dp["identifier"]
            protocol_data: dict = dp["protocol_data"]
            self.logger.info(protocol_data.get("mms_path"))
            identifier: str = protocol_data["attribute_identifier"]
            is_primary_attribute = protocol_data.get("is_primary_attribute", True)
            functional_constraint_value: str = protocol_data["functional_constraint"]
            functional_constraint = MMSFunctionalConstraints(functional_constraint_value)
            # TODO: remove IEC61850_ prefix, must be implemented in iec61850_python!
            attribute_type: str = protocol_data["type"]
            attribute_type: iec61850_python.DataAttributeType = iec61850_python_mappings.attribute_type_mapping.get("IEC61850_" + attribute_type)
            if attribute_type is None:
                raise Exception("Could not find data attribute type.")
            device_name: str = protocol_data["logical_device"]
            node_name: str = protocol_data["logical_node"]
            object_names: list[str] = protocol_data["data_objects"]
            attribute_names: list[str] = protocol_data.get("data_attributes", [])
            trigger_options = [MMSTriggerOptions(trigger_option) for trigger_option in protocol_data.get("trigger_options", [])]
            logical_node = self.model.ensure_logical_device(device_name).ensure_logical_node(logical_node_name=node_name)

            initial_value = dp.get("value")

            data_attribute = logical_node.ensure_data_objects(object_names).ensure_data_attributes(
                attribute_names + [identifier],
                data_attribute_type=attribute_type,
                functional_constraint=functional_constraint,
                trigger_options=trigger_options
            )

            mms_name = data_attribute.get_mms_path()
            data_object = data_attribute.get_parent_data_object()
            attributes_for_write_access_handler.append(data_attribute)
            data_attribute.link_data_point(data_point_identifier)

            if data_attribute.is_measurement() and is_primary_attribute:
                self.logger.info(f"Subscribing to {data_point_identifier} updates")
                self._data_point_subscriptions.append(data_point_identifier)

            if data_attribute.is_control():
                objects_for_control_handler.add(data_object)

            if initial_value is not None:
                self._initial_attribute_values.append((data_attribute, initial_value))

            if "data_sets" in protocol_data:
                for data_set in protocol_data["data_sets"]:
                    data_set_data: dict = data_set["data_set_data"]
                    data_set_name: str = data_set_data["data_set_identifier"]
                    data_set_logical_device: str = data_set_data["logical_device"]
                    data_set_logical_node: str = data_set_data["logical_node"]
                    data_set = self.model.ensure_logical_device(data_set_logical_device).ensure_logical_node(data_set_logical_node).ensure_data_set(
                        data_set_name
                    )
                    if data_set_name not in data_sets:
                        data_sets[data_set_name] = data_set
                    data_set.add_entry(data_attribute)
                    self.logger.debug(f"Added point, {identifier} with mms id {mms_name} to data set {data_set_name}")

        # Add all report control blocks.
        for rcb_id, rcb in self._get_report_control_blocks().items():
            if rcb_id in report_control_blocks:
                raise Exception(f"Trying to add rcb with id {rcb_id} more than once?")

            rcb_data = rcb["rcb_data"]
            self.logger.debug(f"Processing rcb with id {rcb_id}")

            rcb_identifier: str = rcb_data["rcb_identifier"]
            is_buffered: bool = rcb_data["is_buffered"]
            configuration_revision: int = rcb_data["configuration_revision"]
            trigger_options = [MMSTriggerOptions(trigger_option) for trigger_option in rcb_data["trigger_options"]]
            inclusion_options = [MMSReportInclusionOptions(inclusion_option) for inclusion_option in rcb_data["inclusion_options"]]
            buffering_time: int = rcb_data["buffering_time"]
            integrity_period: int = rcb_data["integrity_period"]

            device_name: str = rcb_data["logical_device"]
            node_name: str = rcb_data["logical_node"]
            logical_node = self.model.ensure_logical_device(device_name).ensure_logical_node(node_name)
            # self.logger.debug(f"Node is {logical_node}")
            data_set = None
            if "data_set" in rcb_data:
                data_set_data: dict = rcb_data["data_set"]["data_set_data"]
                data_set_name: str = data_set_data["data_set_identifier"]
                data_set_logical_device: str = data_set_data["logical_device"]
                data_set_logical_node: str = data_set_data["logical_node"]
                data_set = self.model.ensure_logical_device(data_set_logical_device).ensure_logical_node(data_set_logical_node).ensure_data_set(data_set_name)
                self.logger.debug(f"Got data set {data_set}")

            report_control_block = logical_node.ensure_report_control_block(
                report_control_block_name=rcb_identifier,
                trigger_options=trigger_options,
                inclusion_options=inclusion_options,
                configuration_revision=configuration_revision,
                is_buffered=is_buffered,
                buffering_time=buffering_time,
                integrity_period=integrity_period,
                data_set=data_set
            )

            self.logger.debug(f"Added report control block {rcb_id}: {report_control_block}, {report_control_block.name}")

        self.logger.info(f"Adding Server Socket: {self.rtu.ip}:{self.port}")

        self.server: iec61850_python.Server = iec61850_python.Server(
            self.rtu.ip,
            self.port,
            self.tick_rate_ms,
            self.max_open_connections,
            self.model.lib_object,
            None
        )

        if self.server is None:
            raise Exception("Could not instantiate iec61850 server.")

        # Set initial values
        for data_attribute, initial_value in initial_attribute_values:
            success = data_attribute.update_model_value(initial_value)
            # TODO: Why does this not work?!
            self.logger.info(f"Initializing data attribute {data_attribute.get_mms_path()} with value {initial_value} ({data_attribute.get_mms_value_type().name}) ({success=})")

        self.server.set_connection_indication_callback(self.on_connection_indication)
        self.server.set_read_access_handler(self.on_read_access)
        self.server.set_report_control_block_event_handler(self.on_report_control_block_event)

        for data_attribute in attributes_for_write_access_handler:
            self.server.set_write_access_handler(data_attribute.lib_object, self.on_write_access, True)

        for data_object in objects_for_control_handler:
            self.logger.info(f"Setting control handler for {data_object.name}")
            self.server.set_control_handler(data_object.lib_object, self.on_control)
            data_object.set_control_model(iec61850_python.ControlModel.DIRECT_NORMAL)
        self.logger.debug("Done with setup_socket.")

    def _update_data_points(self):
        data_points = []
        data_points_dict = {}

        for identifier, dp in self.rtu.data_point_dict.items():
            if dp.get("protocol") == ProtocolName.IEC61850_MMS.value:
                data_points.append(dp)
                data_points_dict[identifier] = dp

        def _compare_dp(dpa, dpb) -> int:
            dpa_o_count = len(dpa.get("protocol_data", {}).get("data_objects", []))
            dpb_o_count = len(dpb.get("protocol_data", {}).get("data_objects", []))
            dpa_a_count = len(dpa.get("protocol_data", {}).get("data_attributes", []))
            dpb_a_count = len(dpb.get("protocol_data", {}).get("data_attributes", []))
            if dpa_o_count != dpb_o_count:
                return dpb_o_count - dpa_o_count
            return dpb_a_count - dpa_a_count

        # Sort according to attribute level
        data_points.sort(key=cmp_to_key(_compare_dp), reverse=True)
        self._data_points = data_points
        self._data_point_dict = data_points_dict

    def _get_report_control_blocks(self) -> dict:
        report_control_blocks = {}

        for dp in self._data_points:
            if "report_control_blocks" in dp["protocol_data"]:
                for rcb in dp["protocol_data"]["report_control_blocks"]:
                    rcb_identifier = rcb["rcb_data"]["rcb_identifier"]
                    if rcb_identifier not in report_control_blocks:
                        report_control_blocks[rcb_identifier] = rcb

        return report_control_blocks

    def _get_data_sets(self) -> dict:
        data_sets = {}

        # Data points can have data sets.
        for dp in self._data_points:
            if "data_sets" in dp["protocol_data"]:
                for data_set in dp["protocol_data"]["data_sets"]:
                    data_set_identifier = data_set["data_set_data"]["data_set_identifier"]
                    if data_set_identifier not in data_sets:
                        data_sets[data_set_identifier] = data_set

        # Report control blocks can also have data sets.
        for identifier, rcb in self._get_report_control_blocks().items():
            if "data_set" in rcb["rcb_data"]:
                data_set = rcb["rcb_data"]["data_set"]
                data_set_identifier = data_set["data_set_data"]["data_set_identifier"]
                if data_set_identifier not in data_sets:
                    data_sets[data_set_identifier] = data_set

        return data_sets

    def start(self):
        callback_id = self.rtu.manager.add_on_change_callback(self._on_data_point_update, self._data_point_subscriptions)
        self.logger.info(f"Subscribing to updates for {self._data_point_subscriptions}")
        self._data_point_callbacks.append(callback_id)
        # Update attribute values once
        self.model.update_data_point_values(self.rtu.get_value, [])
        self.server.start()

    def stop(self):
        self.server.stop()

    def get_data_point_by_attribute(self, data_attribute: IEC61850DataAttribute) -> Optional[dict]:
        identifier = data_attribute.data_point_identifier
        if identifier is None or identifier not in self._data_point_dict:
            return None
        return self._data_point_dict[identifier]

    def set_datapoint(self, point: iec61850_python):
        pass

    """
    Callbacks
    """
    def _on_data_point_update(self, data_point_identifier: str, value: DataPointValue, state_id: str):
        data_attribute = self.model.get_data_attribute_by_data_point_identifier(data_point_identifier)
        if data_attribute is None:
            self.logger.warning(f"Got update for {data_point_identifier} with no matching data attribute")
            return
        # self.logger.info(f"Got update for {data_attribute.get_mms_path()} (from {data_point_identifier}) to {value}")
        try:
            if not data_attribute.update_model_value(value):
                self.logger.warning(f"Could not update value for {data_attribute.get_mms_path()} (from {data_point_identifier}) to {value}")
        except Exception as e:
            self.logger.error(f"Could not update value for {data_attribute.get_mms_path()} (from {data_point_identifier}) to {value}")
            self.logger.error(traceback.format_exc())

    def on_connection_indication(self, server: iec61850_python.Server, local_address: str, peer_address: str, connected: bool) -> None:
        if connected:
            self.logger.info("Incoming connection")
        else:
            self.logger.info("Lost connection")
        self.logger.info(f"local address: {local_address}, peer_address: {peer_address}")

    def on_report_control_block_event(
            self,
            server: iec61850_python.Server, report_control_block: iec61850_python.ReportControlBlock, local_address: str,
            peer_address: str, event: iec61850_python.ReportControlBlockEventType, parameter_name: Optional[str],
            service_error: Optional[iec61850_python.MmsDataAccessError]) -> None:

        self.logger.debug(f"RCB {report_control_block.get_name}: {event}")

    def on_control(
            self,
            server: iec61850_python.Server,
            action: iec61850_python.ControlAction,
            data_object: iec61850_python.DataObject,
            control_value: iec61850_python.MmsValue,
            test: bool) -> iec61850_python.ControlHandlerResult:

        self.logger.info(f"Handling ControlAction for {data_object.get_name()} ({data_object.get_mms_path()}) -> {control_value.get_type().name} ({test=})")

        py_data_object = self.model.get_child_by_path(data_object.get_mms_path())
        if py_data_object is None:
            self.logger.error(f"Could not find data object in model")
            return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED

        # Find control attribute
        control_attribute = py_data_object.get_data_attribute_by_path(["Oper", "ctlVal"])
        if control_attribute is None:
            self.logger.error(f"Could not find control attribute for {data_object.get_name()}")
            return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED

        if control_value.get_type() == iec61850_python.MmsType.MMS_STRUCTURE:
            # Unpack value
            if control_value.get_size() == 1:
                try:
                    child_value = control_value.get_element(0)
                except Exception as e:
                    self.logger.error(f"Could not extract child value ({e=})")
                    return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED
                if child_value.get_type() == iec61850_python.MmsType.MMS_INTEGER:
                    if control_attribute.has_child("i"):
                        control_attribute = control_attribute.get_child("i")
                        control_value = child_value
                    else:
                        self.logger.error(f"Unpacked an MMS_INTEGER, but ctrVal.i does not exist")
                        return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED
                elif child_value.get_type() == iec61850_python.MmsType.MMS_FLOAT:
                    if control_attribute.has_child("f"):
                        control_attribute = control_attribute.get_child("f")
                        control_value = child_value
                    else:
                        self.logger.error(f"Unpacked an MMS_FLOAT, but ctrVal.f does not exist")
                        return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED
            else:
                self.logger.error(f"MMS Structure has {control_value.get_size()} elements - expected 1")
                return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED

        data_point_identifier = control_attribute.data_point_identifier
        if data_point_identifier is None:
            self.logger.error(f"Could not find data point for control attribute {control_attribute.get_mms_path()}")
            return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED

        mms_control_value = IEC61850MMSValue(control_value, control_attribute)

        self.logger.info(f"Setting {control_attribute.get_mms_path()} to {mms_control_value.value}")

        if self.rtu.set_value(data_point_identifier, mms_control_value.value):
            return iec61850_python.ControlHandlerResult.CONTROL_RESULT_OK
        self.logger.error(f"Control failed (IED Error)")
        return iec61850_python.ControlHandlerResult.CONTROL_RESULT_FAILED

    def on_read_access(self,
                       server: iec61850_python.Server,
                       logical_device: iec61850_python.LogicalDevice,
                       logical_node: iec61850_python.LogicalNode,
                       data_object: iec61850_python.DataObject,
                       functional_constraint: iec61850_python.FunctionalConstraint,
                       local_address: str,
                       peer_address: str) -> iec61850_python.MmsDataAccessError:
        self.logger.info(f"Handling read access from {peer_address} on object {data_object.get_name()} with functional constraint {functional_constraint}")

        # TODO: Update value?
        return iec61850_python.MmsDataAccessError_e.DATA_ACCESS_ERROR_SUCCESS

    def on_write_access(self,
                        server: iec61850_python.Server,
                        data_attribute: iec61850_python.DataAttribute,
                        value: iec61850_python.MmsValue,
                        local_address: str,
                        peer_address: str) -> iec61850_python.MmsDataAccessError:
        self.logger.info(f"Handling write access")
        self.logger.info(f"Handling write access from {peer_address} to attribute {data_attribute.get_name()} -> {value.get()}")
        self.logger.info(f"  {data_attribute.get_type().name} ({data_attribute.get_value().get_type().name}) -> {value.get_type().name}")

        py_data_attribute = self.model.get_child_by_path(data_attribute.get_mms_path())
        if py_data_attribute is None:
            self.logger.error(f"Could not find data attribute in model")
            return iec61850_python.MmsDataAccessError_e.DATA_ACCESS_ERROR_OBJECT_NONE_EXISTENT

        # Updating (local) value of data attribute if writable via data point
        data_point = self.get_data_point_by_attribute(py_data_attribute)
        if data_point is None:
            self.logger.warning(f"No data point found for {py_data_attribute.get_mms_path()} - allowing update anyway")
            return iec61850_python.MmsDataAccessError_e.DATA_ACCESS_ERROR_SUCCESS

        # Update in RTU
        if not self.rtu.set_value(data_point["identifier"], value.get()):
            self.logger.warning(f"Failed to update {data_point['identifier']} to {value.get()}")

        return iec61850_python.MmsDataAccessError_e.DATA_ACCESS_ERROR_SUCCESS
