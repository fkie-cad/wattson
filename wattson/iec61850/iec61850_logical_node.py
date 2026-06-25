import logging
import threading
from typing import List, TYPE_CHECKING, Optional, Union

import iec61850_python
from powerowl.layers.network.configuration.protocols.iec61850.mms_report_inclusion_options import MMSReportInclusionOptions
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from wattson.iec61850.common.iec61850_helpers import is_error, parse_variable
from wattson.iec61850.common.iec61850_python_mappings import iec61850_python_mappings
from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_model import IEC61850Model
    from wattson.iec61850.iec61850_data_set import IEC61850DataSet
    from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute


class IEC61850LogicalNode:
    def __init__(self,
                 lib_object: iec61850_python.LogicalDevice,
                 logical_device: Optional['IEC61850LogicalDevice'] = None,
                 data_objects: Optional[List['IEC61850DataObject']] = None) -> None:

        if data_objects is None:
            data_objects = []
        self.logical_device = logical_device
        self.lib_object = lib_object
        self.data_sets: List['IEC61850DataSet'] = []
        self.report_control_blocks: List['IEC61850ReportControlBlock'] = []
        self.data_objects = data_objects

    @property
    def name(self) -> str:
        return self.lib_object.get_name()

    @property
    def reference(self) -> str:
        # TODO: Include device?
        return f"{self.logical_device.reference}/{self.name}"

    def get_mms_reference(self) -> str:
        return self.reference

    def get_logical_device(self) -> 'IEC61850LogicalDevice':
        return self.logical_device

    def get_model(self) -> 'IEC61850Model':
        return self.get_logical_device().get_model()

    def add_data_set(self, data_set: 'IEC61850DataSet') -> bool:
        if data_set in self.data_sets:
            return False
        data_set.logical_node = self
        self.data_sets.append(data_set)
        return True

    def add_report_control_block(self, report_control_block: 'IEC61850ReportControlBlock') -> bool:
        if report_control_block in self.report_control_blocks:
            return False
        self.report_control_blocks.append(report_control_block)
        report_control_block.logical_node = self
        return True

    def add_data_object(self, data_object: 'IEC61850DataObject') -> bool:
        if self.has_data_object(data_object):
            return False
        data_object.parent = self
        self.data_objects.append(data_object)
        return True

    def has_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> bool:
        if isinstance(data_object, str):
            for existing_data_object in self.data_objects:
                if existing_data_object.name == data_object:
                    return True
            return False
        return data_object in self.data_objects

    def get_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> 'IEC61850DataObject':
        data_object_name = data_object
        if isinstance(data_object, str):
            for existing_data_object in self.data_objects:
                if existing_data_object.name == data_object:
                    return existing_data_object
        else:
            data_object_name = data_object.name

        if not self.has_data_object(data_object):
            raise KeyError(f"DataObject {data_object_name} does not exist")
        return data_object

    def get_child_by_path(self, path: List[str]) -> Optional[Union['IEC61850DataAttribute', 'IEC61850DataObject']]:
        if len(path) == 0:
            return None
        first_child = path.pop(0)
        if self.has_data_object(first_child):
            return self.get_data_object(first_child).get_child_by_path(path)
        return None

    def ensure_data_objects(self, data_object_names: List[str]) -> 'IEC61850DataObject':
        from wattson.iec61850.iec61850_data_object import IEC61850DataObject
        if len(data_object_names) == 0:
            raise ValueError(f'Cannot ensure empty set of data objects in logical node {self.name}')
        first_data_object_name = data_object_names[0]
        if self.has_data_object(first_data_object_name):
            parent = self.get_data_object(first_data_object_name)
        else:
            lib_data_object = self.lib_object.add_data_object(first_data_object_name, 0)
            parent = IEC61850DataObject(lib_data_object)
            self.add_data_object(parent)
        return parent.ensure_data_objects(data_object_names[1:])

    def get_data_sets(self) -> List['IEC61850DataSet']:
        return self.data_sets

    def get_data_set(self, data_set_name: str) -> 'IEC61850DataSet':
        for data_set in self.data_sets:
            if data_set.name == data_set_name:
                return data_set
        raise KeyError(f"DataSet {data_set_name} does not exist in logical node {self.name}")

    def has_data_set(self, data_set_name: str) -> bool:
        try:
            self.get_data_set(data_set_name)
            return True
        except KeyError:
            return False

    def ensure_data_set(self, data_set_name: str):
        from wattson.iec61850.iec61850_data_set import IEC61850DataSet
        if self.has_data_set(data_set_name):
            return self.get_data_set(data_set_name)

        lib_data_set = self.lib_object.add_data_set(data_set_name)
        data_set = IEC61850DataSet(lib_data_set)
        self.add_data_set(data_set)
        return data_set

    def get_report_control_block(self, report_control_block_name: str) -> 'IEC61850ReportControlBlock':
        for report_control_block in self.report_control_blocks:
            if report_control_block.name == report_control_block_name:
                return report_control_block
        raise KeyError(f"ReportControlBlock {report_control_block_name} does not exist in logical node {self.name}")

    def has_report_control_block(self, report_control_block_name: str) -> bool:
        try:
            self.get_report_control_block(report_control_block_name)
            return True
        except KeyError:
            return False

    def ensure_report_control_block(
            self,
            report_control_block_name: str,
            trigger_options: List[MMSTriggerOptions],
            inclusion_options: List[MMSReportInclusionOptions],
            configuration_revision: int = 1,
            is_buffered: bool = False,
            buffering_time: int = 10,
            integrity_period: int = 10,
            data_set: Optional['IEC61850DataSet'] = None,
        ) -> 'IEC61850ReportControlBlock':
        from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock

        if self.has_report_control_block(report_control_block_name):
            return self.get_report_control_block(report_control_block_name)
        data_set_name = None if data_set is None else data_set.name
        lib_trigger_options = 0
        lib_inclusion_options = 0
        for option in inclusion_options:
            option = iec61850_python_mappings.inclusion_options_mapping.get(option.value)
            if option is None:
                raise Exception(f"Could not find inclusion option {option}.")
            lib_inclusion_options |= option
        for option in trigger_options:
            option = iec61850_python_mappings.trigger_options_mapping.get(option.value)
            if option is None:
                raise Exception(f"Could not find trigger option {option}.")
            lib_trigger_options |= option

        lib_report_control_block = self.lib_object.add_report_control_block(
            report_control_block_name,
            report_control_block_name,
            is_buffered,
            data_set_name,
            configuration_revision,
            lib_trigger_options,
            lib_inclusion_options,
            buffering_time,
            integrity_period
        )
        report_control_block = IEC61850ReportControlBlock(lib_report_control_block)
        self.add_report_control_block(report_control_block)
        return report_control_block

    def get_data_attributes(self) -> List['IEC61850DataAttribute']:
        attributes = []
        for data_object in self.data_objects:
            attributes.extend(data_object.get_data_attributes())
        return attributes

    def build_from_connection(self, connection: iec61850_python.Connection, logger: logging.Logger):
        from wattson.iec61850.iec61850_remote_data_set import IEC61850RemoteDataSet
        from wattson.iec61850.iec61850_remote_report_control_block import IEC61850RemoteReportControlBlock

        # Read variables
        # TODO: This assumes a flat hierarchy, i.e., no nested objects or attributes
        variables, error = connection.get_logical_node_variables(self.reference)
        if is_error(error):
            logger.error(f"Failed while retrieving variables for {self.reference}: {error=}")
            return False
        logger.debug(f"Logical node {self.reference} has {len(variables)} variables")

        report_control_block_references = []

        attribute_interrogation_events = {}

        def _perform_interrogation(_data_attribute: 'IEC61850RemoteDataAttribute'):
            _event = attribute_interrogation_events.get(_data_attribute.get_attribute_reference())
            lib_functional_constraint: iec61850_python.FunctionalConstraint = iec61850_python_mappings.functional_constraint_mapping.get(
                "IEC61850_FC_" + _data_attribute.functional_constraint.value
            )
            if lib_functional_constraint is None:
                _event.set()
                raise KeyError(f"Could not find functional constraint {functional_constraint.value}.")
            data_attribute_path = data_attribute.get_mms_path()
            mms_variable_specification, _error = connection.get_variable_specification(data_attribute_path, lib_functional_constraint)
            if is_error(_error):
                logger.error(f"Failed while retrieving variable specification for {data_attribute_path}: {error=}")
                _event.set()
                return False
            data_attribute.mms_type = mms_variable_specification.get_type()
            _event.set()

        for variable in variables:
            parsed_variable = parse_variable(variable)
            if parsed_variable["is_report"]:
                if not parsed_variable["is_report_attribute"]:
                    rcb_id = parsed_variable["report_name"]
                    if rcb_id not in report_control_block_references:
                        report_control_block_references.append(rcb_id)
            elif parsed_variable["is_data_object"]:
                # Create data object
                self.ensure_data_objects([parsed_variable["object_name"]])
            elif parsed_variable["is_data_attribute"]:
                # Get specification
                data_object = self.ensure_data_objects([parsed_variable["object_name"]])
                parent_attributes = parsed_variable["parent_attributes"]
                data_attribute_name = parsed_variable["attribute_name"]
                functional_constraint = parsed_variable["functional_constraint"]

                if data_object.has_data_attribute_path(parent_attributes + [data_attribute_name]):
                    continue

                data_attribute = data_object.ensure_remote_data_attributes(
                    parent_attributes + [data_attribute_name],
                    # This will be overwritten after the interrogation
                    iec61850_python.MmsType.MMS_DATA_ACCESS_ERROR,
                    functional_constraint=functional_constraint,
                )
                """
                data_attribute_path = data_attribute.get_attribute_reference()
                attribute_interrogation_events[data_attribute_path] = threading.Event()

                threading.Thread(target=_perform_interrogation, args=(data_attribute,)).start()
                """
                # logger.debug(f"Got attribute {data_attribute.get_mms_path()} with {data_attribute.functional_constraint.value} and type {data_attribute.mms_type.name} / {data_attribute.mms_type.value}")

        for data_attribute, event in attribute_interrogation_events.items():
            event.wait()

        logger.debug(f"Attributes created")

        # Read Data Sets
        acsi_class = iec61850_python.ACSIClass.ACSI_CLASS_DATA_SET
        data_sets, error = connection.get_logical_node_directory(self.reference, acsi_class)
        if is_error(error):
            logger.error(f"Failed while retrieving data sets for {self.reference}: {error=}")
            return False
        logger.debug(f"Node {self.name} has {len(data_sets)} data sets")
        for data_set_name in data_sets:
            logger.debug(f"  {data_set_name}")
            if self.has_data_set(data_set_name):
                logger.warning(f"Data set {data_set_name} already exists")
                continue
            data_set_reference = f"{self.reference}${data_set_name}"
            # Request data set directory
            logger.debug(f"Querying {data_set_reference}")
            directory, is_deletable, error = connection.get_data_set_directory(data_set_reference)
            if is_error(error):
                logger.error(f"Failed while retrieving data set {data_set_reference} for {self.reference}: {error=}")
                return False

            data_set = IEC61850RemoteDataSet(data_set_name)
            self.add_data_set(data_set)

            logger.debug(f"Found data set directory for {data_set_reference}")
            for entry in directory:
                logger.debug(f"   {entry}")
                parts: list = entry.split(".")
                # Remove device/node
                parts.pop(0)
                # Remove functional constraint
                parts[-1] = parts[-1].split("[")[0]

                if len(parts) >= 2:
                    if not self.has_data_object(parts[0]):
                        logger.error(f"Cannot add {entry} to data set - {parts[0]} data object not found")
                        continue
                    data_object = self.get_data_object(parts[0])
                    if not data_object.has_data_attribute_path(parts[1:]):
                        logger.error(f"Cannot add {entry} to data set - {'.'.join(parts[1:])} data attribute not found")
                        continue
                    data_attribute = data_object.get_data_attribute_by_path(parts[1:])
                    data_set.add_entry(data_attribute, add_to_library=False)
                else:
                    logger.error(f"Cannot add {entry} to data set - only invalid number of parts")
                    continue

        logger.debug(f"Attempting to register report control blocks: {report_control_block_references}")
        for rcb_id in report_control_block_references:
            if self.has_report_control_block(rcb_id):
                logger.warning(f"RCB {rcb_id} already exists")
                continue
            report_control_block = IEC61850RemoteReportControlBlock(rcb_id)
            self.add_report_control_block(report_control_block)
            # Read RPC information
            rcb_reference = report_control_block.get_mms_reference()
            rcb, error = connection.get_report_control_block_values(rcb_reference, None)
            if is_error(error):
                logger.error(f"Could not load report control block information for {rcb_reference}: {error=}")
                return False
            # Extract Data Set Information
            data_set_reference = rcb.get_data_set_reference()
            parts = data_set_reference.split("$")
            if len(parts) == 2:
                data_set_name = parts[1]
                logger.debug(f"  RCB {rcb_reference} has data set reference {data_set_reference}")
                if not self.has_data_set(data_set_name):
                    logger.error(f"Cannot load data set - {data_set_reference} data set not found")
                    return False
                data_set = self.get_data_set(data_set_name)
                report_control_block.data_set = data_set

            # Enable RCB
            connection.add_report_to_callback(rcb_reference, rcb_id)
            rcb.set_report_enabled(True)
            report_options = iec61850_python.RPT_OPT_DATA_SET | iec61850_python.RPT_OPT_DATA_REFERENCE
            rcb.set_optional_fields(report_options)

            options = iec61850_python.RCB_ELEMENT_RPT_ENA | iec61850_python.RCB_ELEMENT_OPT_FLDS
            # options = report_options
            logger.info(f"Setting RCB options to {options} ({bin(options)})")
            error = connection.set_report_control_block_values(rcb, options, True)
            logger.info(f"  RCB options set")
            if is_error(error):
                logger.error(f"Could not activate reports for report control block {rcb_reference}: {error=}")
                return False

        return True
