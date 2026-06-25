from typing import List, TYPE_CHECKING, Optional, Union

import iec61850_python

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_data_set import IEC61850DataSet
    from wattson.iec61850.iec61850_model import IEC61850Model


class IEC61850ReportControlBlock:
    def __init__(self,
                 lib_object: Union[iec61850_python.ReportControlBlock, iec61850_python.RemoteReportControlBlock],
                 logical_node: Optional['IEC61850LogicalNode'] = None,
                 data_set: Optional['IEC61850DataSet'] = None) -> None:

        self.logical_node = logical_node
        self.lib_object = lib_object
        self.data_set = data_set

    @property
    def name(self) -> str:
        if isinstance(self.lib_object, iec61850_python.ReportControlBlock):
            return self.lib_object.get_name()
        return self.lib_object.get_report_id()

    def get_mms_reference(self) -> str:
        return f"{self.get_logical_device().name}/{self.get_logical_node().name}.RP.{self.name}"

    def get_logical_node(self) -> 'IEC61850LogicalNode':
        return self.logical_node

    def get_logical_device(self) -> 'IEC61850LogicalDevice':
        return self.get_logical_node().get_logical_device()

    def get_model(self) -> 'IEC61850Model':
        return self.get_logical_node().get_model()

    def get_data_set(self) -> 'IEC61850DataSet':
        return self.data_set

    def has_data_set(self) -> bool:
        return self.data_set is not None

    def has_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> bool:
        return self.get_data_set().has_data_object(data_object)

    def has_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> bool:
        return self.get_data_set().has_data_attribute(data_attribute)
