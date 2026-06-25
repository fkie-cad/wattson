from typing import List, TYPE_CHECKING, Optional, Union

import iec61850_python

from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_remote_data_set import IEC61850RemoteDataSet
    from wattson.iec61850.iec61850_model import IEC61850Model


class IEC61850RemoteReportControlBlock(IEC61850ReportControlBlock):
    def __init__(self,
                 name: str,
                 logical_node: Optional['IEC61850LogicalNode'] = None,
                 data_set: Optional['IEC61850RemoteDataSet'] = None) -> None:

        super().__init__(None)
        self._name = name
        self.logical_node = logical_node
        self.data_set = data_set

    @property
    def name(self) -> str:
        return self._name

    def get_data_set(self) -> 'IEC61850RemoteDataSet':
        return self.data_set

    def has_data_set(self) -> bool:
        return self.data_set is not None
