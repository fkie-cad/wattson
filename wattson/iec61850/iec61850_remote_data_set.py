from typing import List, TYPE_CHECKING, Optional, Union

import iec61850_python
from wattson.iec61850.common.iec61850_helpers import is_error
from wattson.iec61850.common.mms_error import MmsError

from wattson.iec61850.iec61850_data_set import IEC61850DataSet
from wattson.iec61850.iec61850_mms_array import IEC61850MMSArray
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue
from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock
    from wattson.iec61850.iec61850_model import IEC61850Model


class IEC61850RemoteDataSet(IEC61850DataSet):
    def __init__(self,
                 name: str,
                 logical_node: Optional['IEC61850LogicalNode'] = None,
                 entries: Optional[List[Union['IEC61850DataAttribute', 'IEC61850DataObject']]] = None) -> None:

        super().__init__(None)
        if entries is None:
            entries = []
        self._name = name
        self.logical_node = logical_node
        self.entries = entries

    @property
    def name(self) -> str:
        return self._name

    def read_from_server(self) -> List[IEC61850MMSValue]:
        connection = self.get_model().connection
        data_set, error = connection.read_data_set_values(self.get_mms_reference(), None)
        if is_error(error):
            raise MmsError("Could not read data set from server")
        mms_array = IEC61850MMSArray(data_set.get_values())
        mms_values = mms_array.get_values()
        # Update attributes
        attributes = self.get_attribute_list()
        if len(attributes) != len(mms_values):
            raise MmsError("Mismatching length of data set contents")
        for i, data_attribute in enumerate(attributes):
            if isinstance(data_attribute, IEC61850RemoteDataAttribute):
                data_attribute.set_value(mms_values[i].value)
        return mms_array.get_values()

    def add_entry(self, entry: Union['IEC61850DataAttribute', 'IEC61850DataObject'], add_to_library: bool = False) -> bool:
        if self.has_entry(entry):
            return False
        self.entries.append(entry)
