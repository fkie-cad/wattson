from typing import List, Tuple, Optional

import iec61850_python
from wattson.iec61850.iec61850_mms_array import IEC61850MMSArray
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue
from wattson.iec61850.iec61850_model import IEC61850Model
from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute
from wattson.util import get_logger


class IEC61850MMSReport:
    def __init__(self, lib_object: iec61850_python.RemoteReport, model: IEC61850Model):
        self.model = model
        self.lib_object = lib_object
        # Get RCB
        report_control_block_reference = self.lib_object.get_remote_control_block_reference()
        self.report_control_block = self.model.find_report_control_block(report_control_block_reference)
        # Get data set
        self.data_set = self.report_control_block.get_data_set()
        self.mms_values: List[IEC61850MMSValue] = []
        self._extract_mms_values()

    def _extract_mms_values(self):
        if self.data_set is None:
            print("No data set", flush=True)
            return

        attributes = self.data_set.get_attribute_list()
        values = self.lib_object.get_data_set_values()
        mms_array = IEC61850MMSArray(values)
        if len(attributes) != len(mms_array):
            raise AttributeError("Incompatible length of data set attributes and report values")
        for i, attribute in enumerate(attributes):
            mms_value = mms_array[i]
            mms_value.data_attribute = attribute
            self.mms_values.append(mms_value)

    def get_values(self) -> List[IEC61850MMSValue]:
        return self.mms_values

    def get_report_entries(self) -> List[Tuple[IEC61850MMSValue, Optional[IEC61850RemoteDataAttribute]]]:
        return [(mms_value, mms_value.data_attribute) for mms_value in self.get_values()]
