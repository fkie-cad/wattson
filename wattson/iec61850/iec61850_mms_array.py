from typing import List

import iec61850_python
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue
from wattson.util import get_logger


class IEC61850MMSArray:
    def __init__(self, lib_object: iec61850_python.MmsArray):
        self.lib_object = lib_object
        self._iter_index = 0
        self._mms_values: List[IEC61850MMSValue] = []
        self._build_list()
        self._iterator = None

    def _build_list(self):
        i = 0
        while i < self.lib_object.get_size():
            self._mms_values.append(IEC61850MMSValue(self.lib_object.get_element(i)))
            i += 1

    def get_values(self) -> List[IEC61850MMSValue]:
        return self._mms_values

    def __iter__(self):
        self._iterator = self._mms_values.__iter__()
        return self._iterator

    def __next__(self) -> IEC61850MMSValue:
        return next(self._iterator)

    def __len__(self):
        return len(self._mms_values)

    def __getitem__(self, item: int) -> IEC61850MMSValue:
        return self._mms_values[item]

    def __setitem__(self, key: int, value: IEC61850MMSValue):
        self._mms_values[key] = value
