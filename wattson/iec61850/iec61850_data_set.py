from typing import List, TYPE_CHECKING, Optional, Union

import iec61850_python

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock
    from wattson.iec61850.iec61850_model import IEC61850Model


class IEC61850DataSet:
    def __init__(self,
                 lib_object: Union[iec61850_python.DataSet, iec61850_python.RemoteDataSet],
                 logical_node: Optional['IEC61850LogicalNode'] = None,
                 entries: Optional[List[Union['IEC61850DataAttribute', 'IEC61850DataObject']]] = None) -> None:

        if entries is None:
            entries = []
        self.logical_node = logical_node
        self.lib_object = lib_object
        self.entries = entries

    @property
    def name(self) -> str:
        if isinstance(self.lib_object, iec61850_python.DataSet):
            return self.lib_object.get_name()
        return self.lib_object.get_reference().split("$")[-1]

    def read_from_server(self):
        raise NotImplementedError("Not implemented for non-remote data set")

    def get_mms_reference(self) -> str:
        # TODO: Fix
        # return f"{self.get_logical_node().reference}.DS.{self.name}"
        return f"{self.get_logical_node().reference}.{self.name}"

    def get_logical_node(self) -> 'IEC61850LogicalNode':
        return self.logical_node

    def get_logical_device(self) -> 'IEC61850LogicalDevice':
        return self.get_logical_node().get_logical_device()

    def get_model(self) -> 'IEC61850Model':
        return self.get_logical_node().get_model()

    def add_entry(self, entry: Union['IEC61850DataAttribute', 'IEC61850DataObject'], add_to_library: bool = True) -> bool:
        if self.has_entry(entry):
            return False
        self.entries.append(entry)
        if add_to_library:
            self.lib_object.add_entry(entry.get_mms_path(), -1, None)

    def get_entry(self, entry: Union[str, 'IEC61850DataObject', 'IEC61850DataAttribute']) -> Union['IEC61850DataAttribute', 'IEC61850DataObject']:
        entry_name = entry
        if not isinstance(entry, str):
            entry_name = entry.name
        if not self.has_entry(entry):
            raise KeyError(f"DataSet {self.name} does not have {entry_name}")
        if isinstance(entry, str):
            for existing_entry in self.entries:
                if existing_entry.name == entry:
                    return existing_entry
        return entry

    def has_entry(self, entry: Union['IEC61850DataAttribute', 'IEC61850DataObject', str]) -> bool:
        if isinstance(entry, str):
            for existing_entry in self.entries:
                if existing_entry.get_name() == entry:
                    return True
            return False
        return entry in self.entries

    def has_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> bool:
        if isinstance(data_object, str):
            for entry in self.entries:
                if entry.name == data_object:
                    return True
            return False
        return data_object in self.entries

    def has_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> bool:
        if isinstance(data_attribute, str):
            for entry in self.entries:
                if entry.name == data_attribute:
                    return True
            return False
        return data_attribute in self.entries

    def get_attribute_list(self) -> List['IEC61850DataAttribute']:
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
        return [attribute for attribute in self.entries if isinstance(attribute, IEC61850DataAttribute)]
