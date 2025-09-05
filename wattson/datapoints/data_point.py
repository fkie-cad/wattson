from typing import Union, TYPE_CHECKING

from wattson.datapoints.interface import DataPointValue

if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class DataPoint:
    """A data point wrapper for maintaining the OOP paradigm"""
    def __init__(self, identifier: str, manager: "DataPointManager"):
        self._identifier = identifier
        self._manager = manager

    @property
    def identifier(self):
        return self._identifier

    @property
    def value(self) -> DataPointValue:
        return self._manager.get_value(self.identifier)

    @value.setter
    def value(self, value: DataPointValue):
        if not self._manager.set_value(self.identifier, value):
            raise RuntimeError("Data point value could not be set")
