import dataclasses
import time
from typing import Any, Optional, Union, TYPE_CHECKING

from powerowl.layers.powergrid.elements import GridElement, Line, Transformer, Bus
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.units.unit import Unit

from wattson.util.time.virtual_time import VirtualTime

import pandapower as pp


@dataclasses.dataclass
class PowerOwlMeasurement:
    grid_value: GridValue
    value: Any
    timeout: bool = False
    timestamp: float = 0

    def get_key(self):
        return f"{self.grid_value.get_identifier()}"

    def is_valid(self, timeout, virtual_time: Optional[VirtualTime]):
        t = time.time if virtual_time is None else virtual_time.time
        valid = not self.timeout or self.timestamp > t() - timeout

        return valid

    def update(self, value: Union[Any, 'PowerOwlMeasurement'], timestamp: Optional[float] = None,
               timeout: Optional[bool] = None):
        if isinstance(value, PowerOwlMeasurement):
            # Copy from Measurement object
            if self.get_key() != value.get_key():
                raise ValueError("Invalid measurement supplied for update - non-matching key")
            if timestamp is not None or timeout is not None:
                raise AttributeError("Timestamp and Timeout invalid arguments when supplying a measurement object")
            self.value = value.value
            self.timeout = value.timeout
            self.timestamp = value.timestamp
        else:
            # Update from raw value
            self.value = value
            if self.timeout or timeout:
                self.timeout = True
                if timestamp is None:
                    self.timestamp = time.time()
                else:
                    self.timestamp = timestamp

    def get_element(self) -> GridElement:
        return self.grid_value.get_grid_element()

    def get_element_type(self) -> str:
        prefix = self.get_element().prefix
        return prefix

    def get_element_index(self):
        return self.grid_value.get_grid_element().index

    def get_measurement_type(self):
        unit = self.grid_value.unit
        if unit == Unit.WATT:
            return "p"
        if unit == Unit.VAR:
            return "q"
        if unit == Unit.AMPERE:
            return "i"
        if unit == Unit.VOLT:
            return "v"
        return None

    def get_side(self):
        if not isinstance(self.get_element(), (Line, Transformer)):
            return None
        if self.get_measurement_type() is None:
            return None
        name_parts = self.grid_value.name.split("_")
        valid_sides = ["from", "to", "lv", "hv"]
        for valid_side in valid_sides:
            if valid_side in name_parts:
                return valid_side
        return None

    def add_to_net(self, net: 'pp.pandapowerNet'):
        net[self.table].at[self.index, self.column] = self.value

    @property
    def std_dev(self):
        return {
            "v": 0.001,
            "i": 0.01,
            "p": 0.03,
            "q": 0.03
        }.get(self.get_measurement_type())

    def add_as_measurement(self, net: 'pp.pandapowerNet'):
        if not isinstance(self.get_element(), (Bus, Line, Transformer)):
            return
        m_type = self.get_measurement_type()
        if m_type not in ["v", "i", "p", "q"]:
            return
        if isinstance(self.get_element(), Bus):
            pp.create_measurement(net, meas_type=m_type, element_type='bus', element=self.index,
                                  value=self.value, std_dev=self.std_dev)
        else:
            side = self.get_side()
            if side is None:
                return
            pp.create_measurement(net, meas_type=m_type, element_type=self.get_element_type(), element=self.index,
                                  value=self.value, std_dev=self.std_dev, side=side)

    def is_relevant(self):
        if not isinstance(self.get_element(), (Bus, Line, Transformer)):
            return False
        m_type = self.get_measurement_type()
        if m_type not in ["v", "i", "p", "q"]:
            return False
        if not isinstance(self.get_element(), Bus):
            if self.get_side() is None:
                return False
        return True

    @staticmethod
    def copy(other: 'PowerOwlMeasurement'):
        o = other
        return PowerOwlMeasurement(o.grid_value, o.value, o.timeout, o.timestamp)

    def to_dict(self):
        return {
            "element": self.get_element().get_identifier(),
            "grid-value": self.grid_value.get_identifier(),
            "type": self.get_measurement_type(),
            "s_value": self.pandapower_value,
            "value": self.value,
            "stddev": self.std_dev,
            "side": self.get_side()
        }
