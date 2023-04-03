import dataclasses
import time
from typing import Any, Optional, Union, TYPE_CHECKING

from wattson.util.time.virtual_time import VirtualTime

import pandapower as pp


@dataclasses.dataclass
class Measurement:
    table: str
    index: int
    column: str
    value: Any
    timeout: bool = False
    timestamp: float = 0

    def get_key(self):
        return f"{self.table}.{self.index}.{self.column}"

    def is_valid(self, timeout, virtual_time: Optional[VirtualTime]):
        t = time.time if virtual_time is None else virtual_time.time
        valid = not self.timeout or self.timestamp > t() - timeout

        return valid

    def update(self, value: Union[Any, 'Measurement'], timestamp: Optional[float] = None,
               timeout: Optional[bool] = None):
        if isinstance(value, Measurement):
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

    def get_element_type(self):
        t = self.table.replace("res_", "").replace("_est", "")
        if t == "trafo":
            t = "transformer"
        return t

    def get_element_index(self):
        return self.index

    def get_measurement_type(self):
        return {
            "vm_pu": "v",
            "i_ka": "i",
            "i_from_ka": "i",
            "i_to_ka": "i",
            "i_hv_ka": "i",
            "i_mv_ka": "i",
            "i_lv_ka": "i",
            "p_mw": "p",
            "p_from_mw": "p",
            "p_to_mw": "p",
            "p_hv_mw": "p",
            "p_mv_mw": "p",
            "p_lv_mw": "p",
            "q_mvar": "q",
            "q_from_mvar": "q",
            "q_to_mvar": "q",
            "q_hv_mvar": "q",
            "q_mv_mvar": "q",
            "q_lv_mvar": "q"
        }.get(self.column)

    def get_side(self):
        if self.get_element_type() not in ["line", "trafo", "trafo3w"]:
            return None
        if self.get_measurement_type() is None:
            return None
        if len(self.column.split("_")) != 3:
            return None
        return self.column.split("_")[1]

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
        if self.get_element_type() not in ["bus", "line", "trafo", "trafo3w"]:
            return
        if self.get_measurement_type() not in ["v", "i", "p", "q"]:
            return

        m_type = self.get_measurement_type()
        if self.get_element_type() == "bus":
            pp.create_measurement(net, meas_type=m_type, element_type='bus', element=self.index,
                                  value=self.value, std_dev=self.std_dev)
        else:
            side = self.get_side()
            if side is None:
                return
            pp.create_measurement(net, meas_type=m_type, element_type=self.get_element_type(), element=self.index,
                                  value=self.value, std_dev=self.std_dev, side=side)

    @staticmethod
    def copy(other: 'Measurement'):
        o = other
        return Measurement(o.table, o.index, o.column, o.value, o.timeout, o.timestamp)
