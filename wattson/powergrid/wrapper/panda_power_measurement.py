import dataclasses
import time
import typing
from typing import Any, Optional, Union, TYPE_CHECKING

from powerowl.layers.powergrid.elements import Switch
from powerowl.simulators.pandapower import PandaPowerGridModel

from wattson.powergrid.wrapper.power_owl_measurement import PowerOwlMeasurement
from wattson.util.time.virtual_time import VirtualTime

import pandapower as pp


@dataclasses.dataclass
class PandaPowerMeasurement(PowerOwlMeasurement):
    @property
    def table(self) -> str:
        return self.grid_value.simulator_context[0]

    @property
    def index(self) -> int:
        return  self.grid_value.simulator_context[1]

    @property
    def column(self) -> str:
        return self.grid_value.simulator_context[2]

    @property
    def pandapower_value(self):
        _, target_scale = PandaPowerGridModel.extract_unit_and_scale(self.column)
        source_scale = self.grid_value.scale
        pandapower_value = target_scale.from_scale(self.value, source_scale)
        return pandapower_value

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
        net[self.table].at[self.index, self.column] = self.pandapower_value

    @property
    def std_dev(self):
        return {
            "v": 0.001,
            "i": 0.01,
            "p": 0.03,
            "q": 0.03
        }.get(self.get_measurement_type())

    def add_as_measurement(self, net: 'pp.pandapowerNet'):
        if isinstance(self.get_element(), Switch):
            switch: Switch = typing.cast(Switch, self.get_element())
            if self.grid_value.name == "is_closed":
                net["switch"].at[switch.index, self.grid_value.simulator_context[2]] = self.value
                return

        if self.get_element_type() not in ["bus", "line", "trafo", "trafo3w"]:
            return
        if self.get_measurement_type() not in ["v", "i", "p", "q"]:
            return

        m_type = self.get_measurement_type()
        if self.get_element_type() == "bus":
            pp.create_measurement(net, meas_type=m_type, element_type='bus', element=self.index,
                                  value=self.pandapower_value, std_dev=self.std_dev)
        else:
            side = self.get_side()
            if side is None:
                return
            pp.create_measurement(net, meas_type=m_type, element_type=self.get_element_type(), element=self.index,
                                  value=self.pandapower_value, std_dev=self.std_dev, side=side)

    @staticmethod
    def copy(other: 'PandaPowerMeasurement'):
        o = other
        return PandaPowerMeasurement(o.grid_value, o.value, o.timeout, o.timestamp)

    @staticmethod
    def transform(other: PowerOwlMeasurement) -> 'PandaPowerMeasurement':
        other.__class__ = PandaPowerMeasurement
        return typing.cast(PandaPowerMeasurement, other)
