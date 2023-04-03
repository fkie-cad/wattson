import os
import signal
import subprocess
import threading
import time

import numpy as np
from c104 import Cot
from typing import Optional
from typing import Optional
from copy import deepcopy

from wattson.datapoints.interface import DataPointValue
from wattson.hosts.rtu.rtu_logic import RTULogic
from wattson.iec104.interface.server import IECServerInterface
from wattson.iec104.interface.types import COT
from wattson.powergrid.common.events import MTU_READY
from wattson.datapoints.providers.pandapower_provider.provider import PandapowerProvider


class CircuitBreakerSafetyLogic(RTULogic):
    def __init__(self, rtu: 'RTU', **kwargs):
        super().__init__(rtu, **kwargs)
        self._monitored_dps = {}
        self._delay = 10
        self._auto_detect = kwargs.get("auto", True)
        self._constraints = kwargs.get("constraints", {})
        self._manual_configuration = kwargs.get("triggers", {})
        self._update_to_switch_mapping = {}
        self._safety_enabled = {}
        self._switch_constraints = {}
        self._pandapower_provider: Optional[PandapowerProvider] = None

    def on_start(self):
        super().on_start()

        switch_providers: dict = self.rtu.manager.provider_search("pandapower", {
            "pp_table": "switch"
        })

        switch_constraints = {}
        # Find Switches with constraints from data points
        trigger_columns = {f"{m}{k}": f"{m}_{kw}" for m in ["u", "i"]
                           for k, kw in {"<<": "under", "<": "low", ">": "high", ">>": "over"}.items()}

        if self._auto_detect:
            for _id, providers in switch_providers.items():
                for provider in providers:
                    data = provider["provider_data"]
                    s_id = int(data["pp_index"])
                    if s_id not in switch_constraints:
                        switch_constraints[s_id] = deepcopy(self._constraints)
                    col = data["pp_column"]
                    if col not in trigger_columns:
                        continue
                    col_name = trigger_columns.get(col)
                    if col_name in switch_constraints[s_id]:
                        continue
                    dp = self.rtu.manager.get_data_point_dict(_id)
                    value = dp.get("value")
                    self.logger.debug(f"{s_id}: {col} = {value}")
                    if col in trigger_columns:
                        switch_constraints.setdefault(s_id, {})[col_name] = value

        # Update with manual configurations
        switch_constraints.update(self._manual_configuration)

        if len(switch_constraints) == 0:
            self.logger.info(f"No switches controlled by this RTU - stopping logic")
            return

        # Find Busses and Lines associated to this switch
        for s_id in switch_constraints.keys():
            element_type = self.rtu.power_net["switch"].at[s_id, "et"]
            element = self.rtu.power_net["switch"].at[s_id, "element"]
            bus = self.rtu.power_net["switch"].at[s_id, "bus"]
            element_table = {
                "b": "res_bus",
                "l": "res_line",
                "t": "res_trafo",
                "t3": "res_trafo3w"
            }.get(element_type)
            switch_info = {
                "bus": bus,
                "bus_voltage": f"res_bus.{bus}.vm_pu",
                "element_type": element_type,
                "element_table": element_table,
                "element_id": element,
                "element_current": None,
                "alarm_state_u": "normal",
                "alarm_state_i": "normal"
            }
            if element_type in ["l", "t"]:
                if element_type == "l":
                    constraints = switch_constraints[s_id]
                    max_ka = self.rtu.power_net["line"].at[element, "max_i_ka"]
                    for key, constraint in constraints.items():
                        if "i_" in key:
                            if type(constraint) == str and "%" in constraint:
                                perc = float(constraint.replace("%", "")) / 100
                                if not np.isnan(max_ka) and max_ka is not None:
                                    nval = max_ka * perc
                                    constraints[key] = nval
                                    self.logger.info(f"Switch {s_id}: {key} = {constraint} of {max_ka} -> {nval}")
                                else:
                                    self.logger.warning(f"Cannot auto adjust constraint {key} = {constraint}")
                    switch_info["element_current"] = f"res_line.{element}.i_ka"
                elif element_type == "t":
                    trafo_hv = self.rtu.power_net["trafo"].at[element, "hv_bus"]
                    side = "hv" if trafo_hv == bus else "lv"
                    switch_info["element_current"] = f"res_trafo.{element}.i_{side}_ka"
            switch_constraints[s_id]["config"] = switch_info

        for s_id, constraints in switch_constraints.items():
            for key, val in constraints.items():
                if key in trigger_columns.values():
                    self.logger.info(f"Switch {s_id}: {key} = {val}")

        pandapower_provider = self.rtu.manager.get_provider("pandapower")
        pandapower_provider.add_on_change(self._on_change)

        element_subscriptions = set()
        for s_id, constraints in switch_constraints.items():
            self._safety_enabled[s_id] = True
            config = constraints["config"]
            self._update_to_switch_mapping[config["bus_voltage"]] = s_id
            element_subscriptions.add(config["bus_voltage"])
            element_current = config["element_current"]
            if element_current is not None:
                self._update_to_switch_mapping[element_current] = s_id
                element_subscriptions.add(element_current)
            element_subscriptions.add(f"switch.{s_id}.closed")
        pandapower_provider: PandapowerProvider
        pandapower_provider.add_filter_paths(element_subscriptions)
        self._pandapower_provider = pandapower_provider

        self._switch_constraints = switch_constraints

    def on_stop(self):
        super().on_stop()

    def _on_change(self, identifier: str, value: DataPointValue, state_id: Optional[str], context_info: Optional[str]):
        if context_info != "PATH":
            return
        path = identifier
        table, index, column = path.split(".", 3)
        index = int(index)
        if table == "switch" and column == "closed":
            s_id = index
            self._safety_enabled[s_id] = value
            if value:
                self.logger.info(f"Enabling Safety Measures for Switch {s_id}")
            else:
                self.logger.info(f"Disabling Safety Measures for Switch {s_id}")
            return

        s_id = self._update_to_switch_mapping.get(path)
        if s_id is None:
            return
        if not self._safety_enabled.get(s_id, False):
            self.logger.debug(f"Safety Measures for Switch {s_id} are disabled")
            return
        switch_data = self._switch_constraints.get(s_id)
        if switch_data is None:
            self.logger.warning(f"Unconfigured Switch {s_id} triggered")
            return
        config = switch_data.get("config")

        # Check if measure triggers
        if table == "res_bus":
            # Voltage Check
            alarm_key = "alarm_state_u"
            last_state = config.get(alarm_key, "normal")
            if column != "vm_pu":
                self.logger.warning(f"Got Bus Measure: {column}")
                return
            n_state = self._calculate_state(prefix="u", s_id=s_id, value=value)
            if last_state != n_state:
                self.logger.info(f"Switch {s_id} entered voltage state: {n_state}")
                config[alarm_key] = n_state
                if n_state in ["under", "over"]:
                    self._trigger_protection(s_id)
        else:
            # Current Check
            alarm_key = "alarm_state_i"
            last_state = config.get(alarm_key, "normal")
            if "ka" not in column:
                self.logger.warning(f"Got Current Measure: {table}.{index}.{column}")
                return
            n_state = self._calculate_state(prefix="i", s_id=s_id, value=value)
            if last_state != n_state:
                self.logger.info(f"Switch {s_id} entered current state: {n_state}")
                config[alarm_key] = n_state
                if n_state in ["under", "over"]:
                    self._trigger_protection(s_id)

    def _calculate_state(self, prefix: str, s_id: int, value: DataPointValue):
        switch_data = self._switch_constraints.get(s_id)
        under = switch_data.get(f"{prefix}_under")
        low = switch_data.get(f"{prefix}_low")
        high = switch_data.get(f"{prefix}_high")
        over = switch_data.get(f"{prefix}_over")
        if under is not None and value <= under:
            n_state = "under"
        elif low is not None and value <= low:
            n_state = "low"
        elif over is not None and value >= over:
            n_state = "over"
        elif high is not None and value >= high:
            n_state = "high"
        else:
            n_state = "normal"
        return n_state

    def _trigger_protection(self, s_id: int):
        # Trigger protection measure
        self.logger.warning(f"Opening Switch {s_id}")
        self._pandapower_provider.client.update_value(
            table="switch",
            index=s_id,
            column="closed",
            value=False
        )

    def configure(self):
        super().configure()
