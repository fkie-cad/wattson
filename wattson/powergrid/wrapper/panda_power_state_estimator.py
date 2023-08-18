import copy
import logging
import sys
import threading
from pathlib import Path
from threading import Thread, Lock, Event
import typing

import numpy as np

import pandapower as pp
import pandas as pd
from pandas import DataFrame, set_option
import pandapower.estimation.util
from copy import deepcopy

from pandapower.estimation import ALGORITHM_MAPPING
from powerowl.layers.powergrid.elements import Line, Switch
from powerowl.layers.powergrid.values.units.scale import Scale
from powerowl.layers.powergrid.values.units.unit import Unit
from powerowl.simulators.pandapower import PandaPowerGridModel

from wattson.powergrid.wrapper.panda_power_measurement import PandaPowerMeasurement
from wattson.powergrid.wrapper.power_owl_measurement import PowerOwlMeasurement
from wattson.util.hidden_print import HiddenPrint
from wattson.util.log import get_logger
from wattson.util.powernet import sanitize_power_net
from wattson.util.time.virtual_time import VirtualTime


class PandaPowerStateEstimator(Thread):
    global_estimation_lock = threading.Lock()

    def __init__(self,
                 power_grid_model: PandaPowerGridModel,
                 update_required: typing.Optional[Event],
                 estimation_done_callback: typing.Callable,
                 estimation_started_callback: typing.Callable,
                 **kwargs: typing.Any):
        """
        Initializes and configures the state estimator.

        @param power_grid_model: The (PowerOwl) PandaPowerGridModel
        @param update_required: The event to use for triggering a new estimation iteration. Optional.
        @param estimation_done_callback: A callback to call every time the estimation is completed (successful or not)
        @param estimation_started_callback: A callback to call every time a new estimation iteration is started.
        @param kwargs: Additional arguments

        Keyword Arguments:
        * fault_detection (bool) = True --
          Whether to try to detect faulty lines
        * interval (float) = 0.5 --
          The internal waiting interval in seconds
        * max_delay (float) = 10 --
          The maximum time in seconds between two iterations
        * estimation_mode (str) = "default" --
          How to handle measurement validit. Either "default" or "decay"
        * measurement_decay (float) = 12 --
          The number of seconds a periodic measurement is valid. Only used for "decay" mode

        """
        super().__init__()
        self.power_grid_model = power_grid_model
        self.net_lock = threading.Lock()
        self.update_required: Event = update_required if update_required is not None else threading.Event()
        self.estimation_done_callback: typing.Callable = estimation_done_callback
        self.estimation_started_callback: typing.Callable = estimation_started_callback
        self.terminate: Event = Event()
        self.interval = kwargs.get("interval", 0.5)
        self.logger = get_logger("PowerGrid", "StateEstimator")
        self.counter = 0
        self.max_delay = kwargs.get("max_delay", 10)
        self.mode = kwargs.get("estimation_mode", "default")
        self.decay = kwargs.get("measurement_decay", 12)
        self._measurements: typing.Dict[str, PowerOwlMeasurement] = {}
        self._measurement_lock = threading.Lock()
        self.name = kwargs.get("name", "StateEstimator")
        self.virtual_time = kwargs.get("virtual_time", VirtualTime.get_instance())
        self._schedule_lock = threading.Lock()
        self._schedule_start: typing.Optional[float] = None
        self._schedule_max_wait = 5
        self._fault_detection_enabled = kwargs.get("fault_detection", True)

    def run(self) -> None:
        if not self.power_grid_model.is_prepared():
            self.power_grid_model.prepare_simulator()

        while not self.terminate.is_set():
            if self.update_required.wait(self.interval):
                self.update_required.clear()
                with self._schedule_lock:
                    self._schedule_start = None
                self.estimate_state()
                self.counter = 0
            self.counter += 1
            if 0 < self.max_delay < self.counter * self.interval:
                self.update_required.set()

    def stop(self):
        self.terminate.set()

    def measure(self, *measurements: PowerOwlMeasurement):
        with self._measurement_lock:
            changed = False
            for m in measurements:
                measurement = PowerOwlMeasurement.copy(m)
                key = measurement.get_key()
                if self.mode == "default":
                    measurement.timeout = False
                if key in self._measurements:
                    old = self._measurements[key]
                    if old.value != measurement.value:
                        changed = True
                    self._measurements[key].update(measurement)
                    self._queue_iteration(changed)
                else:
                    self._measurements[key] = measurement
                    changed = True
            self._queue_iteration(changed)

    def _queue_iteration(self, changed: bool = False):
        with self._schedule_lock:
            if self._schedule_start is None:
                if changed:
                    self._schedule_start = self.virtual_time.time()
                else:
                    return
            if self._schedule_start < self.virtual_time.time() - self._schedule_max_wait:
                self.update_required.set()

    def get_power_grid_model(self) -> PandaPowerGridModel:
        return self.power_grid_model

    def estimate_state(self) -> None:
        self.estimation_started_callback(self.name)

        with self.net_lock:
            pnet = self.power_grid_model.get_panda_power_net()
        sanitize_power_net(pnet)

        # Clear old measurements
        if "measurement" in pnet:
            pnet.measurement.drop(pnet.measurement.index, inplace=True)

        self.clear_net(pnet)

        # Filter measurement dictionary
        with ((((self._measurement_lock)))):
            prev = len(self._measurements)
            faulty_lines = set()
            measurements = {}
            for key, measurement in self._measurements.items():
                if measurement.is_valid(self.decay, self.virtual_time):
                    measurements[key] = measurement
                    if self._fault_detection_enabled:
                        threshold = 0.0001
                        element = measurement.get_element()

                        if isinstance(element, Line) and \
                                measurement.grid_value.unit == Unit.AMPERE and \
                                measurement.grid_value.name in ["current_from", "current_to", "current"]:

                            max_i = element.get_property("maximum_current")
                            max_ka = max_i.to_scale(Scale.KILO)

                            if abs(measurement.value) < threshold * max_ka:
                                faulty_lines.add(element)

            self._measurements = measurements
            post = len(self._measurements)
            self.logger.info(f"{prev - post} measurements timed-out")
            self._add_measurements_to_net(pnet)

            if self._fault_detection_enabled:
                # Handle faulty lines
                for line in self.power_grid_model.get_elements_by_type("line"):
                    line: Line
                    switches = [switch for switch in self.power_grid_model.get_annotators(line) if isinstance(switch, Switch)]
                    if line in faulty_lines:
                        # Open a switch to allow correct estimation
                        any_opened = False
                        for switch in switches:
                            s_id = switch.index
                            any_opened |= not pnet.switch.at[s_id, "closed"]
                        if not any_opened and len(switches) > 0:
                            self.logger.info(f"{line.get_identifier()} detected as faulty")
                            s_id = switches[0].index
                            self.logger.info(f"Opening Switch {s_id}")
                            pnet.switch.at[s_id, "closed"] = False
                    else:
                        # Switches must be closed
                        for switch in switches:
                            s_id = switch.index
                            if not pnet.switch.at[s_id, "closed"]:
                                self.logger.info(f"Closing Switch {s_id}")

        # Prepare Estimation
        self.drop_nan_measurements(pnet)

        self.logger.info(f"Currently {len(pnet.measurement.index)} valid measurements")
        zero_injection = self.get_zero_injection_busses(pnet)

        # Estimate
        success = False
        algorithm_iterations = {
            "bad": 100,
            'wls': 1000,
            'wls_with_zero_constraint': 100,
            'opt': [50, 1e-08],
            # 'lp': 30,
            # 'irwls': 20,
        }

        used_algorithm = None

        #if len(pnet.measurement) > 100:
        #    pnet.measurement.to_csv("measurements.csv")
        #    sys.exit()
        # pnet.measurement

        with PandaPowerStateEstimator.global_estimation_lock:
            for algorithm, iterations in algorithm_iterations.items():
                alg_args = {}
                if type(iterations) == list:
                    iterations, err = iterations[0], iterations[1]
                    alg_args["tolerance"] = err
                try:
                    self.logger.info(f"Starting Estimation {algorithm=}")
                    with HiddenPrint():
                        if algorithm == "bad":
                            success = pp.estimation.remove_bad_data(pnet, maximum_iterations=iterations)
                        else:
                            success = pp.estimation.estimate(
                                pnet,
                                algorithm=algorithm,
                                init="flat",
                                zero_injection=zero_injection,
                                maximum_iterations=iterations,
                                **alg_args
                                )
                    if not success:
                        self.logger.error(f"{algorithm}: Estimation failed")
                    else:
                        self.logger.info(f"{algorithm}: Estimation success")
                        used_algorithm = algorithm
                        break
                except Exception as e:
                    self.logger.error(f"SE Error {algorithm}: {e=}")
                finally:
                    pass

        # Update to add measurements to the original network's estimation results
        if success:
            with self.net_lock:
                self.logger.info("Copying results")
                for key in pnet.keys():
                    keysplit = key.split("_")
                    if isinstance(pnet[key], DataFrame) and keysplit[-1] == "est":
                        pnet[key].fillna(0, inplace=True)
                        self.logger.debug(f"... {key}")
                        table = key  # .replace("res_", "").replace("_est", "")
                        for index, row in pnet[key].iterrows():
                            for column in pnet[key]:
                                value = row[column]
                                # Scale to PowerOwl
                                _, pandapower_scale = self.power_grid_model.extract_unit_and_scale(column)
                                grid_value = self.power_grid_model.get_grid_value_by_pandapower_path(table, index, column)
                                if grid_value is not None:
                                    grid_value.raw_set_value(value, value_scale=pandapower_scale)
        if not success:
            self.logger.error("Estimation failed")
        self.estimation_done_callback(self.name, success, used_algorithm)

    def drop_nan_measurements(self, net):
        net.measurement["value"].replace(np.nan, 0, inplace=True)

    def fix_disconnected_measurements(self, net):
        ext_busses = net.ext_grid["bus"].to_list()
        connected_busses = self.breadth_search(net, ext_busses)
        busses = list(net.bus.index)
        disconnected_busses = [bus for bus in busses if bus not in connected_busses]

        # Drop Measurements
        df = net.measurement
        drop_index = df[(df["element_type"] == "bus") & (df["element"].isin(disconnected_busses))]
        net.measurement.drop(drop_index.index, inplace=True)
        l = net.line
        disconnected_lines = list(l[(l["from_bus"].isin(disconnected_busses)) | (l["to_bus"].isin(disconnected_busses))].index)
        drop_index = df[(df["element_type"] == "line") & (df["element"].isin(disconnected_lines))]
        net.measurement.drop(drop_index.index, inplace=True)

    def breadth_search(self, net, start):
        found = []
        stack = []
        for i in start:
            stack.append(i)

        while len(stack) > 0:
            e = stack.pop()
            if e in found:
                continue
            found.append(e)
            busses = self.get_adjacent_busses(net, e)
            for bus in busses:
                bid = bus["bus"]
                if bid in found:
                    continue

                if bus["connected"]:
                    if not bid in found:
                        stack.append(bid)
        return found

    def get_adjacent_busses(self, net, bus):
        busses = []

        df = net.line
        line_busses = df[(df["from_bus"] == bus) | (df["to_bus"] == bus)]
        for i, row in line_busses.iterrows():
            bid = row["from_bus"] if bus == row["to_bus"] else row["to_bus"]
            elem = row.to_remote_representation()
            elem["etype"] = "line"
            elem["index"] = i
            busses.append(
                {
                    "bus": bid,
                    "connector": elem,
                    "connected": self.element_is_closed(net, elem)
                }
            )

        df = net.trafo
        trafo_busses = df[(df["lv_bus"] == bus) | (df["hv_bus"] == bus)]
        for i, row in trafo_busses.iterrows():
            bid = row["lv_bus"] if bus == row["hv_bus"] else row["hv_bus"]
            elem = row.to_remote_representation()

            elem["etype"] = "trafo"
            elem["index"] = i
            busses.append(
                {
                    "bus": bid,
                    "connector": elem,
                    "connected": self.element_is_closed(net, elem)
                }
            )

        return busses

    def element_is_closed(self, net, element):
        if not "etype" in element:
            return False
        df = net.switch
        switches = None
        if element["etype"] == "line":
            switches = df[(df["et"] == "l") & (df["element"] == element["index"])]
        elif element["etype"] == "trafo":
            switches = df[(df["et"] == "t") & (df["element"] == element["index"])]
        else:
            return False
        if switches is not None:
            for i, row in switches.iterrows():
                if not row["closed"]:
                    return False
        return True

    def get_zero_injection_busses(self, net):
        zero_injection_buses = np.array(
            list(
                set(net.bus.index) - set(net.load.bus) - set(net.sgen.bus) -
                set(net.shunt.bus) - set(net.gen.bus) -
                set(net.ext_grid.bus) - set(net.ward.bus) -
                set(net.xward.bus)
                )
            )
        return zero_injection_buses

    def drop_zero_injection_bus_measurements(self, net):
        injection_buses = set()
        for table in ["load", "shunt", "sgen", "gen", "ext_grid"]:
            injection_buses.update(net[table]["bus"])
        zero_injection_buses = list(set(net.bus.index) - injection_buses)
        zero_injection_buses = np.array(
            list(
                set(net.bus.index) - set(net.load.bus) - set(net.sgen.bus) -
                set(net.shunt.bus) - set(net.gen.bus) -
                set(net.ext_grid.bus) - set(net.ward.bus) -
                set(net.xward.bus)
                )
            )
        df = net.measurement
        drop_ms = df[(df["measurement_type"].isin(["p", "q"])) & (df["element_type"] == "bus") & (
            df["element"].isin(zero_injection_buses))]

        self.logger.info(f"Will drop {len(drop_ms.index)} measurements of zero-injection buses")
        net.measurement.drop(drop_ms.index, inplace=True)
        # Add fake 0 measurements
        for bus in zero_injection_buses:
            self.logger.info(f"Adding virtual zero measurement for bus {bus}")
            for m_type in ("p", "q"):
                pp.create_measurement(net, meas_type=m_type, element_type='bus', element=bus, value=0, std_dev=0.1)
        pp.toolbox.close_switch_at_line_with_two_open_switches(net)

    def _add_measurements_to_net(self, pnet):
        for measurement in self._measurements.values():
            pp_measurement = PandaPowerMeasurement.transform(measurement)
            pp_measurement.add_as_measurement(pnet)
            # measurement.add_to_net(pnet)

    def clear_net(self, pnet):
        # Drop all non-estimation results
        for key in pnet.keys():
            if isinstance(pnet[key], DataFrame) and key[:3] == "res" \
                    and pnet[key].shape[0] and "est" not in key:
                self.logger.info(f"Clearing {key}")
                pnet[key].loc[:, :] = np.nan
                # pnet[key].drop(pnet[key].index, inplace=True)
