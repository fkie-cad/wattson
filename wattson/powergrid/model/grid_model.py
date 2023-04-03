import copy
import json
import logging
import pathlib
import pickle
import queue
import threading
import datetime
import time
from pathlib import Path
from typing import Callable, Optional, List, Dict

import numpy as np
import pandas as pd

import pandapower as pp
import pandas as pandas
import pytz
from pandas import DataFrame

from wattson.apps.interface.util import messages
from wattson.datapoints.interface import DataPointValue
from wattson.powergrid.model.measurement import Measurement
from wattson.powergrid.model.state_estimator import StateEstimator
from wattson.util import get_logger
from wattson.util.powernet import sanitize_power_net
from wattson.util.time.virtual_time import VirtualTime


class GridModel:
    def __init__(self, grid: pp.pandapowerNet, datapoints: dict, logger: Optional[logging.Logger], **kwargs):
        self.net = copy.deepcopy(grid)
        self.net = sanitize_power_net(grid)
        self.virtual_time = kwargs.get("virtual_time", VirtualTime.get_instance())

        for key in self.net.keys():
            if isinstance(self.net[key], DataFrame) and key[:3] == "res":
                for col in self.net[key].columns:
                    self.net[key][col].values[:] = np.nan

        self.data_points = datapoints
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("GridModel", "GridModel")
        self.lock = threading.Lock()
        self._dp_cache = {}
        self._export_folder = None
        self._export_interval = 1
        self._stop_export_event = threading.Event()
        self._export_thread = None
        self._export_ref_time_callback = None

        self._last_power_flow = 0
        self._power_flow_required = threading.Event()

        self._callbacks = {
            "on_element_change": [],
            "on_power_flow_completed": [],
            "on_data_point_update": [],
        }

        self._power_flow_thread = None
        self._on_power_flow_completed = kwargs.get("on_power_flow_completed", None)
        self._automatic_power_flow = kwargs.get("enable_automatic_power_flow", False)
        self._stop_power_flow_event = threading.Event()
        self._power_flow_min_interval_s = kwargs.get("power_flow_min_interval_s", 1)
        self._power_flow_lock = threading.Lock()

        self._measurement_thread = None
        self._export_measurements = False
        self._export_measurements_root = None
        self._export_measurements_files = {}
        self._export_measurements_ref_time_callback = None
        self._export_measurements_queue = queue.Queue()
        self._stop_export_measurement_event = threading.Event()

        self._dp_timeout_s = 15
        self._dp_timings = {}
        self._dp_timing_lock = threading.Lock()
        self._dp_timing_thread = None
        self._dp_timing_enabled = False
        self._dp_timing_terminate = threading.Event()

        self._stop_se_export_event = threading.Event()
        self._export_se_thread = None
        self._export_se_queue = queue.Queue()
        self._export_se_ref_time_callback = None
        self._estimator_lock = threading.Lock()
        self._state_estimators: Dict[str, Dict] = {}
        self._enable_state_estimation = kwargs.get("enable_state_estimation", False)
        # SE mode: default or decay
        self._state_estimation_mode = kwargs.get("state_estimation_mode", "default")
        # Amount of seconds a periodic measurement remains valid
        self._state_estimation_decay = kwargs.get("state_estimation_decay", 12)
        self._state_estimation_name = kwargs.get("state_estimation_name", "model_default")
        self._state_estimation_required = threading.Event()
        if self._enable_state_estimation:
            self.start_state_estimation(self._state_estimation_name)
        if self._automatic_power_flow:
            self.start_power_flow_loop()

    def handle_measurement(self, update: messages.ProcessInfoMonitoring):
        try:
            # Update Pandapower
            coa = update.coa
            ioas = list(update.val_map.keys())
            if self._dp_timing_enabled and update.cot == 3:
                arrival = time.time()
                with self._dp_timing_lock:
                    for ioa in ioas:
                        dp_id = f"{coa}.{ioa}"
                        self._dp_timings.setdefault(dp_id, {
                            "state": "on-time",
                            "last-seen": 0
                        })["last_arrival"] = arrival

            if self._export_measurements:
                ref_time = self._export_measurements_ref_time_callback()
                val_map = copy.deepcopy(update.val_map)
                self._export_measurements_queue.put({
                    "coa": coa,
                    "value_map": val_map,
                    "sim-time": ref_time,
                    "clock-time": time.time()
                })
            changed = False
            with self.lock:
                for ioa, value in update.val_map.items():
                    changed |= self.update_pandapower(coa, ioa, value)
            return changed
        except Exception as e:
            raise e

    def run_power_flow(self) -> bool:
        try:
            self._power_flow_required.clear()
            with self._power_flow_lock:
                pp.runpp(self.net)
            self.logger.debug("Power Flow done")
            self._last_power_flow = time.time()
            if self._on_power_flow_completed is not None:
                self._on_power_flow_completed(True)
            return True
        except Exception as e:
            self.logger.error("Power Flow failed")
            self.logger.error(f"{e=}")
            self._power_flow_required.set()
            if self._on_power_flow_completed is not None:
                self._on_power_flow_completed(False)
            return False

    def get_net(self):
        return copy.deepcopy(self.net)

    def get_value(self, table, index, column):
        with self._power_flow_lock:
            return self.net[table].at[index, column]

    def start_measurement_export(self, folder: Path, ts_callback: Callable):
        self.logger.info(f"Enabling measurement exports to {folder.__str__()}")
        self._export_measurements = True
        self._export_measurements_root = folder
        self._export_measurements_ref_time_callback = ts_callback
        self._measurement_thread = threading.Thread(target=self._measurement_export_loop)
        self._measurement_thread.start()

    def start_periodic_export(self, folder: Path, interval: float, ts_callback: Callable):
        self.logger.info(f"Enabling grid exports every {interval} seconds to {folder.__str__()}")
        self._export_folder = folder
        self._export_interval = interval
        self._export_ref_time_callback = ts_callback
        self._export_thread = threading.Thread(target=self._periodic_export_loop)
        self._export_thread.start()

    def start_timing_monitoring(self, timeout_seconds: float = 15):
        self.logger.info(f"Enabling data point timeout monitoring with timeout {timeout_seconds} s")
        self._dp_timing_enabled = True
        self._dp_timeout_s = timeout_seconds
        self._dp_timing_thread = threading.Thread(target=self._timing_monitoring_loop)
        self._dp_timing_thread.start()

    def start_state_estimation(self, name: str,
                               estimation_mode: Optional[str] = None,
                               measurement_decay: Optional[float] = None,
                               export: bool = False,
                               ts_callback: Optional[Callable] = None,
                               export_folder: Optional[Path] = None) -> bool:
        with self._estimator_lock:
            if estimation_mode is None:
                estimation_mode = self._state_estimation_mode
            if measurement_decay is None:
                measurement_decay = self._state_estimation_decay
            if export and ts_callback is None:
                raise AttributeError("ts_callback required when export is True")
            if export and export_folder is None:
                raise AttributeError("export_folder required when export is True")
            if name in self._state_estimators:
                self.logger.warning(f"StateEstimator '{name}' is already running")
                return False

            self._state_estimators[name] = {
                "estimator": StateEstimator(
                    power_net=self.net,
                    update_required=threading.Event(),
                    estimation_done_callback=self._estimation_done,
                    estimation_started_callback=self._estimation_started,
                    estimation_mode=estimation_mode,
                    measurement_decay=measurement_decay,
                    virtual_time=self.virtual_time,
                    name=name
                ),
                "name": name,
                "export": export,
                "ts_callback": ts_callback,
                "folder": export_folder
            }
            self.logger.info(f"Starting StateEstimator {name} with mode {estimation_mode}")
            self._state_estimators[name]["estimator"].start()
        if export:
            self.logger.info(f"Starting StateEstimation Export for {name} to {export_folder}")
            if self._export_se_thread is None:
                self._export_se_thread = threading.Thread(target=self._estimation_export_loop)
                self._export_se_thread.start()

    def _timing_monitoring_loop(self):
        while not self._dp_timing_terminate.is_set():
            now = time.time()
            with self._dp_timing_lock:
                for dp_id, timing_info in self._dp_timings.items():
                    delay = now - timing_info["last_arrival"]
                    if delay > self._dp_timeout_s:
                        state = "delayed"
                    else:
                        state = "on-time"
                    current_state = timing_info["state"]
                    if state != current_state:
                        timing_info["state"] = state
                        if state == "delayed":
                            timing_info["last-seen"] = timing_info["last_arrival"]
                            self.logger.warning(f"Data Point {str(dp_id).ljust(12)} is delayed!")
                        else:
                            seen_after = now - timing_info["last-seen"]
                            self.logger.info(f"Data Point {str(dp_id).ljust(10)} seen again after {seen_after} s")
            time.sleep(1)

    def _measurement_export_loop(self):
        while not self._stop_export_measurement_event.is_set():
            try:
                update = self._export_measurements_queue.get(block=True, timeout=1)
                coa = update["coa"]
                handle = self._export_measurements_files.get(coa)
                if handle is None:
                    self.logger.debug(f"Creating measurement export file for COA {coa}")
                    file: pathlib.Path = self._export_measurements_root.joinpath(f"measurements-{coa}.json.log")
                    file.touch(0o755, exist_ok=True)
                    handle = open(file=file, mode="w")
                    self._export_measurements_files[coa] = handle
                handle.write(json.dumps(update) + "\n")
                handle.flush()
            except queue.Empty:
                pass

    def _estimation_export_loop(self):
        while not self._stop_se_export_event.is_set():
            try:
                task = self._export_se_queue.get(True, 1)
            except queue.Empty:
                continue
            if task is None:
                continue
            estimator_info = self._state_estimators.get(task)
            if estimator_info is None:
                self.logger.warning(f"Inconsistent state: SE {task} not found")
            if estimator_info["export"]:
                self.logger.debug(f"Exporting: {task}")
                e: 'StateEstimator' = estimator_info["estimator"]
                ref_time = estimator_info["ts_callback"]()
                ref_time = datetime.datetime.fromtimestamp(ref_time, tz=pytz.UTC)
                folder = estimator_info["folder"]
                self.export(e.get_power_net(), folder, ref_time)

    def _periodic_export_loop(self):
        last_export = 0
        while not self._stop_export_event.is_set():
            try:
                diff = time.time() - last_export
                if diff > self._export_interval:
                    with self.lock:
                        ref_time = self._export_ref_time_callback()
                        ref_time = datetime.datetime.fromtimestamp(ref_time, tz=pytz.UTC)
                        self.export(self.net, self._export_folder, ref_time)
                        last_export = time.time()
            except Exception as e:
                self.logger.error(f"Could not export: {repr(e)}")

    def start_power_flow_loop(self):
        self._power_flow_thread = threading.Thread(target=self._power_flow_loop)
        self._power_flow_thread.start()

    def stop_power_flow_loop(self):
        self._stop_power_flow_event.set()

    def _power_flow_loop(self):
        while not self._stop_power_flow_event.is_set():
            if self._power_flow_required.is_set():
                if time.time() - self._last_power_flow > self._power_flow_min_interval_s:
                    self.run_power_flow()
            time.sleep(self._power_flow_min_interval_s / 2)

    def stop_periodic_export(self):
        self._stop_export_event.set()
        if self._export_thread is not None:
            self._export_thread.join()

    def stop_timing_monitoring(self):
        self._dp_timing_terminate.set()
        if self._dp_timing_thread is not None:
            self._dp_timing_thread.join()

    def stop_measurement_export(self):
        self._stop_export_measurement_event.set()
        if self._measurement_thread is not None:
            self._measurement_thread.join()
        # Close open files
        for handle in self._export_measurements_files.values():
            handle.close()

    def stop_estimations(self):
        self._stop_se_export_event.set()
        for key in self._state_estimators.keys():
            self.stop_estimation(key)

    def stop_estimation(self, name):
        estimator_info = self._state_estimators.get(name)
        if estimator_info is None:
            return False
        estimator_info["estimator"].stop()
        estimator_info["estimator"].join()
        return True

    def _estimation_done(self, name: str, success: bool):
        self.logger.debug(f"SE for {name}: {success=}")
        if success and self._state_estimators.get(name, {}).get("export", False):
            self.logger.debug(f"Scheduling export for {name}")
            self._export_se_queue.put(name)

    def _estimation_started(self, name: str):
        self.logger.debug(f"Started Estimation {name}")

    def export(self, net, folder: Path, ref_time):
        #ref_time = self.coordinator.get_current_simulated_time()
        filename = ref_time.strftime('%Y-%m-%d-%H-%M-%S-%f')[:-3] + ".grid.p"
        file = folder.joinpath(filename)
        pickle.dump(net, open(file.absolute().__str__(), "wb"))
        file.chmod(0o755)

    def get_dataframe_row(self, net, ref_time: datetime.datetime, table_cols: Optional[List[str]] = None):
        data = {"datetime": [ref_time.timestamp()]}
        if table_cols is None:
            table_cols = ["*"]
        for table in dir(net):
            if isinstance(net[table], pd.DataFrame):
                for col in net[table].columns:
                    if f"{table}.{col}" in table_cols or "*" in table_cols:
                        for index in net[table].index:
                            key = f"{table}.{col}.{index}"
                            data[key] = [net[table].at[index, col]]
        df = pd.DataFrame(data)
        df.set_index("datetime")
        return df

    def on_element_change(self, callback: Callable[[str, int, str, DataPointValue, DataPointValue], None]):
        self._callbacks["on_element_change"].append(callback)

    def _notify_on_element_change(self, table: str, index: int, column: str,
                                  old_value: DataPointValue, new_value: DataPointValue):

        for callback in self._callbacks["on_element_change"]:
            callback(table, index, column, old_value, new_value)

    def update_grid_element(self, table, index, column, value):
        old_val = self.net[table].at[index, column]
        if old_val != value:
            self.net[table].at[index, column] = value
            self._power_flow_required.set()
            self._notify_on_element_change(table, index, column, old_val, value)
            return True
        return False

    def update_pandapower(self, coa, ioa, value):
        dp = self._get_datapoint(coa, ioa)
        if dp is not None:
            info = self.get_pandapower_element(dp)
            type_id = self._dp_get_type_id(dp)
            if type_id == 45:
                value = bool(value)
            if info is None:
                return False
            table = info["pp_table"]
            col = info["pp_column"]
            index = info["pp_index"]
            changed = self.update_grid_element(table, index, col, value)
            # Add measurements to state estimators
            timeout = False
            if dp["protocol_data"]["cot"] == 1:
                timeout = True
            measurement = Measurement(table, index, col, value, timeout, self.virtual_time.time())
            for estimator_info in self._state_estimators.values():
                estimator_info["estimator"].measure(measurement)
            return changed
        return False

    def get_measurement(self, coa, ioa, value, arrival_time: Optional[float] = None) -> Optional[Measurement]:
        dp = self._get_datapoint(coa, ioa)
        if dp is not None:
            info = self.get_pandapower_element(dp)
            if info is None:
                return None
            table = info["pp_table"]
            col = info["pp_column"]
            index = info["pp_index"]
            timeout = False
            arrival_time = arrival_time if arrival_time is not None else self.virtual_time.time()
            if int(dp["protocol_data"]["cot"]) == 1:
                timeout = True
            measurement = Measurement(table, index, col, value, timeout, arrival_time)
            return measurement
        return None

    def get_pandapower_element(self, dp) -> Optional[dict]:
        if dp is not None:
            provider = self._get_pandapower_provider(dp, "sources")
            if provider is None:
                provider = self._get_pandapower_provider(dp, "targets")
            if provider is None:
                if "pp_info" in dp and dp["pp_info"].get("identity", True):
                    return dp["pp_info"]
                return None
            return provider
        return None

    def get_pandapower_elements(self, pp_type: str) -> Optional[pandas.DataFrame]:
        """
        Returns the pandapower elements of the specified type
        :param pp_type: The elements to return, e.g., "sgen"
        :return: A dataframe with said elements in case the pp_type is valid. Otherwise, None
        """
        if pp_type in self.net:
            return copy.deepcopy(self.net[pp_type])
        return None

    def get_datapoint(self, pp_type, pp_index, pp_column, direction) -> Optional[dict]:
        points = self.get_datapoints_for_element(pp_type, pp_index)
        for p in points:
            iec = self.get_104_info(p)
            if iec is None:
                continue
            provider_direction = "sources" if direction == "monitoring" else "targets"
            provider = self._get_pandapower_provider(p, provider_direction)
            if iec["direction"] == direction and provider is not None:
                if provider.get("pp_column") == pp_column:
                    return p
        return None

    def get_datapoints_for_element(self, pp_type, pp_index) -> list:
        """
        Given a single pandapower element, e.g., "sgen" 1, compiles a list of data points that are associated with
        this element.
        :param pp_type: The pandapower element type
        :param pp_index: The pandapower element index
        :return: A list of datapoints
        """
        result = []
        for device, dps in self.data_points.items():
            for dp in dps:
                control_provider = self._get_pandapower_provider(dp, "targets")
                monitoring_provider = self._get_pandapower_provider(dp, "sources")
                for p in [control_provider, monitoring_provider]:
                    if p is None:
                        continue
                    table = p["pp_table"].replace("res_", "")
                    index = p["pp_index"]
                    if table == pp_type and index == pp_index:
                        result.append(dp)
                        break
        return result

    def _get_pandapower_provider(self, dp, direction: str):
        if direction not in dp["providers"]:
            return None
        candidates = []
        for provider in dp["providers"][direction]:
            if provider["provider_type"] == "pandapower":
                candidates.append(provider["provider_data"])
        if len(candidates) != 1:
            return None
        return candidates[0]

    def _get_datapoint(self, coa, ioa):
        key = f"{coa}.{ioa}"
        if key in self._dp_cache:
            return self._dp_cache[key]
        for host, dps in self.data_points.items():
            for dp in dps:
                info = self.get_104_info(dp)
                if info is not None:
                    if info["coa"] == coa and info["ioa"] == ioa:
                        self._dp_cache[key] = dp
                        return dp
        return None

    def get_104_info(self, dp):
        if dp["protocol"] == "60870-5-104":
            return dp["protocol_data"]

        return None

    def _dp_get_type_id(self, dp):
        info = self.get_104_info(dp)
        if info is not None:
            return info.get("type_id")
        return None
