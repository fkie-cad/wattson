import copy
import csv
import datetime
import fnmatch
import json
import logging
import pathlib
import pickle
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional, List, Dict, Any, Union

import pandas as pd
import pytz
from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext

from wattson.apps.interface.util import messages
from wattson.powergrid.wrapper.power_owl_measurement import PowerOwlMeasurement
from wattson.powergrid.wrapper.state_estimator import StateEstimator
from wattson.powergrid.wrapper.panda_power_state_estimator import PandaPowerStateEstimator
from wattson.time import WattsonTime, WattsonTimeType
from wattson.util import get_logger
from wattson.util.time.virtual_time import VirtualTime


class GridWrapper:
    """
    This class wraps a PowerGridModel and provides additional functionalities for analysing the grid and associated measurements.
    """
    def __init__(self, power_grid_model: PowerGridModel, datapoints: dict, logger: Optional[logging.Logger], **kwargs):
        self.power_grid_model = power_grid_model
        # The virtual time is used to synchronize actions with arbitrary time-speeds
        self.virtual_time = kwargs.get("virtual_time", VirtualTime.get_instance())

        self.data_points = datapoints
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("GridWrapper")
        self.lock = threading.Lock()
        self._dp_cache = {}
        self._export_folder = None
        self._export_interval = 1
        self._stop_export_event = threading.Event()
        self._export_thread = None
        self._export_wattson_time = None

        self._last_power_flow = 0
        self._power_flow_required = threading.Event()

        self.wattson_time = kwargs.get("wattson_time", WattsonTime())

        self._callbacks = {
            "on_element_change": [],
            "on_element_update": [],
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
        self._export_measurements_wattson_time_callback = None
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

    def get_data_point(self, coa, ioa):
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

    def get_grid_values_for_data_point(self, data_point) -> List[GridValue]:
        grid_values = []
        providers = data_point.get("providers", {})
        sources = providers.get("sources", [])
        for provider in sources:
            if provider["provider_type"] == "POWER_GRID":
                provider_data = provider["provider_data"]
                context = provider_data["context"]
                attribute = provider_data["attribute"]
                grid_element = provider_data["grid_element"]
                grid_value = self.power_grid_model.get_grid_value_by_identifier(
                    f"{grid_element}.{context}.{attribute}"
                )
                grid_values.append(grid_value)
        return grid_values

    def update_iec104_value(self, coa, ioa, value) -> bool:
        """
        Updates all GridValues associated with the given COA and IOA to the given value.
        Returns whether any value has actually been changed.
        @param coa: The COA
        @param ioa: The IOA
        @param value: The value to apply
        @return:
        """
        changed = False
        data_point = self.get_data_point(coa, ioa)
        if data_point is not None:
            type_id = self._dp_get_type_id(data_point)
            cot = self._dp_get_cot(data_point)
            if type_id == 45:
                value = bool(value)
            grid_values = self.get_grid_values_for_data_point(data_point)
            with self.lock:
                for grid_value in grid_values:
                    old_value = grid_value.get_value()
                    self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
                    # Add measurements to state estimators
                    timeout = False
                    if cot == 1:
                        timeout = True
                    measurement = PowerOwlMeasurement(grid_value, value, timeout, self.virtual_time.time())
                    for estimator_info in self._state_estimators.values():
                        estimator_info["estimator"].measure(measurement)
        return changed

    def handle_measurement(self, update: messages.ProcessInfoMonitoring):
        changed = False
        try:
            # Update PowerGrid
            coa = update.coa
            for ioa, value in update.val_map.items():
                changed |= self.update_iec104_value(coa, ioa, value)

            # update dp timings
            ioas = list(update.val_map.keys())
            if self._dp_timing_enabled and update.cot == 3:
                arrival = time.time()
                with self._dp_timing_lock:
                    for ioa in ioas:
                        dp_id = f"{coa}.{ioa}"
                        self._dp_timings.setdefault(dp_id, {"state": "on-time", "last-seen": 0})[
                            "last_arrival"] = arrival

            # export measurements
            if self._export_measurements:
                ref_time: WattsonTime = self._export_measurements_wattson_time_callback()
                val_map = copy.deepcopy(update.val_map)
                self._export_measurements_queue.put({
                    "coa": coa,
                    "value_map": val_map,
                    "sim-time": ref_time.sim_clock_time(),
                    "clock-time": ref_time.wall_clock_time()
                })
        except Exception as e:
            self.logger.error(e)
            raise e
        return changed

    def run_power_flow(self) -> bool:
        self._power_flow_required.clear()
        if self.power_grid_model.simulate():
            self._last_power_flow = time.time()
            self.logger.debug("Power Flow done")
            if self._on_power_flow_completed is not None:
                self._on_power_flow_completed(True)
            return True
        else:
            self._power_flow_required.set()
            self.logger.error("Power Flow failed")
            if self._on_power_flow_completed is not None:
                self._on_power_flow_completed(False)
            return False

    def get_pandapower_net(self):
        """
        Returns a pandapower representation of the power grid
        @return:
        """
        return self.power_grid_model.to_external()

    def start_measurement_export(self, folder: Path, ts_callback: Callable[[], WattsonTime]):
        self.logger.info(f"Enabling measurement exports to {folder.__str__()}")
        folder.mkdir(exist_ok=True, parents=True)
        self._export_measurements = True
        self._export_measurements_root = folder
        self._export_measurements_wattson_time_callback = ts_callback
        self._measurement_thread = threading.Thread(target=self._measurement_export_loop)
        self._measurement_thread.start()

    def start_periodic_export(self, folder: Path, interval: float, wattson_time: WattsonTime):
        self.logger.info(f"Enabling grid exports every {interval} seconds to {folder.__str__()}")
        folder.mkdir(exist_ok=True, parents=True)
        self._export_folder = folder
        self._export_interval = interval
        self._export_wattson_time = wattson_time
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
                               wattson_time: Optional[WattsonTime] = None,
                               export_folder: Optional[Path] = None) -> bool:
        with self._estimator_lock:
            if estimation_mode is None:
                estimation_mode = self._state_estimation_mode
            if measurement_decay is None:
                measurement_decay = self._state_estimation_decay
            if export and wattson_time is None:
                raise AttributeError("wattson_time_callback required when export is True")
            if export and export_folder is None:
                raise AttributeError("export_folder required when export is True")
            if name in self._state_estimators:
                self.logger.warning(f"StateEstimator '{name}' is already running")
                return False

            self._state_estimators[name] = {
                "estimator": PandaPowerStateEstimator(
                    power_grid_model=self.power_grid_model,
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
                "wattson_time": wattson_time,
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
                    file: pathlib.Path = self._export_measurements_root.joinpath(f"measurements-{coa}.jsonl")
                    file.touch(0o755, exist_ok=True)
                    handle = open(file=file, mode="w")
                    self._export_measurements_files[coa] = handle
                handle.write(json.dumps(update) + "\n")
                handle.flush()
            except queue.Empty:
                pass

    def _estimation_export_loop(self):
        # TODO: Abstract from pandapower
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
                wattson_time = estimator_info["wattson_time"]
                folder = estimator_info["folder"]
                self.export(e.power_net, folder, wattson_time)

    def _periodic_export_loop(self):
        last_export = 0
        while not self._stop_export_event.is_set():
            try:
                diff = time.time() - last_export
                if diff > self._export_interval:
                    with self.lock:
                        self.export(self.power_grid_model.to_primitive_dict(), self._export_folder, self._export_wattson_time)
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

    def _estimation_done(self, name: str, success: bool, used_algorithm: str | None):
        self.logger.debug(f"SE for {name} with {used_algorithm} algorithm: {success=}")
        if success and self._state_estimators.get(name, {}).get("export", False):
            self.logger.debug(f"Scheduling export for {name}")
            self._export_se_queue.put(name)

    def _estimation_started(self, name: str):
        self.logger.debug(f"Started Estimation {name}")

    def export(self, data, folder: Path, ref_time: WattsonTime):
        ref_time_wall = ref_time.file_name(time_type=WattsonTimeType.WALL, with_milliseconds=True)
        ref_time_sim = ref_time.file_name(time_type=WattsonTimeType.SIM, with_milliseconds=True)

        filename = f"WALL-{ref_time_wall}__SIM-{ref_time_sim}.powerowl.p"
        file = folder.joinpath(filename)
        pickle.dump(data, open(file.absolute().__str__(), "wb"))
        file.chmod(0o755)

    def export_grid_values(self, output_file: Path, value_filter: list, ref_time: WattsonTime, file_format: str = "csv", append: bool = True):
        ref_time_wall = ref_time.file_name(time_type=WattsonTimeType.WALL, with_milliseconds=True)
        ref_time_sim = ref_time.file_name(time_type=WattsonTimeType.SIM, with_milliseconds=True)
        if file_format not in ["csv", "json", "jsonl"]:
            raise AttributeError(f"Invalid file format: {file_format}")
        data = []
        field_names = []
        # Potentially load existing data
        if output_file.exists():
            if not append:
                raise FileExistsError(f"Export file already exists. Either delete it or set append=True")
            if file_format == "csv":
                data = self.load_grid_value_csv(output_file)
            elif file_format == "json":
                with output_file.open("r", encoding="utf8") as f:
                    data = json.load(f)
            elif file_format == "jsonl":
                # Nothing to do here, just append the next line
                pass
        # Add date time to data
        grid_values = {
            "wall-date-time": ref_time_wall,
            "sim-date-time": ref_time_sim,
            "wall-timestamp": ref_time.wall_clock_time(),
            "sim-timestamp": ref_time.sim_clock_time()
        }
        field_names.extend(grid_values.keys())
        # Add grid values to data
        for grid_value in self.power_grid_model.get_grid_values():
            identifier = grid_value.get_identifier()
            for filter_expression in value_filter:
                if fnmatch.fnmatch(identifier, filter_expression):
                    grid_values[identifier] = grid_value.value
                    field_names.append(identifier)
                    break
        # Write to file
        if file_format in ["csv", "json"]:
            data.append(grid_values)
            # Write file as a whole
            with output_file.open("w", encoding="utf8") as f:
                if file_format == "csv":
                    for line in data[:-1]:
                        for key in line.keys():
                            if key not in field_names:
                                field_names.append(key)
                    csv_writer = csv.DictWriter(f, fieldnames=field_names)
                    csv_writer.writeheader()
                    csv_writer.writerows(data)
                elif file_format == "json":
                    json.dump(data, f, indent=4)
        elif file_format == "jsonl":
            # Append line to file
            with output_file.open("a", encoding="utf8") as f:
                f.write(json.dumps(grid_values))
                f.write("\n")

    @staticmethod
    def load_grid_value_csv(file_name: Path) -> list:
        data = []
        with file_name.open("r", encoding="utf8") as f:
            reader = csv.DictReader(f)
            line: dict
            for line in reader:
                data.append(line)
        return data

    def get_dataframe_row(self, net, ref_time: datetime.datetime, table_cols: Optional[List[str]] = None):
        raise DeprecationWarning("get_dataframe_row has been deprecated")
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

    # def on_element_change(self, callback: Callable[[str, int, str, DataPointValue, DataPointValue], None]):
    def on_element_change(self, callback: Callable[[GridValue, Any, Any], None]):
        self._callbacks["on_element_change"].append(callback)

    def on_element_update(self, callback: Callable[[GridValue, Any, Any], None]):
        self._callbacks["on_element_update"].append(callback)

    def _notify_on_element_update(self, grid_value: GridValue, old_value: Any, new_value: Any):
        for callback in self._callbacks["on_element_update"]:
            callback(grid_value, old_value, new_value)
        if new_value != old_value:
            self._notify_on_element_change(grid_value, old_value=old_value, new_value=new_value)

    def _notify_on_element_change(self, grid_value: GridValue, old_value: Any, new_value: Any):
        for callback in self._callbacks["on_element_change"]:
            callback(grid_value, old_value, new_value)

    def update_grid_value(self, element_type: str, element_index: int, value_context: GridValueContext, value_name: str, value):
        grid_element = self.power_grid_model.get_element(element_type=element_type, element_id=element_index)
        grid_value = grid_element.get(key=value_name, context=value_context)
        old_value = grid_value.get_value()
        if old_value != value:
            grid_value.set_value(value)
            self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
            self._power_flow_required.set()
            return True
        self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
        return False

    def get_measurement(self, coa, ioa, value, arrival_time: Optional[float] = None) -> Optional[PowerOwlMeasurement]:
        dp = self.get_data_point(coa, ioa)
        if dp is not None:
            grid_values = self.get_grid_values_for_data_point(dp)
            cot = self._dp_get_cot(dp)
            if len(grid_values) != 1:
                if len(grid_values) == 0:
                    self.logger.error(f"No grid value for {coa=}.{ioa=} found")
                    return None
                self.logger.error("Multiple measurements exist")
            grid_value = grid_values[0]
            timeout = False
            arrival_time = arrival_time if arrival_time is not None else self.virtual_time.time()
            if cot == 1:
                timeout = True
            measurement = PowerOwlMeasurement(grid_value, value, timeout, arrival_time)
            return measurement
        return None

    def get_grid_elements(self, element_type: str) -> List[GridElement]:
        """
        Returns the pandapower elements of the specified type
        :param element_type: The elements to return, e.g., "sgen"
        :return: A dataframe with said elements in case the pp_type is valid. Otherwise, None
        """
        return self.power_grid_model.get_elements_by_type(element_type=element_type)

    def get_datapoints_for_element(self, grid_element: GridElement) -> list:
        """
        Given a single power grid element, compiles a list of data points that are associated with
        this element.
        :param grid_element: The grid element
        :return: A list of datapoints
        """
        result = []
        identifiers = set()
        for device, dps in self.data_points.items():
            for dp in dps:
                control_provider = self._get_power_grid_provider(dp, "targets")
                monitoring_provider = self._get_power_grid_provider(dp, "sources")
                for p in [control_provider, monitoring_provider]:
                    if p is None:
                        continue
                    if grid_element.get_identifier() == p["grid_element"]:
                        if dp["identifier"] not in identifiers:
                            result.append(dp)
                            identifiers.add(dp["identifier"])
                        break
        return result

    def get_data_points_for_grid_value(self, grid_value: GridValue) -> List[dict]:
        """
        Given a single power grid value, compiles a list of data points that this value matches to
        @param grid_value: The grid value to search for
        @return: A list of data points
        """
        element_data_points = self.get_datapoints_for_element(grid_element=grid_value.get_grid_element())
        data_points = []
        for dp in element_data_points:
                control_provider = self._get_power_grid_provider(dp, "targets")
                monitoring_provider = self._get_power_grid_provider(dp, "sources")
                for p in [control_provider, monitoring_provider]:
                    if p is None:
                        continue
                    if p["context"] == grid_value.value_context.value and p["attribute"] == grid_value.name:
                        data_points.append(dp)
        return data_points

    @staticmethod
    def _get_power_grid_provider(dp, direction: str):
        if direction not in dp["providers"]:
            return None
        candidates = []
        for provider in dp["providers"][direction]:
            if provider["provider_type"] == "POWER_GRID":
                candidates.append(provider["provider_data"])
        if len(candidates) != 1:
            return None
        return candidates[0]

    @staticmethod
    def get_104_info(data_point):
        if data_point["protocol"] == "60870-5-104":
            return data_point["protocol_data"]
        return None

    def _dp_get_type_id(self, dp):
        info = self.get_104_info(dp)
        if info is not None:
            return info.get("type_id")
        return None

    def _dp_get_cot(self, dp):
        info = self.get_104_info(dp)
        if info is not None:
            return info.get("cot")
        return None
