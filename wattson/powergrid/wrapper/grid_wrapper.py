import copy
import csv
import fnmatch
import json
import logging
import pathlib
import pickle
import queue
import threading
import time
from pathlib import Path
from typing import Callable, Optional, List, Dict, Any

from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.simulators.pandapower import PandaPowerGridModel

from wattson.hosts.ccx.protocols import CCXProtocol
from wattson.powergrid.wrapper.panda_power_state_estimator import PandaPowerStateEstimator
from wattson.powergrid.wrapper.power_owl_measurement import PowerOwlMeasurement
from wattson.powergrid.wrapper.state_estimator import StateEstimator
from wattson.time import WattsonTime, WattsonTimeType
from wattson.util import get_logger
from wattson.util.events.queue_event import QueueEvent
from wattson.util.time.virtual_time import VirtualTime


class GridWrapper:
    """This class wraps a PowerGridModel and provides additional functionalities for analysing the grid and associated measurements."""
    def __init__(self, power_grid_model: PowerGridModel, datapoints: dict, logger: Optional[logging.Logger], **kwargs):
        self.power_grid_model = power_grid_model
        # The virtual time is used to synchronize actions with arbitrary time-speeds
        self.virtual_time = kwargs.get("virtual_time", VirtualTime.get_instance())

        if kwargs.get("disable_measurement_sources", False):
            self._clear_source_values(GridValueContext.MEASUREMENT)
        if kwargs.get("disable_estimation_sources", False):
            self._clear_source_values(GridValueContext.ESTIMATION)

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

        self._on_estimation_started_callbacks: List[Callable[[str], None]] = []
        self._on_estimation_done_callbacks: List[Callable[[str, bool, Optional[str]], None]] = []

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
        self._init_on_set_callbacks()

    def _init_on_set_callbacks(self):
        for grid_element in self.power_grid_model.get_elements():
            for _, grid_value in grid_element.get_grid_values():
                grid_value.add_on_set_callback(self._on_grid_value_set)

    def _clear_source_values(self, context: GridValueContext):
        for grid_element in self.power_grid_model.get_elements():
            for _, grid_value in grid_element.get_grid_values(context=context):
                grid_value.source = None

    def get_data_point(self, identifier: str):
        if identifier in self._dp_cache:
            return self._dp_cache[identifier]
        for host, data_points in self.data_points.items():
            for data_point in data_points:
                if data_point["identifier"] == identifier:
                    self._dp_cache[identifier] = data_point
                    return data_point
        return None

    def get_iec61850mms_data_point(self, server_id, mms_path):
        key = f"IEC61850MMS.{server_id}.{mms_path}"
        if key in self._dp_cache:
            return self._dp_cache[key]
        for host, dps in self.data_points.items():
            for dp in dps:
                if dp.get("protocol") == CCXProtocol.IEC61850_MMS:
                    if dp["protocol_data"]["server"] == server_id and dp["protocol_data"]["mms_path"] == mms_path:
                        self._dp_cache[key] = dp
                        return dp
        return None

    def get_iec104_data_point(self, coa, ioa):
        key = f"IEC104.{coa}.{ioa}"
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

    def get_first_grid_value_for_data_point(self, data_point) -> Optional[GridValue]:
        grid_values = self.get_grid_values_for_data_point(data_point=data_point)
        if len(grid_values) == 0:
            return None
        return grid_values[0]

    def update_iec61850mms_value(self, server, mms_path, value) -> bool:
        """
        Updates all GridValues associated with the given Server and MMS Path to the given value.
        Returns whether any value has actually been changed.

        Args:
            server:
                The server ID
            mms_path:
                The attributes MMS path
            value:
                The value

        Returns:
            bool: Whether any value has actually been changed.
        """
        changed = False
        data_point = self.get_iec61850mms_data_point(server, mms_path)
        if data_point is not None:
            grid_values = self.get_grid_values_for_data_point(data_point)
            trigger_options = [MMSTriggerOptions(trigger_option) for trigger_option in data_point["protocol_data"]["trigger_options"]]
            timeout = MMSTriggerOptions.INTEGRITY in trigger_options
            changed = self._update_grid_values(grid_values, value, timeout)
        else:
            self.logger.warning(f"No data point found for MMS attribute {mms_path}")
        return changed

    def update_iec104_value(self, coa, ioa, value) -> bool:
        """
        Updates all GridValues associated with the given COA and IOA to the given value.
        Returns whether any value has actually been changed.

        Args:
            coa:
                The COA
            ioa:
                The IOA
            value:
                The value to apply
        """
        changed = False
        data_point = self.get_iec104_data_point(coa, ioa)
        if data_point is not None:
            type_id = self.dp_get_type_id(data_point)
            cot = self.dp_get_cot(data_point)
            if type_id == 45:
                value = bool(value)
            grid_values = self.get_grid_values_for_data_point(data_point)
            timeout = cot == 1
            changed = self._update_grid_values(grid_values, value, timeout)

        return changed

    def _update_grid_values(self, grid_values, value, timeout: bool) -> bool:
        changed = False
        with self.lock:
            for grid_value in grid_values:
                old_value = grid_value.get_value()
                # TODO: Check if "set_value" is ok here or if we (again) need raw_set_value
                changed |= grid_value.set_value(value)

                # self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
                # Add measurements to state estimators
                measurement = PowerOwlMeasurement(grid_value, value, timeout, self.virtual_time.time())
                for estimator_info in self._state_estimators.values():
                    estimator_info["estimator"].measure(measurement)
        return changed

    def handle_data_point_update(self, data_point_identifier: str, value: Any, protocol_name: str, protocol_data: Optional[Dict] = None):
        changed = False

        try:
            data_point = self.get_data_point(data_point_identifier)
            export_data = {}
            export_host = data_point.get("host", "catchAll")
            if data_point is None:
                self.logger.warning(f"Unknown data point {data_point_identifier} - cannot handle update")
                return False
            if protocol_name == CCXProtocol.IEC104:
                if protocol_data is not None:
                    coa = protocol_data["coa"]
                    ioa = protocol_data["ioa"]
                else:
                    iec104info = self.get_104_info(data_point)
                    if iec104info is None:
                        self.logger.error(f"Invalid IEC104 data point: {data_point_identifier} - cannot handle update")
                        return False
                    coa = iec104info["coa"]
                    ioa = iec104info["ioa"]
                export_data["ioa"] = ioa
                export_data["coa"] = coa
                if export_host == "catchAll":
                    export_data = f"{coa}"
                changed = self.update_iec104_value(coa, ioa, value)
            elif protocol_name == CCXProtocol.IEC61850_MMS:
                mms_info = protocol_data
                if mms_info is None:
                    mms_info = data_point["protocol_data"]

                try:
                    server = mms_info["server"]
                    mms_path = mms_info["mms_path"]
                except KeyError as e:
                    self.logger.error(f"Invalid MMS data point - requires server and mms_path ({e=}")
                    return False

                export_data["server"] = server
                export_data["mms_path"] = mms_path
                if export_host == "catchAll":
                    export_data = f"{server}"
                changed = self.update_iec61850mms_value(server, mms_path, value)
            else:
                self.logger.error(f"Unknown protocol {protocol_name} - cannot handle update")
                return False

            # Update data point timings
            if self._dp_timing_enabled:
                # NIY
                pass

            # Export measurements
            if self._export_measurements:
                ref_time: WattsonTime = self._export_measurements_wattson_time_callback()
                self._export_measurements_queue.put(
                    {
                        "identifier": data_point_identifier,
                        "host": export_host,
                        "value": value,
                        "data": export_data,
                        "sim-time": ref_time.sim_clock_time(),
                        "clock-time": ref_time.wall_clock_time()
                    }
                )
        except Exception as e:
            self.logger.error(f"{e=}")
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
        """Returns a pandapower representation of the power grid"""
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

    def add_on_estimation_started_callback(self, callback: Callable[[str], None]):
        self._on_estimation_started_callbacks.append(callback)

    def add_on_estimation_done_callback(self, callback: Callable[[str, bool], None]):
        self._on_estimation_done_callbacks.append(callback)

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

            if not isinstance(self.power_grid_model, PandaPowerGridModel):
                self.logger.error("Cannot start state estimator. Only PandaPowerGridModel supported.")
            else:
                self._state_estimators[name] = {
                    "estimator": PandaPowerStateEstimator(
                        power_grid_model=self.power_grid_model,
                        # update_required=threading.Event(),
                        update_required=QueueEvent(max_queue_interval_s=10),
                        pnet_lock=self._power_flow_lock,
                        estimation_done_callback=self._estimation_done,
                        estimation_started_callback=self._estimation_started,
                        estimation_mode=estimation_mode,
                        measurement_decay=measurement_decay,
                        virtual_time=self.virtual_time,
                        fault_detection=False,
                        name=name,
                    ),
                    "name": name,
                    "export": export,
                    "wattson_time": wattson_time,
                    "folder": export_folder
                }
                self.logger.info(f"Starting StateEstimator {name} with mode {estimation_mode}")
                self._state_estimators[name]["estimator"].add_on_element_update_callback(self._notify_on_element_update)
                self._state_estimators[name]["estimator"].start()

        if export:
            self.logger.info(f"Starting StateEstimation Export for {name} to {export_folder}")
            if self._export_se_thread is None:
                self._export_se_thread = threading.Thread(target=self._estimation_export_loop)
                self._export_se_thread.start()
        return True

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
                host = update["host"]
                handle = self._export_measurements_files.get(host)
                if handle is None:
                    self.logger.debug(f"Creating measurement export file for Host {host}")
                    file: pathlib.Path = self._export_measurements_root.joinpath(f"measurements-{host}.jsonl")
                    file.touch(0o755, exist_ok=True)
                    handle = open(file=file, mode="w")
                    self._export_measurements_files[host] = handle
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
                self.export(e.power_grid_model.to_primitive_dict(), folder, wattson_time)

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

    def _estimation_done(self, name: str, success: bool, used_algorithm: Optional[str] = None):
        self.logger.debug(f"SE for {name}: {success=} - {used_algorithm=}")
        try:
            for callback in self._on_estimation_done_callbacks:
                callback(name, success, used_algorithm)
        except Exception as e:
            self.logger.error(f"Failed to handle estimation done callback: {e=}")
        if success and self._state_estimators.get(name, {}).get("export", False):
            self.logger.debug(f"Scheduling export for {name}")
            self._export_se_queue.put(name)

    def _estimation_started(self, name: str):
        self.logger.debug(f"Started Estimation {name}")
        try:
            for callback in self._on_estimation_started_callbacks:
                callback(name)
        except Exception as e:
            self.logger.error(f"Failed to handle estimation started callback: {e=}")

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

    def _on_grid_value_set(self, grid_value: GridValue, old_value: Any, new_value: Any):
        self._notify_on_element_update(grid_value, old_value=old_value, new_value=new_value)

    def update_grid_value(self, grid_value: GridValue, value):
        old_value = grid_value.get_value()
        changed = grid_value.set_value(value)
        if changed:
            # self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
            self._power_flow_required.set()
            return True
        # self._notify_on_element_update(grid_value, old_value=old_value, new_value=value)
        return False

    def update_grid_value_by_identifiers(self, element_type: str, element_index: int, value_context: GridValueContext, value_name: str, value):
        grid_element = self.power_grid_model.get_element(element_type=element_type, element_id=element_index)
        grid_value = grid_element.get(key=value_name, context=value_context)
        return self.update_grid_value(grid_value, value)

    def get_measurement(self, coa, ioa, value, arrival_time: Optional[float] = None) -> Optional[PowerOwlMeasurement]:
        dp = self.get_iec104_data_point(coa, ioa)
        if dp is not None:
            grid_values = self.get_grid_values_for_data_point(dp)
            cot = self.dp_get_cot(dp)
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

        Args:
            element_type (str):
                The elements to return, e.g., "sgen"

        Returns:
            List[GridElement]: A dataframe with said elements in case the pp_type is valid. Otherwise, None
        """
        return self.power_grid_model.get_elements_by_type(element_type=element_type)

    def get_datapoints_for_element(self, grid_element: GridElement) -> list:
        """
        Given a single power grid element, compiles a list of data points that are associated with this element.

        Args:
            grid_element (GridElement):
                The grid element

        Returns:
            list: A list of datapoints
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

        Args:
            grid_value (GridValue):
                The grid value to search for

        Returns:
            List[dict]: A list of data points
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
        if data_point.get("protocol") == CCXProtocol.IEC104:
            return data_point["protocol_data"]
        return None

    def dp_get_type_id(self, dp):
        info = self.get_104_info(dp)
        if info is not None:
            return info.get("type_id")
        return None

    def dp_get_cot(self, dp):
        info = self.get_104_info(dp)
        if info is not None:
            return info.get("cot")
        return None
