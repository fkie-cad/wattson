import datetime
import importlib
import queue
import threading
import time
from pathlib import Path
from threading import Thread
from typing import Any, Union
from typing import Optional

from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.simulators.pandapower import PandaPowerGridModel

import wattson.util.misc
from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.apps.script_controller.interface import SoloScript, Script, TimedScript
from wattson.apps.script_controller.runner import TimedRunner
from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.hosts.ccx.app_gateway import AppGatewayClient
from wattson.iec104.interface.types import COT
from wattson.powergrid.wrapper.grid_wrapper import GridWrapper
from wattson.powergrid.remote.remote_power_grid_model import RemotePowerGridModel
from wattson.time import WattsonTime
from wattson.util import get_logger

CONTROLLER_WAIT_FOR_MTU_PER_RTU_S = 2


class ScriptControllerApp:
    """
    This app interfaces with the MTU and provides various interaction possibilities for logic scripts.
    - Measurement Export (Exports all arrived measurements per RTU as JSON-Line file (.jsonl) - State Estimation (TODO) - Delay monitoring (Logs
    and exports detected delayed measurements) - TimedScripts (supplied by config) - Script queues actions that should be executed at certain
    points in time - SoloScripts (supplied by config) - Script that is started along with the controller, but runs individually

    """
    def __init__(self,
                 host_ip: str,
                 mtu_ip: str,
                 wattson_client_query_socket: str,
                 wattson_client_publish_socket: str,
                 datapoints: dict,
                 scripts: Optional[list] = None, scenario_path: Optional[Path] = None,
                 export_config: Optional[dict] = None,
                 statistics_config: Optional[dict] = None):

        self.host_ip = host_ip
        self.mtu_ip = mtu_ip
        self.wattson_client_query_socket = wattson_client_query_socket
        self.wattson_client_publish_socket = wattson_client_publish_socket
        self.datapoints = datapoints

        self.wattson_time: Optional[WattsonTime] = None
        self.power_grid_model: Optional[PandaPowerGridModel] = None
        self.remote_grid_model: Optional[RemotePowerGridModel] = None
        self.grid_wrapper: Optional[GridWrapper] = None

        self.scripts = scripts
        self.script_cls = []
        self.scenario_path = Path(scenario_path)
        self.logger = get_logger("ScriptController")

        self.export_enabled = False
        self.export_measurements = False
        self.monitor_dp_delays = False

        self.se_config = []
        if export_config is not None:
            self.export_interval = export_config.get("interval", 1)
            self.export_enabled = export_config.get("power_grid", False)
            self.export_root = Path(export_config.get("folder", ".")).joinpath("controller-export")
            self.export_root.mkdir(exist_ok=True, parents=True)

            self.export_grid_root = self.export_root.joinpath("power-grid")

            self.logger.info(f"Export root is {self.export_root}")

            self.export_measurements = export_config.get("measurements", False)
            self.export_measurement_root = self.export_root.joinpath("measurements")

            self.monitor_dp_delays = export_config.get("delay_monitoring", False)
            self.monitor_dp_delay_timeout = export_config.get("delay_monitoring_timeout", 15)

            self.se_config = export_config.get("state_estimations", [])
            for i, config in enumerate(self.se_config):
                export = config.get("export", False)
                folder = config.get("folder")
                name = config.get("name", f"SE_{i}")
                mode = config.get("mode", "default")
                decay = config.get("decay", 12)

                if export:
                    if folder is None:
                        folder = self.export_root.joinpath(name)
                    else:
                        folder = Path(folder)
                    folder.mkdir(exist_ok=True, parents=True)

                config["mode"] = mode
                config["folder"] = folder
                config["name"] = name
                config["export"] = export
                config["decay"] = decay

        self._threads = []

        self._worker_threads = []
        self._work_terminate = threading.Event()
        self._max_worker_threads = 20
        self._worker_thread_poll = 0.2
        self._work_queue = queue.Queue()

        self._reply_lock = threading.Lock()
        self._reply_events = {}
        self._dp_cache = {}
        self._lock = threading.Lock()

        self.wattson_client = WattsonClient(query_server_socket_string=self.wattson_client_query_socket,
                                            publish_server_socket_string=self.wattson_client_publish_socket,
                                            client_name="ScriptController")

        self.ccx_client = AppGatewayClient(
            ip_address=mtu_ip,
            client_name="script-controller"
        )

        self.statistics = StatisticClient(
            ip=statistics_config.get("server"),
            host="script_controller",
            logger=self.logger,
        )
        self.statistics.start()
        self.logger.info("Finished Script Controller init")

    def _default_scripts(self):
        return []

    def _load_scripts(self):
        if self.scripts is None or len(self.scripts) == 0:
            self.scripts = self._default_scripts()
        self.script_cls = []
        for script in self.scripts:
            if type(script) == str:
                parts = str(script).split(".")
                if len(parts) < 2:
                    raise ValueError(f"Script {script} cannot be loaded")
                module_name = ".".join(parts[:-1])
                cls_name = parts[-1]
                try:
                    module = importlib.import_module(module_name)
                    ocls = getattr(module, cls_name)
                    self.script_cls.append(ocls(self))
                except Exception as e:
                    raise RuntimeError(f"Cannot Import Script {script}: {e}")
            elif type(script) == dict:
                script_config = None
                if "config" in script:
                    script_config = script["config"]
                enabled = script.get("enabled", True)
                if not enabled:
                    continue

                if "file" in script and "cls" in script:
                    file = script["file"]
                    cls_name = script["cls"]
                    config_path = self.scenario_path.joinpath(file)
                    if not config_path.exists():
                        self.logger.warning(f"Script Path {file} does not exist relative to scenario")
                        self.logger.warning(f"Full path: {config_path}")
                    else:
                        spec = importlib.util.spec_from_file_location("custom.script", config_path)
                        custom_script = importlib.util.module_from_spec(spec)
                        self.logger.info(f"Loading script instance from {file}")
                        spec.loader.exec_module(custom_script)
                        ocls = getattr(custom_script, cls_name)
                        script_o = ocls(self, script_config)
                        if isinstance(script_o, Script):
                            self.script_cls.append(script_o)
                        else:
                            self.logger.error("Object is not a valid Controller Script!")
                elif "script" in script:
                    cls_path = script["script"]
                    ocls = wattson.util.misc.dynamic_load_class(cls_path)
                    script_o = ocls(self, script_config)
                    if isinstance(script_o, Script):
                        self.script_cls.append(script_o)
                    else:
                        self.logger.error("Object is not a valid Controller Script!")

            elif isinstance(script, Script):
                self.script_cls.append(script)

    def start(self):
        # Wattson Client
        self.logger.info("Connecting Wattson Client...")
        self.wattson_client.start()
        self.wattson_client.require_connection()
        self.wattson_client.register()
        self.logger.info("Connected Wattson Client")

        # Wattson Time
        self.wattson_time = self.wattson_client.get_wattson_time(enable_synchronization=True)

        # Power Grid Models: Remote and local fork
        self.logger.info("Loading RemotePowerGridModel")
        self.remote_grid_model = RemotePowerGridModel(wattson_client=self.wattson_client)
        self.logger.info("Creating local PowerGridModel")
        self.power_grid_model = PandaPowerGridModel()
        self.power_grid_model.from_primitive_dict(self.remote_grid_model.to_primitive_dict())

        self.grid_wrapper = GridWrapper(self.power_grid_model, self.datapoints, self.logger.getChild("GridModel"), wattson_time=self.wattson_time)

        # MTU connection
        self.logger.info(f"Connecting to MTU")
        self.ccx_client.start()

        # Worker threads
        for i in range(self._max_worker_threads):
            t = Thread(target=self._async_worker)
            t.start()

        if self.export_enabled:
            self.grid_wrapper.start_periodic_export(self.export_grid_root, self.export_interval, lambda: self.wattson_time)
        if self.export_measurements:
            self.grid_wrapper.start_measurement_export(self.export_measurement_root, lambda: self.wattson_time)
        if self.monitor_dp_delays:
            self.grid_wrapper.start_timing_monitoring(self.monitor_dp_delay_timeout)

        for config in self.se_config:
            if config["export"]:
                self.grid_wrapper.start_state_estimation(
                    name=config["name"],
                    estimation_mode=config["mode"],
                    measurement_decay=config["decay"],
                    export=True,
                    export_folder=config["folder"],
                    wattson_time=self.wattson_time
                )

        # Wait for MTU
        rtu_count = len(self.datapoints.keys())
        wait_time = CONTROLLER_WAIT_FOR_MTU_PER_RTU_S * rtu_count
        self.logger.info(f"Waiting for MTU (at most {int(wait_time)}s)")
        self.wattson_client.event_wait(event_name="MTU_READY", timeout=wait_time)

        # Start Scripts (Applications)
        self._load_scripts()
        self._threads = []

        runner = TimedRunner(self)
        self._threads.append(runner)

        for script in self.script_cls:
            if isinstance(script, SoloScript):
                script_name = f"{script.__module__}.{script.__class__.__name__}"
                self.logger.info(f"Starting Script {script_name}")
                t = Thread(target=script.run)
                self._threads.append(t)
            elif isinstance(script, TimedScript):
                runner.add_script(script)
            else:
                self.logger.warning(f"Script {script} will not be started")
        for t in self._threads:
            t.start()
        for t in self._threads:
            t.join()

    def stop(self):
        # Stop SoloScripts
        for script in self.script_cls:
            if isinstance(script, SoloScript):
                script.stop()
        # Stop all workers
        self._work_terminate.set()
        # Join all threads
        for t in self._worker_threads:
            if t.is_alive():
                t.join()
        for t in self._threads:
            if t.is_alive():
                t.join()
        self.ccx_client.stop()
        self.grid_wrapper.stop_periodic_export()
        self.grid_wrapper.stop_measurement_export()
        self.grid_wrapper.stop_timing_monitoring()
        self.grid_wrapper.stop_estimations()

    def _on_cmd_reply(self, update: "messages.Confirmation", orig_msg: "messages.IECMsg"):
        pass

    def _on_cmd_update(self, update: "messages.Confirmation", orig_msg: "messages.IECMsg" = None):
        with self._reply_lock:
            if update.reference_nr in self._reply_events:
                if update.result["status"] in [
                    ConfirmationStatus.POSITIVE_CONFIRMATION.value,
                    ConfirmationStatus.FAIL.value
                ]:
                    e = self._reply_events[update.reference_nr]
                    e.set()
                    del self._reply_events[update.reference_nr]

    def _on_update(self, update: "messages.IECMsg", orig_msg: "messages.IECMsg" = None):
        pass

    def _on_dp_update(self, update: "messages.ProcessInfoMonitoring", ref_arg=False):
        return self.grid_wrapper.handle_measurement(update)

    def _get_dp(self, coa: int, ioa: int) -> Optional[dict]:
        for _, dps in self.datapoints.items():
            for dp in dps:
                if dp["protocol"] == "60870-5-104":
                    if dp["protocol_data"]["coa"] == coa and dp["protocol_data"]["ioa"] == ioa:
                        return dp
        return None

    def _async_worker(self):
        while not self._work_terminate.is_set():
            try:
                task: dict = self._work_queue.get(True, self._worker_thread_poll)
            except queue.Empty:
                continue
            self.logger.info(f"Got task {task}")
            if task["task"] == "get_dp":
                coa = task["coa"]
                ioa = task["ioa"]
                self.get_data_point(coa, ioa, timeout=None, block=True)
            elif task["task"] == "set_dp":
                coa = task["coa"]
                ioa = task["ioa"]
                value = task["value"]
                type_id = task["type_id"]
                cot = task["cot"]
                self.set_data_point(coa, ioa, value, type_id, cot, block=True)
            elif task["task"] == "shutdown":
                self.wattson_client.request_shutdown()
        if not self._work_queue.empty():
            remain = self._work_queue.qsize()
            self.logger.warning(f"Worker Queue is not empty ({remain} tasks remaining)")
        else:
            self.logger.info(f"Worker Queue completed - terminating")

    def send_control(self, message: "messages.ProcessInfoControl", callback=None) -> Optional[str]:
        if message.reference_nr == UNSET_REFERENCE_NR:
            ref_id = self.mtu_client.next_reference_nr
            message.reference_nr = ref_id
        else:
            ref_id = message.reference_nr
        self.mtu_client.send_cmd(message, special_callback=callback)
        return ref_id

    def set_grid_value(self, element_type: str, element_index: int, value_context: GridValueContext, value_name: str, value: Any) -> bool:
        """
        Updates the grid value of the specified element

        Args:
            element_type (str):
                The element type, e.g., bus
            element_index (int):
                The element index
            value_context (GridValueContext):
                The grid value context
            value_name (str):
                The grid value's name
            value (Any):
                The value to set

        Returns:
            bool: Whether the value has been set
        """
        grid_value = self.get_grid_value(element_type, element_index, value_context, value_name)
        if grid_value is None:
            return False
        grid_value.set_value(value=value)
        return True

    def get_grid_value(self, element_type: str, element_index: int, value_context: GridValueContext, value_name: str) -> Optional[GridValue]:
        """
        Returns the GridValue object of the specified element

        Args:
            element_type (str):
                The element type, e.g., bus
            element_index (int):
                The element index
            value_context (GridValueContext):
                The grid value context
            value_name (str):
                The grid value's name

        Returns:
            Optional[GridValue]: The GridValue object or None, if the value cannot be found
        """
        try:
            grid_element = self.remote_grid_model.get_element(element_type=element_type, element_id=element_index)
            grid_value = grid_element.get(key=value_name, context=value_context)
            return grid_value
        except Exception as e:
            self.logger.error(f"GridValue not found: {e=}")
            return None

    def get_grid_value_value(self, element_type: str, element_index: int, value_context: GridValueContext, value_name: str) -> Any:
        """
        Returns the GridValue's value of the specified element

        Args:
            element_type (str):
                The element type, e.g., bus
            element_index (int):
                The element index
            value_context (GridValueContext):
                The grid value context
            value_name (str):
                The grid value's name

        Returns:
            Any: The GridValue's value object or None, if the value cannot be found
        """
        grid_value = self.get_grid_value(element_type, element_index, value_context, value_name)
        if grid_value is None:
            return None
        return grid_value.get_value()

    def set_pandapower(self, table: str, index: int, column: str, value, log_worthy: bool = True) -> bool:
        grid_value = self.grid_wrapper.power_grid_model.get_grid_value_by_identifier(f"{table}.{index}.{column}")
        changed = grid_value.set_value(value)
        return changed

    def get_pandapower(self, table: str, index: int, column: str):
        if isinstance(self.grid_wrapper.power_grid_model, PandaPowerGridModel):
            return self.grid_wrapper.power_grid_model.get_grid_value_by_identifier(f"{table}.{index}.{column}")
        else:
            return None

    def set_data_point(self,
                       coa: int,
                       ioa: int,
                       value: Any,
                       type_id: Optional[int] = None,
                       cot: Optional[int] = None,
                       timeout: Optional[int] = 0,
                       block: bool = True) -> Optional[Union[bool, str]]:
        """
        Sets the addressed data point value via IEC104

        Args:
            coa (int):
                The common address of the data point
            ioa (int):
                The information object address of the data point
            value (Any):
                The value to set
            type_id (Optional[int], optional):
                The IEC104 type ID of the value
                (Default value = None)
            cot (Optional[int], optional):
                The cause of transmission to use
                (Default value = None)
            timeout (Optional[int], optional):
                A timeout in seconds to wait when blocking. Use None to wait indefinitely.
                (Default value = 0)
            block (bool, optional):
                Whether to block until a response is received
                (Default value = True)
        """
        # Get TypeID and COT from data point list if set to None
        if not block:
            self._work_queue.put({
                "task": "set_dp",
                "coa": coa,
                "ioa": ioa,
                "value": value,
                "type_id": type_id,
                "cot": cot
            })
            return

        dp = self._get_dp(coa, ioa)
        if type_id is None:
            if dp is None:
                return None
            type_id = dp["protocol_data"]["type_id"]
        if cot is None:
            if dp is None:
                cot = COT.ACTIVATION
            else:
                cot = dp["protocol_data"]["cot"]

        message = messages.ProcessInfoControl(
            coa=coa,
            type_ID=type_id,
            cot=cot,
            val_map={ioa: value}
        )
        if timeout == 0:
            return self.send_control(message)

        # Setup callback to wait for return of ack / nack
        ref_id = self.mtu_client.next_reference_nr
        message.reference_nr = ref_id
        wait = threading.Event()
        with self._reply_lock:
            self._reply_events[ref_id] = wait
        self.send_control(message)

        success = wait.wait(timeout=timeout)
        return success

    def get_data_point(self, coa: int, ioa: int, timeout: Optional[float] = 5, block: bool = True) -> Any:
        """
        Receive a data point's value via a dedicated IEC104 request.

        Args:
            coa (int):
                The common address of the data point
            ioa (int):
                The information object address of the data point
            timeout (Optional[float], optional):
                A timeout when blocking for a response
                (Default value = 5)
            block (bool, optional):
                Whether to block for the response
                (Default value = True)

        Returns:
            Any: If blocking and the timeout is not exceeded, the received value is returned. Otherwise, None is returned.
        """
        if not block:
            self._work_queue.put({
                "task": "get_dp",
                "coa": coa,
                "ioa": ioa
            })
            return

        message = messages.ReadDatapoint(coa, ioa)
        wait = threading.Event()
        value = None

        now = time.time()

        def callback(update: messages.IECMsg, orig_msg):
            if not isinstance(update, messages.ProcessInfoMonitoring):
                self.logger.warning("Callback does not contain a read value")
                wait.set()
                return True

            nonlocal value
            if update.coa == coa and ioa in update.val_map:
                value = update.val_map[ioa]
            then = time.time()
            if value is not None:
                self.statistics.log(event_name=f"{coa}.{ioa}", event_class="mtu_read", value=then - now)
            else:
                self.statistics.log(event_name=f"{coa}.{ioa}", event_class="mtu_read", value=False)
            wait.set()
            return True

        # self.statistics.log(event_name=f"{coa}.{ioa}", event_class="mtu_read", value="request")
        self.mtu_client.send_cmd(message, special_callback=callback)
        if timeout is None:
            return None
        wait.wait(timeout)

        return value

    def get_simulated_start_time(self):
        """Returns the start timestamp of the simulation in simulated time"""
        return self.wattson_time.sim_start_time()

    def get_current_simulated_time(self):
        """Returns the current timestamp of the simulation in simulated time"""
        return self.wattson_time.sim_clock_time()

    def set_simulated_start_time(self, start_time: Union[datetime.datetime, float], speed: float):
        sim_clock_reference = start_time if isinstance(start_time, float) else start_time.timestamp()
        new_wattson_time = WattsonTime(self.wattson_time.wall_start_time(), sim_clock_reference=sim_clock_reference, speed=speed)
        return self.wattson_client.set_wattson_time(new_wattson_time)
