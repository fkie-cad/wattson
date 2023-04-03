import datetime
import importlib
import queue
import threading
from pathlib import Path
from typing import Optional, Any, Union

import pandapower as pp
from threading import Thread, Lock
import time

import pytz

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.apps.script_controller.interface import SoloScript, Script, TimedScript
from wattson.apps.interface.clients import CombiClient
from wattson.apps.interface.util import messages, ConfirmationStatus, UNSET_REFERENCE_NR
from wattson.apps.script_controller.runner import TimedRunner
from wattson.iec104.common import MTU_READY_EVENT
from wattson.iec104.interface.types import COT
from typing import Optional

from wattson.powergrid import CoordinationClient
from wattson.powergrid.model.grid_model import GridModel
from wattson.util import get_logger
from wattson.util.powernet import sanitize_power_net

CONTROLLER_WAIT_FOR_MTU_PER_RTU_S = 2


class ScriptControllerApp:
    def __init__(self, host_ip: str, mtu_ip: str, coordinator_ip: str, datapoints: dict, grid: pp.pandapowerNet,
                 scripts: Optional[list] = None, scenario_path: Optional[Path] = None,
                 export_config: Optional[dict] = None,
                 statistics_config: Optional[dict] = None):

        self.host_ip = host_ip
        self.mtu_ip = mtu_ip
        self.coordinator_ip = coordinator_ip
        self.datapoints = datapoints
        self.grid = grid
        self.grid = sanitize_power_net(self.grid)
        self.sim_start_time = time.time()
        self._simulated_time = None
        self.scripts = scripts
        self.script_cls = []
        self.scenario_path = Path(scenario_path)
        self.logger = get_logger("ScriptController", "ScriptController")

        self.grid_model = GridModel(self.grid, self.datapoints, self.logger.getChild("GridModel"))
        self.export_enabled = False
        self.export_measurements = False
        self.monitor_dp_delays = False
        self.se_config = []
        if export_config is not None:
            scenario_name = self.scenario_path.name
            start_time = datetime.datetime.now().astimezone().strftime('%Y-%m-%d-%H-%M-%S')
            self.export_interval = export_config.get("interval", 1)
            self.export_enabled = export_config.get("enabled", False)
            self.export_root = Path(export_config.get("folder", "controller-export"))
            # self.export_root = self.export_root.joinpath(scenario_name).joinpath(start_time)
            self.export_root.mkdir(exist_ok=True, parents=True)

            self.export_measurements = export_config.get("measurements", False)
            self.export_measurement_root = self.export_root.joinpath("measurements")
            self.export_measurement_root.mkdir(exist_ok=True)

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

        self._lock = threading.Lock()
        self.coord_client = CoordinationClient(coordinator_ip, node_id="ScriptController")
        self.mtu_client = CombiClient(
            "script_controller",
            mtu_ip=mtu_ip,
            on_cmd_reply=self._on_cmd_reply,
            on_cmd_update=self._on_cmd_update,
            on_dp_update=self._on_dp_update,
            on_update=self._on_update,
            store_dp_update=True
        )

        self.statistics = StatisticClient(
            ip=statistics_config.get("server"),
            host="script_controller",
            logger=self.logger,
        )
        self.statistics.start()

    def _default_scripts(self):
        from wattson.apps.script_controller.scripts.cli_script import CLIScript
        return [
            CLIScript(self)
        ]

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
                    script_o = ocls(self)
                    if isinstance(script_o, Script):
                        self.script_cls.append(script_o)
                    else:
                        self.logger.error("Object is not a valid Controller Script!")
            elif isinstance(script, Script):
                self.script_cls.append(script)

    def start(self):
        # Worker threads
        for i in range(self._max_worker_threads):
            t = Thread(target=self._async_worker)
            t.start()

        # Subscribe
        self.coord_client.set_on_simulated_time_update(self._update_simulated_time)
        self.coord_client.start()
        self.mtu_client.start()

        self.coord_client.wait_for_start_event()
        rtu_count = len(self.datapoints.keys())
        wait_time = CONTROLLER_WAIT_FOR_MTU_PER_RTU_S * rtu_count
        self.logger.info(f"Waiting for MTU (at most {int(wait_time)}s)")
        if self.coord_client.wait_for_event(MTU_READY_EVENT, wait_time):
            self.logger.info("MTU reports to be ready")
        else:
            self.logger.warning(f"MTU did not report to be ready after {int(wait_time)}s - continuing anyway")

        if self.export_enabled:
            self.grid_model.start_periodic_export(self.export_root,
                                                  self.export_interval,
                                                  self.get_current_simulated_time)
        if self.export_measurements:
            self.grid_model.start_measurement_export(self.export_measurement_root, self.get_current_simulated_time)

        if self.monitor_dp_delays:
            self.grid_model.start_timing_monitoring(self.monitor_dp_delay_timeout)

        for config in self.se_config:
            if config["export"]:
                self.grid_model.start_state_estimation(
                    name=config["name"],
                    estimation_mode=config["mode"],
                    measurement_decay=config["decay"],
                    export=True,
                    export_folder=config["folder"],
                    ts_callback=self.get_current_simulated_time
                )

        # Start Scripts
        self._load_scripts()
        self._threads = []

        runner = TimedRunner(self)
        self._threads.append(runner)

        for script in self.script_cls:
            if isinstance(script, SoloScript):
                script_name = f"{script.__module__}.{script}"
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
        self._work_terminate.set()
        for t in self._worker_threads:
            t.join()
        self.mtu_client.stop()
        self.grid_model.stop_periodic_export()
        self.grid_model.stop_measurement_export()
        self.grid_model.stop_timing_monitoring()
        self.grid_model.stop_estimations()

    def _on_cmd_reply(self, update: messages.Confirmation, orig_msg: messages.IECMsg):
        pass

    def _on_cmd_update(self, update: messages.Confirmation, orig_msg: messages.IECMsg = None):
        with self._reply_lock:
            if update.reference_nr in self._reply_events:
                if update.result["status"] in [
                    ConfirmationStatus.POSITIVE_CONFIRMATION.value,
                    ConfirmationStatus.FAIL.value
                ]:
                    e = self._reply_events[update.reference_nr]
                    e.set()
                    del self._reply_events[update.reference_nr]

    def _on_update(self, update: messages.IECMsg, orig_msg: messages.IECMsg = None):
        pass

    def _on_dp_update(self, update: messages.ProcessInfoMonitoring, ref_arg=False):
        self.grid_model.handle_measurement(update)

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
                self.get_dp(coa, ioa, timeout=None, block=True)
            elif task["task"] == "set_dp":
                coa = task["coa"]
                ioa = task["ioa"]
                value = task["value"]
                type_id = task["type_id"]
                cot = task["cot"]
                self.set_dp(coa, ioa, value, type_id, cot, block=True)
            elif task["task"] == "shutdown":
                self.coord_client.request_shutdown()
        if not self._work_queue.empty():
            remain = self._work_queue.qsize()
            self.logger.warning(f"Worker Queue is not empty ({remain} tasks remaining)")
        else:
            self.logger.info(f"Worker Queue completed - terminating")

    def send_control(self, message: messages.ProcessInfoControl, callback=None) -> Optional[str]:
        if message.reference_nr == UNSET_REFERENCE_NR:
            ref_id = self.mtu_client.next_reference_nr
            message.reference_nr = ref_id
        else:
            ref_id = message.reference_nr
        self.mtu_client.send_cmd(message, special_callback=callback)
        return ref_id

    def set_pandapower(self, table: str, index: int, column: str, value, log_worthy: bool = True) -> bool:
        error_msg = self.coord_client.update_value(table, column, index, value, log_worthy=log_worthy)
        return True

    def get_pandapower(self, table: str, index: int, column: str):
        return self.coord_client.retrieve_value(table, column, index)

    def set_dp(self, coa: int, ioa: int, value, type_id: Optional[int] = None, cot: Optional[int] = None,
               timeout: Optional[int] = 0,
               block: bool = True) -> Optional[Union[bool, str]]:
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

    def get_dp(self, coa: int, ioa: int, timeout: Optional[float] = 5, block: bool = True) -> Any:
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
                self.statistics.log(event_name=f"{coa}.{ioa}", event_class="mtu_read", value=then-now)
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

    def _update_simulated_time(self):
        s_time, speed = self.coord_client.get_simulated_time()
        if s_time is not None:
            self._simulated_time = s_time
            self._simulated_speed = speed
        else:
            self.logger.error("Could not load simulated time from coordinator")

    def get_simulated_start_time(self):
        if self._simulated_time is None:
            self._update_simulated_time()
        return self._simulated_time, self._simulated_speed

    def get_current_simulated_time(self):
        passed = time.time() - self.sim_start_time
        t, s = self.get_simulated_start_time()
        passed *= s
        return t + passed

    def set_simulated_start_time(self, start_time: Union[datetime.datetime, float], speed: float):
        return self.coord_client.set_simulated_time(start=start_time, speed=speed)
