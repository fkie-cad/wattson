import traceback
import importlib.util
import pickle
import queue
import threading
import time
from typing import Union

import numpy as np
import pandapower
import yaml

from wattson.powergrid.messages import PPQuery
from wattson.powergrid.server.coord_logic_interface import CoordinatorLogicInterface
import wattson.util.noise
from wattson.util.random import Random

IEC104Value = Union[bool, int, float]


convert_column_dict = {
    f"u_{direction}_bus": f"vm_{direction}_pu" for direction, table
    in zip(["to", "from", "lv", "hv"], ["line"] * 2 + ["trafo"] * 2)
}


def convert_table_names(table: str) -> str:
    if not table.startswith("res_"):
        table = "prev_" + table
    return table


class SimulatorThread(threading.Thread):
    def __init__(self, simulation_manager, net: pandapower.pandapowerNet, ups: float, logger, config: dict,
                 default_soc: float = 0):
        super().__init__(daemon=True)
        self.config = config
        self.simulation_manager = simulation_manager
        self.net: pandapower.pandapowerNet = net
        self.ups = ups
        self.net_lock = threading.Lock()
        self.copy_lock = threading.Lock()
        self._update_queries: queue.Queue = queue.Queue()
        self._stopped = threading.Event()
        self._iterate = threading.Event()
        self.initialized = threading.Event()
        self.logger = logger
        self._last_calculation: Union[None, int] = None
        self._default_soc = default_soc
        self._min_calculation_spacing = 0.1  # seconds
        self._old_net = None
        self._manual_req = threading.Event()
        self.pre_sim_noise = self.config.get("pre_sim_noise", None)
        self._no_noise_grid = None
        self._pre_sim_noise_rules = []
        self._coord_logic_scripts = self._instantiate_logic_scripts()
        self._generate_pre_sim_noise_rules()
        for script in self._coord_logic_scripts:
            self.logger.info(f"Running setup from {script.__class__}")
            script.setup(self.net)

    def request_run(self):
        self._manual_req.set()
        self._iterate.set()

    def run(self) -> None:
        """
        Continuously simulate the pandapower net, with a given maximum UPS (updates per second)
        :param net:
        :param ups:
        :return:
        """
        if "target_p_mw" not in self.net["storage"]:
            self.net["storage"]["target_p_mw"] = self.net["storage"]["p_mw"]
        self._run_simulation()
        self.initialized.set()
        self.logger.info("Starting simulation thread")
        while not self._stopped.is_set():
            try:
                ti = time.time()
                waiting_time = 1 / self.ups
                if self._last_calculation is None or ti - self._last_calculation >= self._min_calculation_spacing:
                    with self.net_lock:
                        if self._iteration_required():
                            self._manual_req.set()
                            while self._manual_req.is_set():
                                self._manual_req.clear()
                                for script in self._coord_logic_scripts:
                                    self.logger.debug(f"Running pre-sim hooks of {script.__class__}")
                                    script.pre_sim_transform(self.net)
                                self._apply_pre_sim_noise()
                                self._run_simulation()
                                self._undo_pre_sim_noise()
                                for script in self._coord_logic_scripts:
                                    self.logger.debug(f"Running post-sim hooks of {script.__class__}")
                                    manual_req = script.post_sim_transform(self.net)
                                    if manual_req:
                                        self._manual_req.set()
                                duration = time.time() - ti
                                self.logger.debug(f"Power Flow Computation executed in {duration}s")
                                self.simulation_manager.coordinator.statistics.log(
                                    "completed", event_class="powerflow.duration", value=duration
                                )
                                self._notify_post_sim_element_update()
                        else:
                            self.logger.debug("Skipping power flow computation due to no changes")
                    waiting_time -= (time.time() - ti)
                self._iterate.wait(max(0.0, waiting_time))
                self._iterate.clear()
            except Exception as e:
                self.logger.error(e)
                tb = traceback.format_exc()
                self.logger.error(tb)

    def _notify_post_sim_element_update(self):
        for element in self.simulation_manager.coordinator.subscribed_elements:
            table, index, column = element.split(".")
            index = int(index)
            if self._old_net is not None:
                old_value = self._old_net[table].at[index, column]
            else:
                old_value = None
            new_value = self.net[table].at[index, column]
            self.simulation_manager.notify_element_updated(table, column, index, old_value, new_value)

    def _run_simulation(self):
        try:
            self._old_net = self.net.deepcopy()
            pandapower.runpp(self.net, calculate_voltage_angles=True)
            self._last_calculation = time.time()
            with self.copy_lock:
                for table in pandapower.toolbox.pp_elements(other_elements=False):
                    self.net[convert_table_names(table)] = self.net[table].copy(True)
            self.simulation_manager.notify_powerflow()
        except pandapower.powerflow.LoadflowNotConverged as e:
            # unable to simulate the net, just skip
            self.logger.error("Error simulating the net:")
            self.logger.error(f"{e}")
        except Exception as e:
            self.logger.error(e)
            tb = traceback.format_exc()
            self.logger.error(tb)

    def _apply_updates(self) -> bool:
        """
        Apply all updates that are stored. This assumes that the updates were
        sanitized before.
        :return:
        """
        updates_applied = False
        MAX_UPDATES = 999
        try:
            for i in range(MAX_UPDATES):
                update_query = self._update_queries.get_nowait()
                old_value = self.net[update_query.table].at[update_query.index, update_query.column]
                if False and update_query.table == "trafo" and update_query.column == "tap_pos":
                # this is the value of a "regulating step command state" (RCS)
                # 1 - decrements; 2 - increment
                # XXX there should be some better solution, but this works for now...
                    if np.isnan(old_value):
                        old_value = self.net[update_query.table].at[update_query.index, "tap_neutral"]
                    new_value = old_value
                    if update_query.value == 1:
                        new_value -= 1
                    elif update_query.value == 2:
                        new_value += 1
                    else:
                        # (invalid)
                        pass
                    self.logger.debug(f"changing tap_pos at index {update_query.index} from {old_value} to {new_value}")
                else:
                    new_value = update_query.value
                    self.logger.debug("Processing update query: " + str(update_query))

                prevent_default = False
                for script in self._coord_logic_scripts:
                    self.logger.debug(f"Running write_transform hooks of {script.__class__}")
                    prevent_default |= script.write_transform(
                        self.net, update_query.table, update_query.index, update_query.column, new_value)

                if not prevent_default:
                    self.net[update_query.table][update_query.column][update_query.index] = new_value

                self.simulation_manager.notify_element_updated(
                    update_query.table, update_query.column, update_query.index, old_value, new_value
                )
                updates_applied = True
        except queue.Empty:
            pass
        return updates_applied

    def add_update_query(self, query: PPQuery) -> None:
        self._update_queries.put(query)

    def get_powernet(self) -> pandapower.pandapowerNet:
        with self.net_lock:
            grid_copy = self.net.deepcopy()
        return grid_copy

    def answer_retrieval_query(self, query: PPQuery) -> IEC104Value:
        """
        Return response for a query. Blocks until the power flow has been
        computed at least once. Returns value in the result of the last power
        flow computation. So, there cannot be any inconsistency after writing
        values.
        :param query: PPQuery for a specific value
        :return: value of the requested entry
        """
        self.initialized.wait()
        # fix old column names
        column = convert_column_dict.get(query.column, query.column)
        table = convert_table_names(query.table)
        with self.net_lock:
            res = self.net[table][column][query.index] #type:ignore
            if np.isnan(res):
                self.logger.debug(f"Rewriting NaN to 0 for {query}")
                res = 0
        return res

    def net_to_yaml(self):
        return yaml.dump(self.net, default_flow_style=False, sort_keys=False)

    def net_to_pickle_file(self, filename):
        pickle.dump(self.net, open(filename, "wb"))

    def stop(self):
        self._stopped.set()
        self._iterate.set()

    def _calculate_storages(self):
        """
        Checks whether active storages are in the grid that change their state of charge.
        If this is the case, the state of charge is calculated and updated in the grid
        (BEFORE THE POWERFLOW COMPUTATION!)
        """
        # p_mw decides whether the battery is charging (positive) or discharging (negative)
        if self._last_calculation is None:
            return False

        soc_changed = False

        if self.net["storage"].size > 0:
            time_passed_s = time.time() - self._last_calculation
            for index, row in self.net["storage"].iterrows():
                if row["in_service"]:
                    soc_changed |= self._handle_storage(index, time_passed_s)

        return soc_changed

    def _handle_storage(self, storage_index, time_passed_s):
        # TODO: FIX POTENTIAL PRESENCE OF REACTIVE POWER?
        current_power_mw = self.net["res_storage"].at[storage_index, "p_mw"]
        if current_power_mw == 0:
            # Battery inactive
            return False

        charge_mwh = self._default_soc
        soc_percent = self.net["storage"].at[storage_index, "soc_percent"]
        max_e_mwh = self.net["storage"].at[storage_index, "max_e_mwh"]
        min_e_mwh = self.net["storage"].at[storage_index, "min_e_mwh"]
        if not np.isnan(soc_percent):
            charge_mwh = soc_percent * 0.01 * max_e_mwh
        time_passed_h = time_passed_s / 3600
        charge_change_mwh = current_power_mw * time_passed_h
        new_charge_mwh = charge_mwh + charge_change_mwh
        new_soc_percent = (new_charge_mwh / max_e_mwh) * 100
        if new_charge_mwh <= min_e_mwh:
            # Battery empty or below specified lower bound - stop discharging
            self.net["storage"]["p_mw"][storage_index]= 0
            self.net["storage"]["soc_percent"][storage_index] = max(0, new_soc_percent)
            self.logger.info(f"Battery {storage_index} drained")
        elif new_charge_mwh >= max_e_mwh:
            # Battery fully charged - stop charging
            self.logger.info(f"Battery {storage_index} fully charged")
            self.net["storage"]["p_mw"][storage_index]= 0
            self.net["storage"]["soc_percent"][storage_index] = 100
        else:
            # Keep current mode active
            self.net["storage"]["soc_percent"][storage_index] = new_soc_percent
        return True

    def _iteration_required(self):
        req = self._manual_req.is_set()
        self._manual_req.clear()
        req |= self._apply_updates()
        req |= self._calculate_storages()
        return req

    def _apply_pre_sim_noise(self):
        if self.pre_sim_noise is None:
            return
        if self._last_calculation is None:
            self.logger.info("No simulation done - cannot apply pre-sim noise yet")
            return
        for table in pandapower.toolbox.pp_elements(other_elements=False, res_elements=False):
            self.net[f"no_noise_{table}"] = self.net[table].copy(True)
        # Apply noise rules
        for rule in self._pre_sim_noise_rules:
            table, row, measure, value = rule
            cur_val = self.net[table].at[row, measure]
            scale = None
            if isinstance(value, str):
                if len(value) > 0 and value[-1] == "%":
                    percentage = float(value[:-1])
                    scale = (percentage / 100) * cur_val
                else:
                    self.logger.error(f"Invalid noise value {value} in rule {rule}")
            elif isinstance(value, float) or isinstance(value, int):
                scale = float(value)
            if scale is not None:
                namespace = f"{table}.{row}.{measure}"
                # self.net[table].at[row, measure] = np.random.normal(cur_val, abs(scale))
                self.net[table].at[row, measure] = Random.normal(cur_val, abs(scale), ns=namespace)

    def _undo_pre_sim_noise(self):
        if self.pre_sim_noise is None:
            return
        for table in pandapower.toolbox.pp_elements(other_elements=False, res_elements=False):
            self.net[table] = self.net[f"no_noise_{table}"]
            del self.net[f"no_noise_{table}"]

    def _generate_pre_sim_noise_rules(self):
        if self.pre_sim_noise is None:
            return
        with self.net_lock:
            for measure in ["p_mw", "q_mvar"]:
                if measure not in self.pre_sim_noise:
                    continue
                for table in pandapower.toolbox.pp_elements(other_elements=False, res_elements=False):
                    if measure in self.net[table].columns:
                        value_rules = {}
                        if isinstance(self.pre_sim_noise[measure], dict):
                            if table in self.pre_sim_noise[measure]:
                                if isinstance(self.pre_sim_noise[measure][table], list):
                                    value_rules = self.pre_sim_noise[measure][table]
                                else:
                                    value = self.pre_sim_noise[measure][table]
                                    value_rules = {row: value for row in list(self.net[table].index)}
                        else:
                            value = self.pre_sim_noise[measure]
                            value_rules = {row: value for row in list(self.net[table].index)}
                        for row, value in value_rules.items():
                            if isinstance(value, str) and len(value) > 0 and value[-1] != "%":
                                value = wattson.util.noise.translate_value(value, measure)

                            self._pre_sim_noise_rules.append((
                                table, row, measure, value
                            ))
        for i, rule in enumerate(self._pre_sim_noise_rules):
            self.logger.info(f"Pre-Sim-Noise-Rule #{i}: {rule}")

    def _instantiate_logic_scripts(self):
        scripts = self.config.get("coord_logic_scripts", [])
        script_objects = []
        for i,s in enumerate(scripts):
            script_path = self.simulation_manager.coordinator.scenario_path.joinpath(s["file"])
            if not script_path.exists():
                self.logger.warning(f"Coord Script Path {s['file']} does not exist relative to scenario")
                self.logger.warning(f"Full path: {script_path}")
            else:
                spec = importlib.util.spec_from_file_location(f"grid.sim.script{i}", script_path)
                grid_configurator = importlib.util.module_from_spec(spec)
                self.logger.info(f"Initializing logic script from {script_path}")
                spec.loader.exec_module(grid_configurator)
                cls = getattr(grid_configurator, s["class"])
                args_for_class = s["args"] if "args" in s else {}
                o = cls(self.simulation_manager.coordinator, args_for_class)
                if not isinstance(o, CoordinatorLogicInterface):
                    self.logger.warning(f"{script_path}.{s['class']} does not implement CoordinatorLogicInterface")
                else:
                    script_objects.append(o)
        return sorted(script_objects, key=lambda x: x.get_priority())
