import threading
import time
import traceback
from pathlib import Path
from typing import Union, Type, Optional, Set, Dict, List, Any


import yaml

from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.simulators.pandapower import PandaPowerGridModel
from wattson.cosimulation.control.messages.wattson_async_group_response import WattsonAsyncGroupResponse
from wattson.cosimulation.control.messages.wattson_async_response import WattsonAsyncResponse

from wattson.cosimulation.exceptions import InvalidScenarioException, InvalidSimulationControlQueryException
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.datapoints.data_point_loader import DataPointLoader
from wattson.powergrid.noise.noise_manager import NoiseManager
from wattson.powergrid.profiles.profile_provider import  ProfileLoader
from wattson.powergrid.simulator.default_configurations.ccx_default_configuration import CCXDefaultConfiguration
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType
from wattson.powergrid.simulator.threads.export_thread import ExportThread
from wattson.services.wattson_python_service import WattsonPythonService
from wattson.cosimulation.simulators.physical.physical_simulator import PhysicalSimulator
from wattson.powergrid.simulator.default_configurations.mtu_default_configuration import \
    MtuDefaultConfiguration
from wattson.powergrid.simulator.default_configurations.rtu_default_configuration import \
    RtuDefaultConfiguration
from wattson.powergrid.simulator.default_configurations.vcc_default_configuration import VccDefaultConfiguration
from wattson.powergrid.simulator.messages.power_grid_control_query import PowerGridControlQuery
from wattson.powergrid.simulator.messages.power_grid_measurement_query import \
    PowerGridMeasurementQuery
from wattson.powergrid.simulator.messages.power_grid_measurement_response import \
    PowerGridMeasurementResponse
from wattson.powergrid.simulator.messages.power_grid_notification import PowerGridNotification
from wattson.powergrid.simulator.messages.power_grid_notification_topic import PowerGridNotificationTopic
from wattson.powergrid.simulator.messages.power_grid_query import PowerGridQuery
from wattson.powergrid.simulator.threads.simulation_thread import SimulationThread
from wattson.util.events.multi_event import MultiEvent
from wattson.util.events.queue_event import QueueEvent
from wattson.util.performance.timed_cache import TimedCache


class PowerGridSimulator(PhysicalSimulator):
    def __init__(self, grid_model_class: Type[PowerGridModel] = PandaPowerGridModel, **kwargs):
        super().__init__()
        self._use_ccx: bool = True  # Whether to use the (old) MTU or the new CCX implementation
        self._use_vcc: bool = True  # Whether to use the (old) GUI or the new VCC implementation
        self._common_addresses = {}
        self._mtu_entity_ids = []
        self._mtu_ids = []
        self._rtu_map = {}
        self._grid_model = grid_model_class()
        self._noise_manager = NoiseManager(power_grid_model=self._grid_model, logger=self.logger.getChild("NoiseManager"))
        self._grid_model.set_pre_sim_noise_callback(self._noise_manager.pre_sim_noise)
        self._grid_model.set_post_sim_noise_callback(self._noise_manager.post_sim_noise)
        self._grid_model.set_measurement_noise_callback(self._noise_manager.measurement_noise)
        self._grid_model.set_on_simulation_configuration_changed_callback(self.queue_iteration_required)

        max_queue_interval_s = kwargs.get("minimum_iteration_pause_seconds", 1)
        self._auto_interval_enable = kwargs.get("auto_iteration_pause_seconds", True)
        self._auto_interval_current = max_queue_interval_s
        self._auto_interval_last_start = None
        self._auto_interval_last_durations = []
        # Keep Sim-CPU at ~25%
        self._auto_interval_target_factor = 3
        self._auto_interval_history_size = 10
        self._auto_interval_adjustment_difference = 0.3
        self._auto_interval_minimum = max_queue_interval_s
        self._auto_interval_maximum = kwargs.get("auto_iteration_maximum_pause_seconds", 20)

        self._termination_requested = threading.Event()
        self._simulator_thread: Optional[SimulationThread] = None
        self._simulation_required = QueueEvent(max_queue_time_s=2, max_wait_time_s=0.05, max_queue_interval_s=max_queue_interval_s)
        self._required_sim_control_clients = set()
        self._profile_thread: Optional[ProfileLoader] = None
        self._export_thread: Optional[ExportThread] = None
        self._auto_export_enable: bool = kwargs.get("auto_export_enable", False)
        self._ready_event = MultiEvent()

        self._use_bulk_grid_value_updates = threading.Event()
        self._use_bulk_grid_value_updates.set()
        self._bulk_grid_value_updates = {}
        self._bulk_grid_value_lock = threading.Lock()
        self._flush_interval = 0.5
        self._flush_thread = threading.Thread(target=self._flush_bulk_grid_value_updates_loop)

        self._grid_representation_cache = TimedCache(cache_refresh_callback=self._get_grid_representation, cache_timeout_seconds=30)
        self._async_group_responses: Dict[str, WattsonAsyncGroupResponse] = {}

    def start(self):
        self._termination_requested.clear()
        self._simulation_required.set()

        simulator_noise_config = self.get_configuration_store().get_configuration("configuration", {}).get("power-grid", {}).get("noise", {})
        scenario_path = Path(self.get_configuration_store().get_configuration("scenario_path", "."))
        simulator_config = self.get_configuration_store().get_configuration("configuration", {}).get("power-grid", {}).get("simulator_config", {})

        pre_sim_noise = simulator_noise_config.get("pre_sim")
        post_sim_noise = simulator_noise_config.get("post_sim")
        measurement_noise = simulator_noise_config.get("measurement")
        self._noise_manager.set_static_noise(pre_sim_noise, post_sim_noise, measurement_noise)
        self.logger.info(f" Initializing Simulator")
        self._simulator_thread = SimulationThread(
            self._grid_model,
            iteration_required_event=self._simulation_required,
            on_iteration_start_callback=self._on_simulation_iteration_start,
            on_iteration_completed_callback=self._on_simulation_iteration_completed,
            on_value_update_callback=self._on_value_update,
            on_value_change_callback=self._on_value_change,
            on_value_state_change_callback=self._on_value_state_change,
            on_protection_equipment_triggered_callback=self._on_protection_equipment_triggered,
            on_protection_equipment_cleared_callback=self._on_protection_equipment_cleared,
            **simulator_config
        )
        self._simulator_thread.daemon = True

        profile_config = self._get_default_profile_config()
        profile_config.update(self.get_configuration_store().get_configuration("configuration", {}).get("power-grid", {}).get("profile-provider", {}))
        self.logger.info(f" Initializing ProfileLoader")
        self._profile_thread = ProfileLoader(
            grid_model=self._grid_model,
            apply_updates_callback=self._apply_profile_updates,
            scenario_path=scenario_path,
            **profile_config
        )
        self._profile_thread.daemon = True
        self.logger.info(f" Initializing Export")
        self._export_thread = ExportThread(
            export_path=self.get_working_directory().joinpath("power_grid_exports"),
            enable=self._auto_export_enable
        )
        self._export_thread.daemon = True
        self._ready_event.monitor(
            self._simulator_thread.ready_event,
            self._profile_thread.ready_event
        )
        self._profile_thread.start()
        self._export_thread.start(wait_for_event=self._simulator_thread.ready_event)
        self._simulator_thread.start(wait_for_event=self._profile_thread.ready_event)

    @staticmethod
    def _get_default_profile_config():
        return {
            "profiles": {
                "load": None,
                "sgen": None
            },
            "profile_path": None,
            "profile_dir": "default_profiles",
            "seed": 0,
            "noise": "1%",
            "interval": 5,
            "interpolate": "cubic",
            "wattson_time": {
                "mode": "standalone",
                "speed": None,
                "start_datetime": None
            },
            "stop": False
        }

    def get_noise_manager(self) -> NoiseManager:
        return self._noise_manager

    def enable_export(self):
        self._export_thread.enable_export()

    def disable_export(self):
        self._export_thread.disable_export()

    def queue_iteration_required(self):
        self._simulation_required.queue()

    def stop(self):
        self._termination_requested.set()
        if self._profile_thread is not None and self._profile_thread.is_alive():
            self._profile_thread.stop()
            self._profile_thread.join(10)
            if self._profile_thread.is_alive():
                self.logger.warning("ProfileThread refused to terminate...")
        if self._simulator_thread is not None and self._simulator_thread.is_alive():
            self._simulator_thread.stop()
            self._simulator_thread.join(10)
            if self._simulator_thread.is_alive():
                self.logger.warning("SimulatorThread refused to terminate.")
        if self._export_thread is not None and self._export_thread.is_alive():
            self._export_thread.stop()
            self._export_thread.join(10)
            if self._export_thread.is_alive():
                self.logger.warning("ExportThread refused to terminate.")

    @property
    def grid_model(self):
        return self._grid_model

    def load_from_grid_model(self, grid_model: PowerGridModel, data_points: dict):
        self._grid_model = grid_model

        self._noise_manager.set_power_grid_model(self._grid_model)
        self._grid_model.set_pre_sim_noise_callback(self._noise_manager.pre_sim_noise)
        self._grid_model.set_post_sim_noise_callback(self._noise_manager.post_sim_noise)
        self._grid_model.set_measurement_noise_callback(self._noise_manager.measurement_noise)

        self._configuration_store.register_configuration("datapoints", data_points)
        # power_grid_data = self._grid_model.to_primitive_dict()
        power_grid_data = self._grid_model.to_external()
        self._configuration_store.register_configuration("power_grid_model", power_grid_data)
        self._configure_network_nodes()
        self._fill_configuration_store()

    def load_scenario(self, scenario_path: Path):
        self.logger.info(f"Loading scenario")
        power_grid_file = scenario_path.joinpath("power-grid.yml")
        data_point_main_file = scenario_path.joinpath("data-points.yml")

        # Load power grid
        self.logger.info(f"  Loading power grid")
        if not power_grid_file.exists():
            raise InvalidScenarioException("Scenario requires power-grid.yml")
        with power_grid_file.open("r") as f:
            power_grid_data = yaml.load(f, Loader=yaml.CLoader)
        self._grid_model.from_primitive_dict(power_grid_data)
        self._configuration_store.register_configuration("power_grid_model", power_grid_data)

        # Load data points
        self.logger.info(f"  Loading data points")
        if not data_point_main_file.exists():
            self.logger.warning(f"No data point configuration found. Using empty data point configuration")
            data_points = {}
        else:
            data_point_loader = DataPointLoader(data_point_main_file_path=data_point_main_file)
            data_points = data_point_loader.get_data_points()
        self._configuration_store.register_configuration("datapoints", data_points)
        # Configuration
        self.logger.info(f"  Configuring network")
        self._configure_network_nodes()
        self._fill_configuration_store()

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        query_type = self.get_simulation_query_type(query)
        return issubclass(query_type, PowerGridQuery)

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        if not isinstance(query, PowerGridQuery):
            raise InvalidSimulationControlQueryException(
                f"PowerGridSimulator does not handle {query.__class__.__name__}"
            )
        if isinstance(query, PowerGridControlQuery):
            # Grid element configuration updates
            try:
                grid_element = self._grid_model.get_element_by_identifier(query.element_identifier)
                grid_value = grid_element.get_config(query.attribute_name)
                self.logger.info(f"Setting {grid_element.get_identifier()}.{grid_value.name} = {query.value}")
                grid_value.set_value(query.value)
                query.mark_as_handled()
            except KeyError:
                return WattsonResponse(False)
            return WattsonResponse(True)

        if isinstance(query, PowerGridMeasurementQuery):
            # Read Requests for measurements
            success = False
            value = None
            try:
                grid_element = self._grid_model.get_element_by_identifier(query.element_identifier)
                value = grid_element.get_measurement_value(query.attribute_name)
                success = True
                query.mark_as_handled()
            finally:
                return PowerGridMeasurementResponse(
                    query.element_identifier, query.attribute_name, value, successful=success
                )

        if isinstance(query, PowerGridQuery):
            if query.query_type == PowerGridQueryType.GET_GRID_VALUE or query.query_type == PowerGridQueryType.GET_GRID_VALUE_VALUE:
                query.mark_as_handled()
                grid_value_identifier = query.query_data.get("grid_value_identifier")
                try:
                    grid_value = self.grid_model.get_grid_value_by_identifier(grid_value_identifier)
                except Exception as e:
                    return WattsonResponse(successful=False, data={"error": repr(e)})
                if query.query_type == PowerGridQueryType.GET_GRID_VALUE_VALUE:
                    value_dict = {"value": grid_value.raw_get_value(override_freeze=True)}
                else:
                    value_dict = self._get_grid_value_remote_dict(grid_value)
                return WattsonResponse(successful=True, data=value_dict)

            if query.query_type == PowerGridQueryType.SET_GRID_VALUE or query.query_type == PowerGridQueryType.SET_GRID_VALUE_SIMPLE:
                query.mark_as_handled()
                grid_value_identifier = query.query_data.get("grid_value_identifier")
                value = query.query_data.get("value")
                override = query.query_data.get("override")
                try:
                    grid_value = self.grid_model.get_grid_value_by_identifier(grid_value_identifier)
                    grid_value.set_value(value, override_lock=override)
                except Exception as e:
                    return WattsonResponse(successful=False, data={"error": repr(e)})
                if query.query_type == PowerGridQueryType.SET_GRID_VALUE_SIMPLE:
                    value_dict = {"value": grid_value.raw_get_value(override_freeze=True)}
                else:
                    value_dict = self._get_grid_value_remote_dict(grid_value)
                return WattsonResponse(successful=True, data=value_dict)

            if query.query_type == PowerGridQueryType.GET_GRID_REPRESENTATION:
                query.mark_as_handled()
                if self._grid_representation_cache.is_up_to_date():
                    response_elements = self._grid_representation_cache.get_raw_content()
                    return WattsonResponse(successful=True, data={"grid_elements": response_elements})
                else:
                    # This query allows for a group response if multiple clients request the same data
                    response = self._get_async_group_response(PowerGridQueryType.GET_GRID_REPRESENTATION)

                    def resolve_power_grid_task(r, d):
                        _response_elements = self._grid_representation_cache.get_content()
                        return WattsonResponse(successful=True, data={"grid_elements": _response_elements})

                    if not response.is_resolvable():
                        response.resolve_with_task(resolve_power_grid_task)
                    return response

            if query.query_type == PowerGridQueryType.SET_GRID_VALUE_STATE:
                query.mark_as_handled()
                grid_value_identifier = query.query_data.get("grid_value_identifier")
                state_type = query.query_data.get("state_type")
                if state_type not in ["freeze", "lock"]:
                    return WattsonResponse(successful=False, data={"error": "Invalid state change requested"})
                state_target = query.query_data.get("state_target")
                frozen_value = query.query_data.get("freeze_value")
                try:
                    grid_value = self.grid_model.get_grid_value_by_identifier(grid_value_identifier)
                    if state_type == "freeze":
                        if state_target:
                            grid_value.freeze(frozen_value)
                        else:
                            grid_value.unfreeze()
                    elif state_type == "lock":
                        if state_target:
                            grid_value.lock()
                        else:
                            grid_value.unlock()
                except Exception as e:
                    return WattsonResponse(successful=False, data={"error": repr(e)})
                return WattsonResponse(successful=True, data=self._get_grid_value_remote_dict(grid_value))

        raise InvalidSimulationControlQueryException(f"PowerGridSimulator does not handle {query.__class__.__name__}")

    @staticmethod
    def _get_grid_value_remote_dict(grid_value: GridValue):
        value_dict = grid_value.to_dict()
        if isinstance(grid_value.value, GridElement):
            value_dict["value"] = {
                "__type": "GridElement",
                "element_type": grid_value.value.prefix,
                "element_index": grid_value.value.index
            }
        return value_dict

    def _get_grid_representation(self):
        response_elements = {}
        for e_type, elements in self.grid_model.elements.items():
            element: 'GridElement'
            for e_id, element in elements.items():
                attributes = {}
                for (value_name, grid_value) in element.get_grid_values():
                    grid_value_dict = self._get_grid_value_remote_dict(grid_value)
                    attributes.setdefault(grid_value.value_context.name, {})[grid_value.name] = grid_value_dict
                response_elements.setdefault(e_type, {})[e_id] = {
                    "attributes": attributes,
                    "data": element.get_data()
                }
        return response_elements

    def _get_async_group_response(self, group_key: str) -> WattsonAsyncGroupResponse:
        response = WattsonAsyncGroupResponse(group_key)
        if group_key not in self._async_group_responses:
            self._async_group_responses[group_key] = response
            response.block()
        else:
            cached_response = self._async_group_responses.get(group_key)
            if not cached_response.is_resolving:
                # Response is still waiting
                if cached_response.block():
                    response = cached_response
                else:
                    response.block()
            else:
                self._async_group_responses[group_key] = response
                response.block()
        return response

    def _configure_network_nodes(self):
        if self._configuration_store is None:
            raise InvalidScenarioException("ConfigurationStore is required")
        self._required_sim_control_clients = set()
        for node in self._network_emulator.get_nodes():
            if node.has_role("rtu"):
                if node.has_service("rtu"):
                    continue
                # Create RTU configuration
                coa = int(node.config["coa"])
                self._common_addresses[node.entity_id] = coa
                rtu_configuration = RtuDefaultConfiguration()
                rtu_configuration["use_syslog"] = self.get_configuration_store().get("configuration", {}).get("use_syslog", False)

                from wattson.hosts.rtu import RtuDeployment
                node.add_service(WattsonPythonService(RtuDeployment, rtu_configuration, node))
                self._required_sim_control_clients.add(node.entity_id)

            if node.has_role("mtu") or node.has_role("ccx"):
                if self._use_ccx:
                    if node.has_service("WattsonCCX"):
                        continue
                else:
                    if node.has_service("MTU/104-Client"):
                        continue
                self._mtu_entity_ids.append(node.entity_id)
                self._mtu_ids.append(node.id)

                # TODO: Remove this at this place!
                node.add_role("vcc")

                if self._use_ccx:
                    # Create CCX configuration
                    ccx_configuration = CCXDefaultConfiguration()
                    ccx_configuration["use_syslog"] = self.get_configuration_store().get_configuration("configuration", {}).get("use_syslog", False)
                    ccx_export_config = self.get_configuration_store().get_configuration("configuration", {}).get("ccx_export")

                    if isinstance(ccx_export_config, dict) and "enabled" in ccx_export_config and "file" in ccx_export_config:
                        file_path = ccx_export_config["file"]
                        if isinstance(file_path, Path):
                            file_path = str(file_path.absolute())
                        ccx_export_config = {
                            "enabled": ccx_export_config["enabled"],
                            "file": file_path
                        }
                        ccx_configuration["export"] = ccx_export_config

                    from wattson.hosts.ccx import CCXDeployment
                    node.add_service(WattsonPythonService(CCXDeployment, ccx_configuration, node))
                else:
                    # Create MTU configuration
                    mtu_configuration = MtuDefaultConfiguration()
                    from wattson.hosts.mtu import MtuDeployment
                    node.add_service(WattsonPythonService(MtuDeployment, mtu_configuration, node))

                rtu_map = node.config.get("rtu_map", {})
                self._rtu_map[node.entity_id] = {rtu_id: {"coa": rtu_id, "ip": rtu_ip.split("/")[0]} for rtu_id, rtu_ip in rtu_map.items()}
                self._required_sim_control_clients.add(node.entity_id)

            if node.has_role("vcc"):
                if node.has_service("Wattson VCC"):
                    continue
                # Create VCC configuration
                # TODO: Dynamically assign the MTU!
                try:
                    vcc_configuration = VccDefaultConfiguration()
                    if not self._use_vcc:
                        from wattson.apps.gui.deployment import GuiDeployment
                        node.add_service(WattsonPythonService(GuiDeployment, vcc_configuration, node))
                    else:
                        from wattson.apps.vcc.deployment import VccDeployment
                        vcc_configuration["export_config"] = self.get_configuration_store().get_configuration("configuration", {}).get("vcc_export", [])
                        node.add_service(WattsonPythonService(VccDeployment, vcc_configuration, node))
                except ImportError as e:
                    self.logger.warning(f"Cannot add VCC service as the module cannot be found ({e=})")

    def _fill_configuration_store(self):
        # MTU Options
        self._configuration_store.register_configuration("mtu_connect_delay", 0)
        self._configuration_store.register_configuration("do_general_interrogation", True)
        self._configuration_store.register_configuration("do_clock_sync", True)
        self._configuration_store.register_configuration("ccx_logic", [])
        # RTU Options
        self._configuration_store.register_configuration("rtu_logic", {
            "*": [{"class": "wattson.hosts.rtu.logic.spontaneous_logic.SpontaneousLogic"}]
        })
        self._configuration_store.register_configuration("do_periodic_updates", True)
        self._configuration_store.register_configuration("periodic_update_start", 0)
        self._configuration_store.register_configuration("periodic_update_ms", 10000)
        self._configuration_store.register_configuration("allowed_mtu_ips", True)
        # General
        self._configuration_store.register_configuration("coas", lambda node, store: self._common_addresses)
        self._configuration_store.register_configuration("mtus", lambda node, store: self._mtu_entity_ids)
        self._configuration_store.register_configuration("mtu_ids", lambda node, store: self._mtu_ids)
        self._configuration_store.register_configuration("rtu_map", lambda node, store: self._rtu_map)
        self._configuration_store.register_short_notation("!coa", "!coas.!entityid")
        self._configuration_store.register_configuration("power_grid", self._grid_model.to_primitive_dict())

    def get_simulation_control_clients(self) -> Set[str]:
        return self._required_sim_control_clients.copy()

    def _on_simulation_iteration_start(self):
        if self._auto_interval_enable:
            self._auto_interval_last_start = time.perf_counter()

    def _on_simulation_iteration_sync(self, successful: bool):
        pass

    def _on_simulation_iteration_completed(self, successful: bool):
        t = time.time()
        # Trigger measurement value updates
        for e_type, elements in self.grid_model.elements.items():
            for e_id, element in elements.items():
                for (value_name, grid_value) in element.get_grid_values(context=GridValueContext.MEASUREMENT):
                    if grid_value.simulator_context is not None:
                        grid_value.get_value()
        self._flush_bulk_grid_value_updates()
        self.send_notification(PowerGridNotification(
            notification_topic=PowerGridNotificationTopic.SIMULATION_STEP_DONE,
            notification_data={"success": successful}
        ))
        # Optionally export grid state
        if self._export_thread.is_enabled():
            export_dict = {}
            for e_type, elements in self.grid_model.elements.items():
                for e_id, element in elements.items():
                    for (value_name, grid_value) in element.get_grid_values(context=[GridValueContext.CONFIGURATION, GridValueContext.MEASUREMENT]):
                        value = grid_value.raw_get_value()
                        if hasattr(value, "item"):
                            value = value.item()
                        if isinstance(value, (float, bool, int, str)):
                            export_dict[grid_value.get_identifier()] = value
            self._export_thread.export(timestamp=t, values=export_dict)

        # Potentially adjust simulation interval
        if successful and self._auto_interval_enable and self._auto_interval_last_start is not None:
            duration = time.perf_counter() - self._auto_interval_last_start
            self._auto_interval_last_durations.append(duration)
            if len(self._auto_interval_last_durations) > self._auto_interval_history_size:
                self._auto_interval_last_durations = self._auto_interval_last_durations[1:]
            average_duration = sum(self._auto_interval_last_durations) / len(self._auto_interval_last_durations)
            calculated_interval = average_duration * self._auto_interval_target_factor
            applicable_interval = min(max(calculated_interval, self._auto_interval_minimum), self._auto_interval_maximum)
            adjustment_step = abs(self._auto_interval_current - applicable_interval)

            if adjustment_step > self._auto_interval_adjustment_difference:
                self.logger.debug(f"Adjusting simulation interval from {self._auto_interval_current}s to {applicable_interval}s")
                self._auto_interval_current = applicable_interval
                self._simulation_required.set_max_queue_interval_s(self._auto_interval_current)

    def _on_protection_equipment_triggered(self, grid_element: GridElement, protection_name: str):
        self.send_notification(PowerGridNotification(
            notification_topic=PowerGridNotificationTopic.PROTECTION_TRIGGERED,
            notification_data={
                "grid_element": grid_element.get_identifier(),
                "protection_name": protection_name
            }
        ))

    def _on_protection_equipment_cleared(self, grid_element: GridElement, protection_name: str):
        self.send_notification(PowerGridNotification(
            notification_topic=PowerGridNotificationTopic.PROTECTION_CLEARED,
            notification_data={
                "grid_element": grid_element.get_identifier(),
                "protection_name": protection_name
            }
        ))

    def _on_value_update(self, grid_value: GridValue, old_value: Any, new_value: Any):
        self._queue_grid_value_update_notification(grid_value=grid_value)

    def _on_value_change(self, grid_value: GridValue, old_value: Any, new_value: Any):
        for related in grid_value.get_related_grid_values():
            # Potentially trigger callbacks for related grid values
            related.get_value()
        if grid_value.value_context == GridValueContext.CONFIGURATION:
            # TODO: Evaluate how storage processing affects the performance.
            #  This should not lead to problems with auto interval enabled.
            #  Hence, we do not skip any configurations now
            """
            skipped_configurations = [("storage", "current_charge"), ("storage", "state_of_charge")]            
            parts = grid_value.get_identifier().split(".")
            for e_type, v_name in skipped_configurations:
                if e_type == parts[0] and v_name == parts[3]:
                    return
            """
            self.queue_iteration_required()

    def _queue_grid_value_update_notification(self, grid_value: GridValue):
        entry = {
            "value": grid_value.raw_get_value(override_freeze=True),
            "wall_clock_time": self.wattson_time.wall_clock_time(),
            "sim_clock_time": self.wattson_time.sim_clock_time()
        }

        if self._use_bulk_grid_value_updates.is_set():
            with self._bulk_grid_value_lock:
                self._bulk_grid_value_updates[grid_value.get_identifier()] = entry
        else:
            self.send_notification(
                PowerGridNotification(
                    notification_topic=PowerGridNotificationTopic.GRID_VALUES_UPDATED,
                    notification_data={"grid_values": {
                        grid_value.get_identifier(): entry
                    }}
                )
            )

    def _flush_bulk_grid_value_updates_loop(self):
        while not self._termination_requested.is_set():
            if self._termination_requested.wait(timeout=self._flush_interval):
                break
            self._flush_bulk_grid_value_updates()

    def _flush_bulk_grid_value_updates(self):
        with self._bulk_grid_value_lock:
            if len(self._bulk_grid_value_updates) > 0:
                self.send_notification(
                    PowerGridNotification(
                        notification_topic=PowerGridNotificationTopic.GRID_VALUES_UPDATED,
                        notification_data={"grid_values": self._bulk_grid_value_updates}
                    )
                )
            self._bulk_grid_value_updates = {}

    def _on_value_state_change(self, grid_value: GridValue):
        self.send_notification(PowerGridNotification(
            notification_topic=PowerGridNotificationTopic.GRID_VALUE_STATE_CHANGED,
            notification_data={"grid_value": {
                "identifier": grid_value.get_identifier(),
                "representation": self._get_grid_value_remote_dict(grid_value)
            }}
        ))

    def _apply_profile_updates(self, updates: List[Dict]):
        for update in updates:
            element = update.get("element", None)
            value_context = update.get("value_context", None)
            value_name = update.get("value_name", None)
            value = update.get("value")
            if element is None or value_context is None or value_name is None:
                self.logger.warning("Cannot apply profile update: Not enough data given")
                continue
            if value is None:
                continue
            try:
                internal_element = self.grid_model.get_element_by_identifier(element.get_identifier())
                internal_grid_value = internal_element.get(key=value_name, context=value_context)
                internal_grid_value.set_value(value)
            except Exception as e:
                self.logger.error(f"{e=}")
                self.logger.error(f"{traceback.format_exc()}")
