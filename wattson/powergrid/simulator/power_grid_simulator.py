import json
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Union, Type, Optional, Set, Dict, List, Any

import yaml

from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.elements.grid_node import GridNode
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.simulators.pandapower import PandaPowerGridModel

from wattson.cosimulation.exceptions import InvalidScenarioException, InvalidSimulationControlQueryException
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.datapoints.data_point_loader import DataPointLoader
from wattson.powergrid.profiles.profile_loader import ProfileLoader
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType
from wattson.powergrid.simulator.threads.export_thread import ExportThread
from wattson.services.wattson_python_service import WattsonPythonService
from wattson.services.wattson_pcap_service import WattsonPcapService
from wattson.services.configuration import ServiceConfiguration
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
from wattson.iec104.common.config import SERVER_UPDATE_PERIOD_MS


class PowerGridSimulator(PhysicalSimulator):
    def __init__(self, grid_model_class: Type[PowerGridModel] = PandaPowerGridModel):
        super().__init__()
        self._common_addresses = {}
        self._mtu_entity_ids = []
        self._mtu_ids = []
        self._rtu_map = {}
        self._grid_model = grid_model_class()
        self._simulator_thread: Optional[SimulationThread] = None
        self._simulation_required = QueueEvent(max_queue_time_s=2, max_queue_interval_s=0.05)
        self._required_sim_control_clients = set()
        self._profile_thread: Optional[ProfileLoader] = None
        self._export_thread: Optional[ExportThread] = None
        self._auto_export_enable: bool = True
        self._ready_event = MultiEvent()

    def start(self):
        self._simulation_required.set()
        profile_config = {
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
                "speed": 1.0,
                "start_datetime": False
            },
            "stop": False
        }
        profile_config.update(self.get_configuration_store().get("configuration", {}).get("power-grid", {}).get("profile-loader", {}))
        self._simulator_thread = SimulationThread(
            self._grid_model,
            iteration_required_event=self._simulation_required,
            on_iteration_completed_callback=self._on_simulation_iteration_completed,
            on_value_change_callback=self._on_value_change
        )
        self._profile_thread = ProfileLoader(
            grid_model=self._grid_model,
            apply_updates_callback=self._apply_profile_updates,
            **profile_config
        )
        self._export_thread = ExportThread(
            export_path=self.get_working_directory().joinpath("power_grid_exports"),
            enable=self._auto_export_enable
        )
        self._ready_event.monitor(
            self._simulator_thread.ready_event,
            self._profile_thread.ready_event
        )
        self._export_thread.start()
        self._simulator_thread.start()
        self._profile_thread.start()

    def enable_export(self):
        self._export_thread.enable_export()

    def disable_export(self):
        self._export_thread.disable_export()

    def queue_iteration_required(self):
        self._simulation_required.queue()

    def stop(self):
        if self._profile_thread is not None and self._profile_thread.is_alive():
            self._profile_thread.stop()
            self._profile_thread.join()
        if self._simulator_thread is not None and self._simulator_thread.is_alive():
            self._simulator_thread.stop()
            self._simulator_thread.join()
        if self._export_thread is not None and self._export_thread.is_alive():
            self._export_thread.stop()
            self._export_thread.join()

    @property
    def grid_model(self):
        return self._grid_model

    def load_from_grid_model(self, grid_model: PowerGridModel, data_points: dict):
        self._grid_model = grid_model
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
            power_grid_data = yaml.load(f, Loader=yaml.Loader)
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
                if grid_value.set_value(query.value):
                    self._simulation_required.set()
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
            if query.query_type == PowerGridQueryType.GET_GRID_VALUE:
                query.mark_as_handled()
                grid_value_identifier = query.query_data.get("grid_value_identifier")
                try:
                    grid_value = self.grid_model.get_grid_value_by_identifier(grid_value_identifier)
                except Exception as e:
                    return WattsonResponse(successful=False, data={"error": repr(e)})
                value_dict = self._get_grid_value_remote_dict(grid_value)
                return WattsonResponse(successful=True, data=value_dict)

            if query.query_type == PowerGridQueryType.SET_GRID_VALUE:
                query.mark_as_handled()
                grid_value_identifier = query.query_data.get("grid_value_identifier")
                value = query.query_data.get("value")
                try:
                    grid_value = self.grid_model.get_grid_value_by_identifier(grid_value_identifier)
                    grid_value.set_value(value)
                except Exception as e:
                    return WattsonResponse(successful=False, data={"error": repr(e)})
                return WattsonResponse(successful=True, data=self._get_grid_value_remote_dict(grid_value))

            if query.query_type == PowerGridQueryType.GET_GRID_REPRESENTATION:
                query.mark_as_handled()
                response_elements = {}
                for e_type, elements in self.grid_model.elements.items():
                    element: 'GridElement'
                    for e_id, element in elements.items():
                        attributes = {}
                        for (value_name, grid_value) in element.get_grid_values():
                            attributes.setdefault(grid_value.value_context.name, []).append(grid_value.name)
                        response_elements.setdefault(e_type, {})[e_id] = {
                            "attributes": attributes
                        }
                return WattsonResponse(successful=True, data={"grid_elements": response_elements})

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
                from wattson.hosts.rtu import RtuDeployment
                node.add_service(WattsonPythonService(RtuDeployment, rtu_configuration, node))
                self._required_sim_control_clients.add(node.entity_id)

            if node.has_role("mtu"):
                if node.has_service("mtu"):
                    continue
                self._mtu_entity_ids.append(node.entity_id)
                self._mtu_ids.append(node.id)

                # TODO: Remove this at this place!
                node.add_role("vcc")

                # Create MTU configuration
                mtu_configuration = MtuDefaultConfiguration()
                from wattson.hosts.mtu import MtuDeployment
                node.add_service(WattsonPythonService(MtuDeployment, mtu_configuration, node))
                # Add pcap services by default for all interfaces of the MTU
                for interface in node.get_interfaces():
                    node.add_service(WattsonPcapService(interface=interface, service_configuration=ServiceConfiguration(), network_node=node))
                rtu_map = node.config.get("rtu_map", {})
                self._rtu_map[node.entity_id] = {rtu_id: {"coa": rtu_id, "ip": rtu_ip.split("/")[0]} for rtu_id, rtu_ip in rtu_map.items()}
                self._required_sim_control_clients.add(node.entity_id)

            if node.has_role("vcc"):
                if node.has_service("vcc"):
                    continue
                # Create VCC configuration
                # TODO: Dynamically assign the MTU!
                try:
                    vcc_configuration = VccDefaultConfiguration()
                    from wattson.apps.gui.deployment import GuiDeployment
                    node.add_service(WattsonPythonService(GuiDeployment, vcc_configuration, node))
                except ImportError:
                    self.logger.warning(f"Cannot add VCC service as the module cannot be found")

    def _fill_configuration_store(self):
        # MTU Options
        self._configuration_store.register_configuration("mtu_connect_delay", 0)
        self._configuration_store.register_configuration("do_general_interrogation", True)
        self._configuration_store.register_configuration("do_clock_sync", True)
        # RTU Options
        self._configuration_store.register_configuration("rtu_logic", {
            "*": [{"class": "wattson.hosts.rtu.logic.spontaneous_logic.SpontaneousLogic"}]
        })
        self._configuration_store.register_configuration("do_periodic_updates", True)
        self._configuration_store.register_configuration("periodic_update_start", 0)
        self._configuration_store.register_configuration("periodic_update_ms", SERVER_UPDATE_PERIOD_MS)
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

    def _on_simulation_iteration_completed(self, successful: bool):
        t = time.time()
        # Trigger measurement value updates
        for e_type, elements in self.grid_model.elements.items():
            for e_id, element in elements.items():
                for (value_name, grid_value) in element.get_grid_values(context=GridValueContext.MEASUREMENT):
                    if grid_value.simulator_context is not None:
                        grid_value.get_value()
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
                        if isinstance(value, (float, bool, int, str)):
                            export_dict[grid_value.get_identifier()] = value
            self._export_thread.export(timestamp=t, values=export_dict)

    def _on_value_change(self, grid_value: GridValue, old_value: Any, new_value: Any):
        for related in grid_value.get_related_grid_values():
            # Potentially trigger callbacks for related grid values
            related.get_value()
        self.send_notification(PowerGridNotification(
            notification_topic=PowerGridNotificationTopic.GRID_VALUE_CHANGED,
            notification_data={"grid_values": {grid_value.get_identifier(): grid_value.raw_get_value()}}
        ))
        if grid_value.value_context == GridValueContext.CONFIGURATION:
            self.queue_iteration_required()

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
                interval_grid_value = internal_element.get(key=value_name, context=value_context)
                interval_grid_value.set_value(value)
            except Exception as e:
                self.logger.error(f"{e=}")
                self.logger.error(f"{traceback.print_exception(*sys.exc_info())}")
