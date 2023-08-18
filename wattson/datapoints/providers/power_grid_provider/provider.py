import threading

from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.powergrid.remote.remote_power_grid_model import RemotePowerGridModel
from wattson.powergrid.simulator.messages.power_grid_control_query import PowerGridControlQuery
from wattson.powergrid.simulator.messages.power_grid_measurement_query import PowerGridMeasurementQuery
from wattson.powergrid.simulator.messages.power_grid_measurement_response import PowerGridMeasurementResponse
from wattson.powergrid.simulator.messages.power_grid_notification import PowerGridNotification
from wattson.powergrid.simulator.messages.power_grid_notification_topic import PowerGridNotificationTopic
from wattson.powergrid.simulator.messages.power_grid_query import PowerGridQuery
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType
from wattson.datapoints.interface import DataPointValue
from wattson.datapoints.interface.data_point_provider import DataPointProvider

from wattson.time import WattsonTimeType
from wattson.time.wattson_time import WattsonTime
from wattson.util import get_logger
from time import time
from typing import Dict, Tuple, TYPE_CHECKING, Optional, List, Set, Iterable, Any

if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class PowerGridProvider(DataPointProvider):
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        super().__init__(provider_configuration, points, manager)
        self.cache: Dict[Tuple[str, int], Dict] = {}
        self.cache_decay = self.config.get("cache_decay", 1)
        self._connection_timeout_seconds = self.config.get("connection_timeout_seconds", 20)

        self._path_to_identifier_map = {}
        self._identifier_to_path_map = {}
        self._state_lock = threading.Lock()
        self._states = {}
        self._state_id = 0
        self._max_state_count = 2000

        self.filter_paths = set()

        self.logger = get_logger("PowerGridProvider", "PowerGridProvider")
        self.statistics = self.config.get("statistics", None)

        self.source_providers = {}
        for identifier, dp in self.data_points.items():
            # Build Path -> Identifier Map
            self._add_identifier_to_map(identifier, dp)

            if "source" in dp["providers"]:
                for i, provider in enumerate(dp["providers"]["source"]):
                    if provider["provider_type"] == "POWER_GRID":
                        self._register_source_provider(identifier, i)

        self._wattson_time: WattsonTime = WattsonTime()
        self.client: Optional[WattsonClient] = self.config.get("wattson_client")
        if self.client is None:
            raise ValueError("No WattsonClient given in provider configuration")
        self.remote_power_grid_model: Optional[RemotePowerGridModel] = None

    def add_filter_ids(self, ids: Iterable):
        """
        Subscribes to the given data point identifiers for on_change events.
        For future change events linked to one of the given identifiers, the configured callbacks are called.
        @param ids: The set or list of data point identifiers
        @return:
        """
        super().add_filter_ids(ids)
        paths = set()
        for identifier in ids:
            paths.update(self._identifier_to_path_map.get(identifier, set()))
        self.add_filter_paths(paths=paths)

    def add_filter_paths(self, paths: Set):
        """
        Subscribes to changes of the identified grid value identifiers (paths), e.g., bus.1.voltage.
        :param paths: A set of grid value identifiers / paths to monitor.
        :return:
        """
        self.filter_paths.update(paths)
        for path in paths:
            try:
                grid_value = self.remote_power_grid_model.get_grid_value_by_identifier(grid_value_identifier=path)
                grid_value.add_on_set_callback(self._on_change)
            except Exception as e:
                self.logger.error(f"Could not subscribe to element updates for {path}: {repr(e)}")

    @property
    def _next_state_id(self) -> str:
        self._state_id += 1
        return str(self._state_id)

    def _store_state(self, value) -> str:
        with self._state_lock:
            next_id = self._next_state_id
            self._states[next_id] = value
            to_delete = str(int(self._next_state_id) - self._max_state_count)
            if to_delete in self._states:
                del self._states[to_delete]
            return next_id

    def start(self):
        self._wait_until_ready()

    def _wait_until_ready(self):
        self.client.require_connection(self._connection_timeout_seconds)
        if not self.client.is_registered:
            self.client.register()
        self.remote_power_grid_model = RemotePowerGridModel(wattson_client=self.client)

        self._wattson_time = self.client.get_wattson_time()
        self.logger.info(f"Got Simulation start time: "
                         f"{self._wattson_time.start_datetime(time_type=WattsonTimeType.WALL).isoformat()} // "
                         f"{self._wattson_time.start_datetime_local(time_type=WattsonTimeType.WALL).isoformat()}")

    def _on_change(self, grid_value: GridValue, old_value: Any, new_value: Any):
        path = grid_value.get_identifier()

        if self.callbacks is not None:
            state_id = self._store_state(new_value)
            data_point_identifiers = self._get_data_point_identifiers_by_grid_value(grid_value.get_grid_element().get_identifier(),
                                                                                    grid_value.value_context.name,
                                                                                    grid_value.name)
            for identifier in data_point_identifiers:
                for callback in self.callbacks:
                    callback(identifier, new_value, state_id, "DP_ID")
            if path in self.filter_paths:
                for callback in self.callbacks:
                    callback(path, new_value, state_id, "PATH")

    def stop(self):
        self.client.stop()

    def _get_grid_value(self, provider_info: dict) -> GridValue:
        grid_value_identifier = f"{provider_info['grid_element']}.{provider_info['context']}.{provider_info['attribute']}"
        grid_value = self.remote_power_grid_model.get_grid_value_by_identifier(grid_value_identifier=grid_value_identifier)
        return grid_value

    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        info = self._get_provider_info(identifier, provider_id, "targets")
        grid_value = self._get_grid_value(info)
        return grid_value.set_value(value=value)

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        if state_id is not None:
            if state_id in self._states:
                return self._states[state_id]
            self.logger.warning(f"Requested state {state_id} is unknown")

        info = self._get_provider_info(identifier, provider_id, "sources")
        try:
            grid_value = self._get_grid_value(info)
            value = grid_value.get_value()
        except Exception as e:
            self.logger.error(f"Failed to read power grid data for {identifier}: {e=}")
            return None
        return value

    def clear_cache(self):
        self.cache = {}

    def update_cache(self):
        self.cache = {}
        for identifier, providers in self.source_providers.items():
            for index in providers:
                self.get_value(identifier, index)

    def _add_identifier_to_map(self, identifier: str, data_point: dict):
        """
        Get all power grid value / attribute identifiers (e_type.e_index.a_context.a_name) for this data point
        and map them to the data point identifier.
        :param identifier: The data point identifier
        :param data_point: The data point configuration dictionary
        :return:
        """
        paths = set()
        dp = data_point
        for p_type in ["sources", "targets"]:
            if p_type in dp["providers"]:
                for provider in dp["providers"][p_type]:
                    if provider["provider_type"] == "POWER_GRID":
                        element_identifier = provider["provider_data"]["grid_element"]
                        attribute_context = provider["provider_data"]["context"]
                        attribute_name = provider["provider_data"]["attribute"]
                        path = f"{element_identifier}.{attribute_context}.{attribute_name}"
                        paths.add(path)
        self._identifier_to_path_map[identifier] = paths

        for path in paths:
            self._path_to_identifier_map.setdefault(path, set()).add(identifier)

    def _get_data_point_identifiers_by_grid_value(self,
                                                  grid_element_identifier: str,
                                                  grid_value_context: str,
                                                  grid_value_name: str) -> List[str]:
        """
        Given a grid value path (i.e., the grid element's identifier and grid value name), returns a list
        of data point identifiers that are linked to this grid value.
        @param grid_element_identifier: The element identifier, e.g., bus.0
        @param grid_value_context: The grid value context, e.g., MEASUREMENT
        @param grid_value_name: The name of the grid value
        @return: A list of data point identifiers that are linked to the specified grid value
        """
        path = f"{grid_element_identifier}.{grid_value_context}.{grid_value_name}"
        return self._path_to_identifier_map.get(path, set())

    def _get_provider_info(self, identifier: str, index: int, key: str = "sources"):
        dp = self.data_points[identifier]
        if key not in dp["providers"]:
            raise ValueError("Invalid provider address")
        provider = dp["providers"][key][index]
        if provider["provider_type"] != "POWER_GRID":
            raise ValueError("Provider address does not represent a POWER_GRID provider")
        return provider["provider_data"]

    def _wattson_notification_handler(self, notification: WattsonNotification):
        if notification.notification_topic == WattsonNotificationTopic.SIMULATION_START:
            self.logger.debug("Simulation started")
        elif isinstance(notification, PowerGridNotification):
            if notification.notification_topic == PowerGridNotificationTopic.SIMULATION_STEP_DONE:
                # TODO: Remove this logging statement
                self.logger.info("Simulation step done notification")
                self.update_cache()

    def _register_source_provider(self, identifier, index):
        if identifier not in self.source_providers:
            self.source_providers[identifier] = []
        self.source_providers[identifier].append(index)
