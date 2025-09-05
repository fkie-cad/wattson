import json
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


class ProtectionProvider(DataPointProvider):
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        super().__init__(provider_configuration, points, manager)
        self._connection_timeout_seconds = self.config.get("connection_timeout_seconds", 20)
        self._retry_connections = self.config.get("retry_connections", True)

        self._path_to_identifier_map = {}
        self._identifier_to_path_map = {}
        self._state_lock = threading.Lock()
        self._states = {}
        self._state_id = 0
        self._max_state_count = 2000

        self.filter_paths = set()

        self.logger = get_logger("ProtectionProvider")
        self.statistics = self.config.get("statistics", None)

        self.source_providers = {}

        self._protection_to_data_point_map = {}
        self._triggered_protections = set()

        for identifier, dp in self.data_points.items():
            if "sources" in dp["providers"]:
                for i, provider in enumerate(dp["providers"]["sources"]):
                    if provider["provider_type"] == "PROTECTION":
                        data = provider["provider_data"]
                        element_id = data.get("grid_element")
                        protection_name = data.get("protection_event")
                        self.logger.info(f"Mapping {element_id} // {protection_name} to {identifier}")
                        self._protection_to_data_point_map.setdefault(element_id, {})[protection_name] = identifier

        self._wattson_time: WattsonTime = WattsonTime()
        self.client: Optional[WattsonClient] = self.config.get("wattson_client")
        if self.client is None:
            raise ValueError("No WattsonClient given in provider configuration")
        self.client.subscribe(PowerGridNotificationTopic.PROTECTION_TRIGGERED, self._on_protection_triggering)
        self.client.subscribe(PowerGridNotificationTopic.PROTECTION_CLEARED, self._on_protection_cleared)

    def _on_protection_cleared(self, notification: WattsonNotification):
        self._handle_protection(notification, triggered=False)

    def _on_protection_triggering(self, notification: WattsonNotification):
        self._handle_protection(notification, triggered=True)

    def _handle_protection(self, notification: WattsonNotification, triggered: bool):
        grid_element_identifier = notification.notification_data.get("grid_element")
        protection_name = notification.notification_data.get("protection_name")
        if triggered:
            self.logger.warning(f"Protection triggered at {grid_element_identifier} for {protection_name}")
        else:
            self.logger.info(f"Protection event cleared at {grid_element_identifier} for {protection_name}")

        # Get identifier
        identifier = self._protection_to_data_point_map.get(grid_element_identifier, {}).get(protection_name)
        if identifier is None:
            self.logger.warning(f"Could not find data point for protection event {grid_element_identifier} {protection_name}")
            return

        if triggered:
            self._triggered_protections.add(identifier)
        elif identifier in self._triggered_protections:
            self._triggered_protections.remove(identifier)

        # TODO: StateID?
        if self.callbacks is not None:
            for callback in self.callbacks:
                callback(identifier, True if triggered else False, "0", "DP_ID")

    def start(self):
        self._wait_until_ready()

    def _wait_until_ready(self):
        while True:
            try:
                self.client.require_connection(self._connection_timeout_seconds)
                break
            except TimeoutError as e:
                if self._retry_connections:
                    self.logger.info(f"Connection timeout occurred - retrying...")
                    continue
                raise e
        if not self.client.is_registered:
            self.client.register()

        self._wattson_time = self.client.get_wattson_time()
        self.logger.info(f"Got Simulation start time: "
                         f"{self._wattson_time.start_datetime(time_type=WattsonTimeType.WALL).isoformat()} // "
                         f"{self._wattson_time.start_datetime_local(time_type=WattsonTimeType.WALL).isoformat()}")

    def _on_set(self, grid_value: GridValue, old_value: Any, new_value: Any):
        path = grid_value.get_identifier()
        changed = True
        try:
            changed = old_value != new_value
        except Exception:
            pass

        if not changed:
            return

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

    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        self.logger.error(f"Cannot set the value of a protection provider point")
        return False

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False, state_id: Optional[str] = None) -> DataPointValue:
        return identifier in self._triggered_protections
