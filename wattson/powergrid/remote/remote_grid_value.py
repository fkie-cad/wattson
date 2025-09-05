import time
from typing import Optional, Any, TYPE_CHECKING, List, Callable, Dict

from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.layers.powergrid.values.grid_value_type import GridValueType
from powerowl.layers.powergrid.values.units.scale import Scale
from powerowl.layers.powergrid.values.units.unit import Unit

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.powergrid.simulator.messages.power_grid_notification_topic import PowerGridNotificationTopic
from wattson.powergrid.simulator.messages.power_grid_query import PowerGridQuery
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType

if TYPE_CHECKING:
    from powerowl.layers.powergrid.elements import GridElement
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient
    from wattson.powergrid.remote.remote_power_grid_model import RemotePowerGridModel


class RemoteGridValue(GridValue, WattsonRemoteObject):
    def __init__(self,
                 remote_power_grid_model: 'RemotePowerGridModel',
                 wattson_client: 'WattsonClient',
                 grid_element: Optional['GridElement'],
                 name: str = None,
                 value_context: GridValueContext = GridValueContext.GENERIC,
                 grid_value_data: Optional[Dict] = None):
        self._initialized = False
        self.wattson_client = wattson_client
        self._remote_power_grid_model = remote_power_grid_model
        self._last_synchronization = 0
        self._synchronization_interval = 60
        super().__init__(
            grid_element=grid_element,
            name=name,
            value_type=Any,
            value_context=value_context
        )
        self.logger = self.wattson_client.logger
        self.add_on_before_read_callback(self._on_before_read)
        self._initialized = True
        if grid_value_data is not None:
            self._update_from_data(grid_value_data)

    def _handle_state_query(self, query):
        response = self.wattson_client.query(query=query, block=True)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"{self.get_identifier()}: Could not lock: {error=}")
            return False
        self._update_from_data(response.data)
        return True

    def lock(self):
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE_STATE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "state_type": "lock",
                "state_target": True
            }
        )
        return self._handle_state_query(query)

    def unlock(self):
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE_STATE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "state_type": "lock",
                "state_target": False
            }
        )
        return self._handle_state_query(query)

    def freeze(self, frozen_value):
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE_STATE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "state_type": "freeze",
                "state_target": True,
                "frozen_value": frozen_value
            }
        )
        return self._handle_state_query(query)

    def unfreeze(self):
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE_STATE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "state_type": "freeze",
                "state_target": False
            }
        )
        return self._handle_state_query(query)

    def grid_value_changed(self, value: Any, timestamp: Optional[float] = None):
        """
        Handler for received grid value changes by the WattsonClient, handled by the associated RemotePowerGridModel

        Args:
            value (Any):
                The new value of this GridValue
            timestamp (Optional[float], optional):
                The timestamp of the update
                (Default value = None)
        """
        if self._last_synchronization == 0:
            self.synchronize()
        else:
            self._last_synchronization = time.time()
            super().set_value(value, timestamp=timestamp, override_lock=True)

    def grid_value_state_changed(self, data: dict):
        """
        Handler for received grid value state changes by the WattsonClient.

        Args:
            data (dict):
                The dict representation of this grid value, including the new state
        """
        # self.wattson_client.logger.info(f"Grid value state changed {self.get_identifier()}")
        self._update_from_data(data)
        
    def set_value(self, value, timestamp: Optional[float] = None, value_scale: Optional[Scale] = None, override_lock: bool = False, set_targets: bool = True) -> bool:
        if not self._initialized:
            return False
        return self._on_set(value, override_lock)

    def synchronize(self, force: bool = False, block: bool = True):
        if not force and (self._synchronization_interval is None or not time.time() - self._last_synchronization > self._synchronization_interval):
            return

        overdue = time.time() - self._last_synchronization
        query = PowerGridQuery(
            query_type=PowerGridQueryType.GET_GRID_VALUE,
            query_data={"grid_value_identifier": self.get_identifier()}
        )
        response = self.wattson_client.query(query=query, block=True)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"{self.get_identifier()}: Could not synchronize: {error=}")
            return
        self._update_from_data(response.data)

    def _update_from_data(self, data: dict):
        self._last_synchronization = time.time()
        if isinstance(data["value"], dict):
            v = data["value"]
            if v.get("__type") == "GridElement":
                data["value"] = self._remote_power_grid_model.get_element(v["element_type"], v["element_index"])
        self.from_dict(data)

    def _on_before_read(self, _: 'GridValue'):
        # Optional synchronization
        self.synchronize()

    def _on_set(self, value: Any, override: bool = False) -> bool:
        # Write value to server
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "value": value,
                "override": override
            }
        )
        response = self.wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"{self.get_identifier()}: Could not synchronize (on_set): {error=}")
            return False
        old_value = self.value
        self._update_from_data(response.data)
        self._trigger_on_set(old_value)
        return True
