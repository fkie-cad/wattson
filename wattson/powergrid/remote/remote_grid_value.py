import time
from typing import Optional, Any, TYPE_CHECKING, List, Callable

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
                 value_context: GridValueContext = GridValueContext.GENERIC):
        self._initialized = False
        self.wattson_client = wattson_client
        self._remote_power_grid_model = remote_power_grid_model
        self._last_synchronization = 0
        self._synchronization_interval = 10
        super().__init__(
            grid_element=grid_element,
            name=name,
            value_type=Any,
            value_context=value_context
        )
        self.logger = self.wattson_client.logger
        self.add_on_before_read_callback(self._on_before_read)
        self._initialized = True

    def grid_value_changed(self, value: Any):
        """
        Handler for received grid value changes by the WattsonClient, handled by the associated RemotePowerGridModel
        @param value: The new value of this GridValue
        @return:
        """
        if self._last_synchronization == 0:
            self.synchronize()
        else:
            self._last_synchronization = time.time()
            super().set_value(value)
        
    def set_value(self, value, timestamp: Optional[float] = None, value_scale: Optional[Scale] = None) -> bool:
        if not self._initialized:
            return False
        return self._on_set(value)

    def synchronize(self, force: bool = False, block: bool = True):
        if not force and not time.time() - self._last_synchronization > self._synchronization_interval:
            return

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

    def _on_set(self, value: Any) -> bool:
        # Write value to server
        query = PowerGridQuery(
            query_type=PowerGridQueryType.SET_GRID_VALUE,
            query_data={
                "grid_value_identifier": self.get_identifier(),
                "value": value
            }
        )
        response = self.wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"{self.get_identifier()}: Could not synchronize (on_set): {error=}")
            return False
        self._update_from_data(response.data)
        return True
