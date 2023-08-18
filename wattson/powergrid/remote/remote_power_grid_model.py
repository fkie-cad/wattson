import typing
from typing import Any, Type, List, Callable

from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.powergrid.remote.remote_grid_value import RemoteGridValue
from wattson.powergrid.simulator.messages.power_grid_notification_topic import PowerGridNotificationTopic
from wattson.powergrid.simulator.messages.power_grid_query import PowerGridQuery
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType


class RemotePowerGridModel(PowerGridModel, WattsonRemoteObject):
    def __init__(self, wattson_client: WattsonClient, **kwargs):
        super().__init__(**kwargs)
        self.wattson_client = wattson_client
        self.logger = self.wattson_client.logger.getChild("RemotePowerGridModel")
        self.synchronize(force=True, block=True)
        self._on_grid_value_changed_callbacks: List[Callable[[RemoteGridValue, Any, Any], Any]] = []

    def add_on_grid_value_change_callback(self, callback: Callable[[RemoteGridValue, Any, Any], Any]):
        self._on_grid_value_changed_callbacks.append(callback)

    def simulate(self) -> bool:
        raise NotImplementedError("Simulation not implemented for RemotePowerGridModel")

    def estimate(self) -> bool:
        raise NotImplementedError("Estimation not implemented for RemotePowerGridModel")

    def from_external(self, external_model: Any):
        raise NotImplementedError("RemotePowerGridModel can only be created via the WattsonClient")

    def to_external(self) -> Any:
        raise NotImplementedError("RemotePowerGridModel can only be exported from the non-remote instance")

    def synchronize(self, force: bool = False, block: bool = True):
        self.elements = {}
        query = PowerGridQuery(
            query_type=PowerGridQueryType.GET_GRID_REPRESENTATION,
            query_data={}
        )
        response = self.wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Could not retrieve power grid: {error=}")
            return
        data = response.data
        # Create Grid Elements
        for e_type, elements in data["grid_elements"].items():
            for e_id, element in elements.items():
                grid_element_class: Type[GridElement] = GridElement.element_class_by_type(e_type)
                grid_element = grid_element_class(create_specification=False, index=e_id)
                self.elements.setdefault(e_type, {})[grid_element.index] = grid_element

        # Fill in Attributes
        for e_type, elements in data["grid_elements"].items():
            for e_id, element in elements.items():
                grid_element = self.get_element(e_type, e_id)
                attributes = element["attributes"]
                for grid_value_context_name, grid_value_names in attributes.items():
                    for grid_value_name in grid_value_names:
                        grid_value = RemoteGridValue(
                            remote_power_grid_model=self,
                            wattson_client=self.wattson_client,
                            grid_element=grid_element,
                            name=grid_value_name,
                            value_context=GridValueContext[grid_value_context_name]
                        )
                        grid_value.add_on_set_callback(self._on_grid_value_set)
                        grid_element.set(grid_value.name, grid_value.value_context, grid_value)
        # Subscribe to element updates
        self.wattson_client.subscribe(PowerGridNotificationTopic.GRID_VALUE_CHANGED, self._grid_value_changed)

    def get_grid_value_by_identifier(self, grid_value_identifier: str) -> RemoteGridValue:
        grid_value = super().get_grid_value_by_identifier(grid_value_identifier=grid_value_identifier)
        return typing.cast(RemoteGridValue, grid_value)

    def _grid_value_changed(self, notification: WattsonNotification):
        changed_values = notification.notification_data.get("grid_values", {})
        for identifier, value in changed_values.items():
            grid_value = self.get_grid_value_by_identifier(identifier)
            grid_value.grid_value_changed(value)

    def _on_grid_value_set(self, grid_value: RemoteGridValue, old_value: Any, new_value: Any):
        if old_value != new_value:
            for callback in self._on_grid_value_changed_callbacks:
                callback(grid_value, old_value, new_value)
