import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.cosimulation.control.co_simulation_controller import CoSimulationController


class ScenarioExtension(abc.ABC):
    def __init__(self, co_simulation_controller: 'CoSimulationController', **kwargs):
        self.co_simulation_controller: 'CoSimulationController' = co_simulation_controller
        self.config = kwargs

    def provides_pre_physical(self) -> bool:
        return type(self).extend_pre_physical != ScenarioExtension.extend_pre_physical

    def provides_post_physical(self) -> bool:
        return type(self).extend_post_physical != ScenarioExtension.extend_post_physical

    def extend_pre_physical(self):
        """
        Called after the initial network creation, but before the physical simulator adds its modifications.
        :return:
        """
        pass

    def extend_post_physical(self):
        """
        Called after the initial network creation and after the physical simulator adds its modifications.
        :return:
        """
        pass
