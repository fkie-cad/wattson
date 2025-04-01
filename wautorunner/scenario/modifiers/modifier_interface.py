from abc import ABC, abstractmethod
from wautorunner.scenario.scenario import Scenario

class ModifierInterface(ABC):
    """
    Interface for all modifiers.
    """

    def __init__(self, scenario: Scenario) -> None:
        self.scenario: Scenario = scenario

    @abstractmethod
    def modify(self) -> None:
        """
        Modify the scenario.
        """
        ...