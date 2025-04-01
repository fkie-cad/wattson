from wautorunner.scenario.modifiers.modifier_interface import ModifierInterface
from wautorunner.scenario.scenario import Scenario

class MultiplyLoadsModifier(ModifierInterface):
    """
    Multiply all loads by a given factor.
    """

    def __init__(self, scenario: Scenario, factor: float) -> None:
        super().__init__(scenario)
        self.factor = factor

    def modify(self) -> None:
        """
        Multiply all loads by the given factor.
        """
        powerGridModel: dict = self.scenario.getPowerGridModel()
        loads: dict = powerGridModel.get("elements", {}).get("load", {})
        # Modify load scales appropriately
        for _, loadData in loads.items():
            origLoadScale: float = loadData["attributes"]["CONFIGURATION"]["scaling"]
            loadData["attributes"]["CONFIGURATION"]["scaling"] = origLoadScale * self.factor

        self.scenario.savePowerGridModel(powerGridModel)

class MultiplyGenerationModifier(ModifierInterface):
    """
    Multiply all generation scales by a given factor.
    """

    def __init__(self, scenario: Scenario, factor: float) -> None:
        super().__init__(scenario)
        self.factor = factor

    def modify(self) -> None:
        """
        Multiply all generation scales by the given factor.
        """
        powerGridModel: dict = self.scenario.getPowerGridModel()
        generators: dict = powerGridModel.get("elements", {}).get("sgen", {})
        # Modify generation scales appropriately
        for _, generatorData in generators.items():
            origGenScale: float = generatorData["attributes"]["CONFIGURATION"]["scaling"]
            generatorData["attributes"]["CONFIGURATION"]["scaling"] = origGenScale * self.factor

        self.scenario.savePowerGridModel(powerGridModel)    

class CloseAllSwitchesModifier(ModifierInterface):
    """
    Close all circuit breakers in the power grid model.
    """

    def __init__(self, scenario: Scenario) -> None:
        super().__init__(scenario)

    def modify(self) -> None:
        """
        Close all circuit breakers in the power grid model.
        """
        powerGridModel: dict = self.scenario.getPowerGridModel()
        switches: dict = powerGridModel.get("elements", {}).get("switch", {})
        # Modify switch states appropriately
        for _, switchData in switches.items():
            switchData["attributes"]["CONFIGURATION"]["closed"] = True
            switchData["attributes"]["ESTIMATION"]["is_closed"] = True
            switchData["attributes"]["MEASUREMENT"]["is_closed"] = True

        self.scenario.savePowerGridModel(powerGridModel)