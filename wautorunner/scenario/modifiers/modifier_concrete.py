from wautorunner.scenario.modifiers.modifier_interface import ModifierInterface
from wautorunner.scenario.scenario import Scenario
from enum import Enum

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

class SetAllSwitchesModifier(ModifierInterface):
    """
    Set state of all circuit breakers in the power grid model.
    """

    def __init__(self, scenario: Scenario, status: bool) -> None:
        super().__init__(scenario)
        self.status = status

    def modify(self) -> None:
        """
        Set state of all circuit breakers in the power grid model.
        """
        powerGridModel: dict = self.scenario.getPowerGridModel()
        switches: dict = powerGridModel.get("elements", {}).get("switch", {})
        # Modify switch states appropriately
        for _, switchData in switches.items():
            switchData["attributes"]["CONFIGURATION"]["closed"] = self.status
            switchData["attributes"]["ESTIMATION"]["is_closed"] = self.status
            switchData["attributes"]["MEASUREMENT"]["is_closed"] = self.status

        self.scenario.savePowerGridModel(powerGridModel)

class SetSwitchesModifier(ModifierInterface):
    """
    Set switches states given a state dictionary
    """

    def __init__(self, scenario: Scenario, status: dict) -> None:
        """
        INPUT
            **status**: dict = { switchIndex: True | False } 
        """
        super().__init__(scenario)
        self.status = status

    def modify(self):
        powerGridModel: dict = self.scenario.getPowerGridModel()
        switches: dict = powerGridModel.get("elements", {}).get("switch", {})

        for id, state in self.status.items():
            switches[id]["attributes"]["CONFIGURATION"]["closed"] = state
            switches[id]["attributes"]["ESTIMATION"]["is_closed"] = state
            switches[id]["attributes"]["MEASUREMENT"]["is_closed"] = state

        self.scenario.savePowerGridModel(powerGridModel)

class StrategyType(Enum):
    """
    Enum for the different types of policies.
    """
    EXPLICIT = "explicit"
    INTERMITTENT = "intermittent"

class AttackerStrategyModifier(ModifierInterface):

    def __init__(self, scenario: Scenario, strategyType: StrategyType, strategy: list[dict] | list[str]) -> None:
        super().__init__(scenario)

        if strategyType == StrategyType.EXPLICIT and not isinstance(strategy, list[dict]):
            raise ValueError("Strategy must be a list of dictionaries")
        if strategyType == StrategyType.INTERMITTENT and not isinstance(strategy, list[str]):
            raise ValueError("Strategy must be a list of strings")
        
        self.strategyType = strategyType
        self.strategy = strategy

    def modify(self) -> None:
        """
        Modify the attack strategy in the scenario.
        """
        config: dict = self.scenario.getScriptControllerConfig()
        services: list = config.get("nodes", {}).get("ctrl", {}).get("services", [])
        service = None
        for s in services:
            if s["module"] == "wattson.apps.script_controller":
                service = s
                break
        if service is None:
            raise ValueError("Service not found in script controller config")
        
        scripts: list = service.get("config", {}).get("scripts", [])
        for script in scripts:
            if script["script"] == "wattson.apps.script_controller.toolbox.close_switches_script.CloseSwitchesScript":
                script["enabled"] = True
                scriptConfig = script["config"]
                break
        else:
            raise ValueError("Attacker strategy script not found in script controller config")

        scriptConfig["close_delay"] = 4
        scriptConfig["strategy"] = self.strategy
        scriptConfig["strategy_type"] = self.strategyType

        self.scenario.saveScriptControllerConfig(config)