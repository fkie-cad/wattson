from wautorunner.scenario.scenario import ScenarioBuilder, Scenario
from wautorunner.scenario.modifiers.modifier_interface import ModifierInterface
from scenario.modifiers.modifier_concrete import MultiplyLoadsModifier, MultiplyGenerationModifier, CloseAllSwitchesModifier
from logging import getLogger
from pathlib import Path


class AutorunnerManager():
    def __init__(self, **kwargs):
        self.logger = getLogger("AutorunnerManager")
        self.logger.info("Building scenario")
        self.scenario: Scenario = kwargs.get("scenario", ScenarioBuilder.build(
            originPath=Path("wautorunner/scenarios/powerowl_example_template"),
            targetPath=Path("wautorunner/scenarios/powerowl_example_final")
        ))
        self.modifiers: list[ModifierInterface] = kwargs.get("modifiers", 
                                                        [MultiplyLoadsModifier(self.scenario, 2.0), 
                                                         MultiplyGenerationModifier(self.scenario, 0.5),
                                                         CloseAllSwitchesModifier(self.scenario)])

    def execute(self):
        """
        Execute the scenario with the given modifiers.
        """
        self.logger.info("Applying modifiers")
        for modifier in self.modifiers:
            modifier.modify()
        
        # self.scenario.run()

    def _stop(self):
        """
        Method to stop the AutorunnerManager.
        """
        pass

    