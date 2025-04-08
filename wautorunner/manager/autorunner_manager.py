from wautorunner.scenario.scenario import ScenarioBuilder, Scenario
from wautorunner.analyzer.experiment_analyzer import ExperimentAnalyzer
from wautorunner.scenario.modifiers.modifier_interface import ModifierInterface
from scenario.modifiers.modifier_concrete import MultiplyLoadsModifier, MultiplyGenerationModifier, CloseAllSwitchesModifier
from wattson.cosimulation.control.co_simulation_controller import CoSimulationController
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator
from logging import getLogger
from pathlib import Path
import time, sys, traceback, signal
from pprint import pprint

class AutorunnerManager():
    DEBUG_ANALYZER: bool = True

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

    def execute(self, period_s: float = 10):
        """
        Execute the scenario with the given modifiers.
        """
        self.logger.info("Applying modifiers")
        for modifier in self.modifiers:
            modifier.modify()
        
        if not AutorunnerManager.DEBUG_ANALYZER:
            config = {}
            controller: CoSimulationController = CoSimulationController(self.scenario.getScenarioPath(),
                                                network_emulator=WattsonNetworkEmulator(),
                                                **config)
            controller.network_emulator.enable_management_network()
            controller.load_scenario()
            controller.start()
            self.logger.info("Wattson started!")

            def teardown(_sig, _frame):
                AutorunnerManager.stopController(controller)

            signal.signal(signalnum=signal.SIGTERM, handler=teardown)
            signal.signal(signalnum=signal.SIGINT, handler=teardown)

            try:
                # Let it run for maximum period_s seconds
                controller.join(period_s)
            except Exception:
                pass
            finally:
                AutorunnerManager.stopController(controller)

        # TODO Perform log analysis
        analyzer: ExperimentAnalyzer = ExperimentAnalyzer(Path("wattson-artifacts"), self.scenario)
        traces: dict = analyzer.genTraces(2)
        print(traces)
        self.logger.info("Finished execution")

    @staticmethod
    def stopController(controller: CoSimulationController):
        try:
            controller.logger.info("Stopping Wattson")
            controller.stop()
        except Exception as e:
            controller.logger.warning(f"Error during teardown occurred - trying cleanup")
            controller.logger.error(f"{e=}")
            controller.logger.error(traceback.print_exception(e))
            
            from wattson.util.clean.__main__ import main as wattson_clean
            wattson_clean()      

    def stop(self):
        """
        Method to stop the AutorunnerManager.
        """
        pass

    