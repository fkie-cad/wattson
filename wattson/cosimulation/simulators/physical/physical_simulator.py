import abc
import threading
from typing import Optional, Type

import wattson.util
from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator
from wattson.cosimulation.simulators.simulator import Simulator
from wattson.util import get_logger


class PhysicalSimulator(Simulator, abc.ABC):
    typed_simulators: dict = {
        "power-grid": "wattson.powergrid.simulator.power_grid_simulator.PowerGridSimulator",
        "default": "wattson.cosimulation.simulators.physical.empty_physical_simulator.EmptyPhysicalSimulator"
    }

    def __init__(self):
        super().__init__()
        self._network_emulator: Optional[NetworkEmulator] = None
        self.logger = get_logger(self.__class__.__name__, self.__class__.__name__)
        self._ready_event = threading.Event()

    @classmethod
    def get_simulator_type(cls) -> str:
        return "physical"

    def set_simulator_ready_event(self):
        self._ready_event.set()

    def wait_until_ready(self, timeout: Optional[float] = None) -> bool:
        return self._ready_event.wait(timeout=timeout)

    def set_network_emulator(self, network_emulator: Optional[NetworkEmulator]):
        self._network_emulator = network_emulator

    @staticmethod
    def register_simulator(scenario_type: str, simulator_class: Type['PhysicalSimulator']):
        PhysicalSimulator.typed_simulators[scenario_type] = simulator_class

    @staticmethod
    def from_scenario_type(scenario_type: str, **kwargs) -> 'PhysicalSimulator':
        simulator_class = PhysicalSimulator.typed_simulators.get(
            scenario_type, PhysicalSimulator.typed_simulators["default"]
        )
        if isinstance(simulator_class, str):
            simulator_class = wattson.util.dynamic_load_class(simulator_class)
        return simulator_class(**kwargs)
