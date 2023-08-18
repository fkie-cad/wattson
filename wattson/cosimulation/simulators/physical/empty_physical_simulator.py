from pathlib import Path
from typing import Union, Type, Optional, Set

from wattson.cosimulation.exceptions import InvalidSimulationControlQueryException
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.simulators.physical.physical_simulator import PhysicalSimulator


class EmptyPhysicalSimulator(PhysicalSimulator):
    """
    This physical simulator does not do anything, it just implements the required interface.
    This is useful to use Wattson for a pure network representation.
    """
    def start(self):
        self.set_simulator_ready_event()

    def stop(self):
        pass

    def load_scenario(self, scenario_path: Path):
        pass

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        return False

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        raise InvalidSimulationControlQueryException(f"EmptyPhysicalSimulator does not handle any queries")

    def get_simulation_control_clients(self) -> Set[str]:
        return set()
