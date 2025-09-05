import abc
from pathlib import Path
from typing import Optional, Set, Callable, TYPE_CHECKING

from wattson.cosimulation.control.interface.wattson_query_handler import WattsonQueryHandler
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.services.configuration import ConfigurationStore
from wattson.time import WattsonTime

if TYPE_CHECKING:
    from wattson.cosimulation.control.co_simulation_controller import CoSimulationController


class Simulator(WattsonQueryHandler):
    """
    A Simulator handles the simulation or emulation of one or multiple aspects of the co-simulation.
    Wattson distinguishes between network and physical simulators, where the default option for the network is an emulation.

    """
    def __init__(self):
        self._controller: Optional['CoSimulationController'] = None
        self._configuration_store: Optional[ConfigurationStore] = None
        self._working_directory: Optional[Path] = None
        self.send_notification_handler: Callable[[WattsonNotification], None] = None
        self._wattson_time: WattsonTime = WattsonTime()

    def set_controller(self, controller: Optional['CoSimulationController']):
        self._controller = controller

    @classmethod
    def get_simulator_type(cls) -> str:
        return "abstract"

    @property
    def wattson_time(self):
        return self._wattson_time

    @wattson_time.setter
    def wattson_time(self, wattson_time: WattsonTime):
        self._wattson_time = wattson_time

    @abc.abstractmethod
    def start(self):
        """
        Starts the simulator / emulator.
        :return:

        """
        ...

    @abc.abstractmethod
    def stop(self):
        """
        Stops the simulator / emulator.
        :return:

        """
        ...

    @abc.abstractmethod
    def load_scenario(self, scenario_path: Path):
        """
        Configures this simulator based on the scenario configuration in the given path.

        Args:
            scenario_path (Path):
                The path where the scenario configuration is stored.
        """
        ...

    def set_configuration_store(self, configuration_store: Optional[ConfigurationStore]):
        """
        Sets the ConfigurationStore for this Simulator to use

        Args:
            configuration_store (Optional[ConfigurationStore]):
                
        """
        self._configuration_store = configuration_store

    def get_configuration_store(self) -> Optional[ConfigurationStore]:
        """
        If set, returns the ConfigurationStore used by this simulator

        """
        return self._configuration_store

    def set_working_directory(self, working_directory: Path):
        """
        Sets the working directory for this simulator.
        This is not (necessarily) the python working directory, but a directory for the simulator to use for, e.g., storing artifacts.

        Args:
            working_directory (Path):
                The directory to use
        """
        self._working_directory = working_directory

    def get_working_directory(self) -> Path:
        """
        Returns the working directory of this simulator.
        If none has been set, a FileNotFoundError is raised.
        :return: The current working directory of this simulator.

        """
        if self._working_directory is None:
            raise FileNotFoundError("Working directory is not set")
        return self._working_directory

    @abc.abstractmethod
    def get_simulation_control_clients(self) -> Set[str]:
        """
        Returns a set of node identifiers that the simulation control server waits for to be connected.

        """
        ...

    def send_notification(self, notification: WattsonNotification):
        if self.send_notification_handler is not None:
            self.send_notification_handler(notification)
