import datetime

from wattson.apps.script_controller.interface import Script
from abc import abstractmethod

from typing import TYPE_CHECKING, Tuple, Optional, Union

if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp
    from wattson.apps.script_controller.runner import TimedRunner


class TimedScript(Script):
    def __init__(self, controller: 'ScriptControllerApp'):
        super().__init__(controller)

    def get_simulated_time_info(self) -> Optional[Union[datetime.datetime, float]]:
        """
        Returns the (simulated) time that this script should start.
        A float value represents a relative offset to the starting simulated time (coordinator).
        A datetime represents an absolute value.
        None equals the float value 0 (i.e., script starts as soon as the simulation starts) :return:

        """
        return None

    def get_start_time(self):
        return 0

    @abstractmethod
    def setup(self, runner: 'TimedRunner'):
        pass
