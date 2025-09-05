import time
import logging
from datetime import datetime
from threading import Thread
from typing import Optional, TYPE_CHECKING, Union, Callable

if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp
    from wattson.apps.script_controller.interface.timed_script import TimedScript


class TimedRunner(Thread):
    def __init__(self, controller: 'ScriptControllerApp', sleep: float = 0.1):
        super().__init__()
        self._controller = controller

        self._wattson_time = controller.wattson_time

        self._scripts = []
        self._pending_actions = []
        self._sleep = sleep
        self._start_time = 0
        self._default_logger = self._controller.logger.getChild("GenericTimedRunner")
        self._default_logger.setLevel(logging.DEBUG)
        self._scope = "default"
        self._loggers = {
            "default": self._default_logger
        }

    def get_controller(self):
        return self._controller

    def add_script(self, script: 'TimedScript'):
        """
        start_date, speed = script.get_simulated_time_info() if speed is None and self._sim_speed is None: speed = 1 if start_date is not None:
        o_start_date = self._wattson_time self._wattson_time = start_date if type(start_date) == float else start_date.timestamp() if o_start_date
        is not None and start_date != o_start_date: self._default_logger.warning("Different scripts requested different start times: " f"{self._wattson_time}
        vs {o_start_date}")
        if speed is not None: if self._sim_speed is not None and speed != self._sim_speed: self._default_logger.warning(f"Different scripts requested
        different simulated speeds: " f"({speed} vs {self._sim_speed})!") self._sim_speed = speed

        Args:
            script ('TimedScript'):
                
        """

        self._scripts.append(script)

    def start(self) -> None:
        script: TimedScript

        """
        if self._wattson_time is not None:
            self._default_logger.info("Updating simulated time at controller")
            self._controller.set_simulated_start_time(self._wattson_time, self._sim_speed)

        while not self._controller.coord_client.is_sim_running():
            time.sleep(0.5)
        
        self._wattson_time, self._sim_speed = self._controller.get_simulated_start_time()

        start_datetime = datetime.fromtimestamp(self._wattson_time)
        self._default_logger.info(f"Simulation started at {start_datetime}")
        
        """

        for script in self._scripts:
            script.setup(self)
        super().start()

    def add_action(self, script: Optional['TimedScript'], at: Union[datetime, str, float, int], action: Callable,
                   logger: Optional = None, apply_sim_offset: bool = True):
        """
        Queues an action to run at the specified time.

        Args:
            script (Optional['TimedScript']):
                The script that adds this action. Required for relative timings. None defaults all timings to the Simulated Start Time
            at (Union[datetime, str, float, int]):
                Timing information. Either a datetime (in simulated time!), a str representing simulated time in Y-m-d H:M:S format, or a relative
                float specifying the simulated time offset from the script's start.
            action (Callable):
                The action to trigger
            logger (Optional, optional):
                An optional logger to pass to the action. Uses a global logger if no specific one is provided.
                (Default value = None)
            apply_sim_offset (bool, optional):
                Whether the timing should be offset with respect to the simulated time
                (Default value = True)
        """
        ref_time = time.time()
        if apply_sim_offset:
            ref_time = self._wattson_time.sim_start_time()
        if script is not None:
            ref_time_script = script.get_simulated_time_info()
            if ref_time_script is not None:
                if type(ref_time_script) == float:
                    ref_time = ref_time + ref_time_script
                else:
                    ref_time = ref_time_script.timestamp()

        if logger is None:
            logger = self._loggers.get(self._scope, self._default_logger)
        at_timestamp = None

        if type(at) == float or type(at) == int:
            at_timestamp = ref_time + at
        elif type(at) == str:
            at = datetime.strptime(at, "%Y-%m-%d %H:%M:%S")

        if type(at) == datetime:
            at_timestamp = at.timestamp()

        if at_timestamp is None:
            self._default_logger.error("Action could not be scheduled due to invalid timing information")
            return
        # Scale to speed
        if apply_sim_offset:
            offset = self._controller.get_current_simulated_time()
            in_sim_seconds = at_timestamp - offset
            in_seconds = in_sim_seconds / self._wattson_time.speed
        else:
            in_seconds = at_timestamp - ref_time
            in_sim_seconds = in_seconds

        if in_sim_seconds < 0:
            in_seconds = 0
            self._default_logger.warning("Action has been scheduled to the past! Will be executed immediately")

        self._default_logger.debug(f"Scheduling action for in {in_seconds}s. Simulated Timestamp: {datetime.fromtimestamp(at_timestamp).isoformat()}")
        self._pending_actions.append({
            "time": in_seconds,
            "action": action,
            "scope": self._scope,
            "logger": logger
        })

    def begin_scope(self, scope: str, logger: Optional = None):
        self._scope = scope
        if logger is not None:
            self.set_scope_logger(logger)

    def set_scope_logger(self, logger):
        self._loggers[self._scope] = logger

    def end_scope(self):
        self._scope = "default"

    def run(self) -> None:
        self._default_logger.info("TimedRunner Started")
        # TODO: FIX THE TIME NOTION!!!
        self._start_time = time.time()
        while len(self._pending_actions) > 0:
            actions, next_action = self._get_actions()
            s = time.time()
            for action in actions:
                logger = action.get("logger", self._default_logger)
                action["action"](self._controller, logger=logger)
            sleep = next_action - (time.time() - s)
            self._default_logger.info(f"Sleeping {sleep}s until next action")
            time.sleep(max(0, sleep))
        self._default_logger.info("TimedRunner completed")

    def _get_actions(self):
        past_actions = []
        future_actions = []
        next_action = None
        offset = time.time() - self._start_time
        self._default_logger.info(f"We are at {offset}s runtime")
        for action in self._pending_actions:
            if action["time"] <= offset:
                past_actions.append(action)
            else:
                if next_action is None or action["time"] < next_action:
                    next_action = action["time"]
                future_actions.append(action)
        self._pending_actions = future_actions
        if next_action is None:
            next_action = 0
        next_action = max(next_action - offset, 0)
        return past_actions, next_action
