import datetime
import time

from wattson.apps.script_controller.interface import SoloScript
from time import sleep

from wattson.util import get_logger
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp


class KonradsTestScript(SoloScript):
    def __init__(self, controller: 'ScriptControllerApp'):
        super().__init__(controller)
        self.times = [
            10,
            15,
            20,
            24,
            31,
            35,
            42,
            50
        ]
        self.actions = [
            "set 4 204983 30, set 4 204986 1",
            "get 4 204821",
            "get 4 204830",
            "set 4 204983 50, set 4 204986 1",
            "get 4 204830",
            "set 4 204983 100, set 4 204986 1",
            "get 4 204961",
            ""
        ]
        # Shift execution for x seconds
        self.offset = 0
        self.times = [x+self.offset for x in self.times]

        self.step = 0
        self.start_timestamp = 0
        self.start_datetime: datetime.datetime = datetime.datetime.now()
        self.logger = None

    def run(self):
        logger = self.controller.logger.getChild("KonradsTestScript")
        self.logger = logger

        logger.info("Running Konrads Test Script")
        while not self.controller.coord_client.is_sim_running():
            sleep(1)

        self.start_timestamp = self.controller.coord_client.get_sim_start_time()
        self.start_datetime = datetime.datetime.fromtimestamp(self.start_timestamp)

        self.logger.info(f"Simulation started at {self.start_datetime.isoformat(sep=' ')}")

        while self.step < len(self.times):
            self._sleep_next()
            self._apply_action()

        logger.info("Sending shutdown request")
        self.controller.coord_client.request_shutdown()

        logger.info("Script completed")

    def _run_attacks(self):
        logger = self.logger
        self.logger.info("Simulating attacks")

        self._sleep_next()
        logger.info("Setting Fronius CL to 100%")
        self.controller.set_data_point(coa=4, ioa=204983, value=100)
        self.controller.set_data_point(coa=4, ioa=204986, value=True)

        self._sleep_next()
        logger.info("Setting Sunny Island to 42% (Charging)")
        self.controller.set_data_point(coa=2, ioa=197825, value=6000)

    def _skip_attacks(self):
        self.logger.info("Skipping attacks (waiting 2 steps)")
        self._sleep_next()
        self._sleep_next()

    def _sleep_next(self):
        if self.step >= len(self.times):
            return False
        step_size = self.times[self.step]
        execute_at: datetime.datetime = self.start_datetime + datetime.timedelta(seconds=step_size)
        seconds_until = int((execute_at - datetime.datetime.now()).seconds)

        self.logger.info(f"Waiting {seconds_until} seconds (until {execute_at.isoformat(sep=' ')})")
        sleep(seconds_until)
        self.step += 1

    def _apply_action(self):
        s = self.step - 1
        if s >= len(self.actions):
            print("Invalid Actions")
            return False
        action = self.actions[s]
        actions = action.split(",")
        for a in actions:
            self._exec_action(a)

    def _exec_action(self, action):
        parts = action.split(" ")
        if parts[0] == "set" and len(parts) == 4:
            coa = int(parts[1])
            ioa = int(parts[2])
            val = float(parts[3])
            self.logger.info(f"Setting {coa}.{ioa} to {val}")
            self.controller.set_data_point(coa=coa, ioa=ioa, value=val)
        elif parts[0] == "get" and len(parts) == 3:
            coa = int(parts[1])
            ioa = int(parts[2])
            val = self.controller.get_data_point(coa=coa, ioa=ioa)
            self.logger.info(f"Read {coa}.{ioa} with value {val}")
