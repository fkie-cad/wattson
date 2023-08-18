import datetime
import time

from wattson.apps.script_controller.interface import SoloScript
from time import sleep

from wattson.util import get_logger
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp


class LaborScript(SoloScript):
    def __init__(self, controller: 'ScriptControllerApp'):
        super().__init__(controller)
        self.times = [
            350,
            365,
            460,
            475,
            605,  # Attack #1
            750,  # Attack #2
            880   # Shutdown
        ]
        # Shift execution for x seconds
        self.offset = 0
        self.times = [x+self.offset for x in self.times]

        # Whether to also simulate the attackers
        self.include_attacks = True

        self.step = 0
        self.start_timestamp = 0
        self.start_datetime: datetime.datetime = datetime.datetime.now()
        self.logger = None

    def run(self):
        logger = self.controller.logger.getChild("LaborScript")
        self.logger = logger

        logger.info("Running Labor Script")
        while not self.controller.coord_client.is_sim_running():
            sleep(1)

        self.start_timestamp = self.controller.coord_client.get_sim_start_time()
        self.start_datetime = datetime.datetime.fromtimestamp(self.start_timestamp)

        self.logger.info(f"Simulation started at {self.start_datetime.isoformat(sep=' ')}")

        self._sleep_next()
        # Set Active Power to 30%
        logger.info("Setting Power of Fronius CL (COA 4) to 30%")
        self.controller.set_data_point(coa=4, ioa=204983, value=30)
        self.controller.set_data_point(coa=4, ioa=204986, value=True)


        self._sleep_next()
        # Set Active Power to 50%
        logger.info("Setting Power of Fronius CL (COA 4) to 50%")
        self.controller.set_data_point(coa=4, ioa=204983, value=50)
        self.controller.set_data_point(coa=4, ioa=204986, value=True)

        self._sleep_next()
        # Set Sunny Island to Discharging with 21%
        logger.info("Setting Sunny Island to -3kW (COA 2)")
        self.controller.set_data_point(coa=2, ioa=197825, value=-3000)

        self._sleep_next()
        # Set Sunny Island to Discharging with 42%
        logger.info("Setting Sunny Island to -6kW (COA 2)")
        self.controller.set_data_point(coa=2, ioa=197825, value=-6000)

        if self.include_attacks:
            self._run_attacks()
        else:
            self._skip_attacks()

        self._sleep_next()
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
