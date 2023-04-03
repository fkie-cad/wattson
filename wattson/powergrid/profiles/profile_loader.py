"""
Module for loading profiles of grid devices. The profiles are given as time
series: for each specified device the value per time step is given.
The time series must have a specified format.
Each time series is given as csv where the columns are the different devices
and the rows are the time steps. The column lables are the indices of the devices
in the dataframe within the pandapower net. The filename has following format:
XXX-element_type-column.csv where XXX has no meaning, element type denotes the
target element type (e.g., load, sgen) and column the target column (e.g., p_mw,
q_mvar). The values of the profile are absolut values that are directly sent
to the power simulator.
"""
import datetime
import logging
import threading
import time
from pathlib import Path
from typing import Union, Optional, Dict

import pandapower

from wattson.powergrid.client.coordination_client import CoordinationClient
from wattson.powergrid.common.events import PROFILES_READY
from wattson.powergrid.messages.request_response_message import RequestResponseMessage
from wattson.powergrid.profiles.profile_provider_factory import ProfileProviderFactory
from wattson.util import get_logger


# noinspection PyMethodMayBeStatic
class ProfileLoader(threading.Thread):
    def __init__(self, coord_addr: str, power_grid: pandapower.pandapowerNet,
                 profiles: dict, seed: int = 0, noise: str = "0", interval: float = 1,
                 interpolate: Union[bool, str] = False, speed: float = 1.0, stop: Union[Dict, float, int, bool] = False,
                 start_datetime: Union[bool, str] = False,
                 profile_path: Optional[str] = None,
                 profile_dir: str = "default_profiles"):

        super().__init__()

        self.logger = get_logger("TimeSeries", "Wattson.Profiles")
        self.logger.setLevel(logging.DEBUG)

        self.power_grid = power_grid
        self._simulated_time_diverges = False
        if not start_datetime:
            self.start_datetime = datetime.datetime.now()
        else:
            self.start_datetime = datetime.datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
            self._simulated_time_diverges = True

        self._simulated_time_diverges |= speed != 1
        self.speed = speed
        self._start_time = time.time()
        self.sim_time = self.start_datetime
        self.start_timestamp = time.time()

        self.stop_config = {
            "sgen": False,
            "load": False
        }
        self._step = 0
        if type(stop) in [int, float]:
            self.stop_config["sgen"] = stop
            self.stop_config["load"] = stop
        elif isinstance(stop, dict):
            self.stop_config["sgen"] = stop.get("sgen", False)
            self.stop_config["load"] = stop.get("load", False)

        self.logger.info(f"Stop Config: {repr(self.stop_config)}")

        if not interpolate:
            interpolate_parts = [False]
            step_size_sec = 300
            step_type = False
        else:
            interpolate_parts = interpolate.split(".")
            step_size_sec = int(interpolate_parts[2]) if len(interpolate_parts) == 3 else 300
            step_type = interpolate_parts[1] if len(interpolate_parts) == 3 else "linear"

        if profile_path is None:
            base_path = Path(__file__).parent
        else:
            base_path = Path(profile_path)
        base_path = base_path.joinpath(profile_dir)

        self._profile_provider_factory = ProfileProviderFactory(power_grid=power_grid, profiles=profiles, seed=seed,
                                                                noise=noise, interpolate=interpolate_parts[0],
                                                                step_size_sec=step_size_sec, step_interpolation_type=step_type,
                                                                base_dir=base_path)
        self._profile_provider = self._profile_provider_factory.get_interface()
        #self._profile_provider = PowerProfileProviderInterface(power_grid=power_grid, profiles=profiles, seed=seed,
        #                                                       noise=noise, interpolate=interpolate_parts[0],
        #                                                       step_size_sec=step_size_sec, step_interpolation_type=step_type,
        #                                                       base_dir=base_path)

        self.interval = interval
        self._terminate = threading.Event()
        self.coord_client = CoordinationClient(coord_addr)

    def start(self):
        self.coord_client.start()
        if self._simulated_time_diverges:# and False:
            self.coord_client.get_response(RequestResponseMessage(request={
                "type": "SET_SIMULATED_TIME_INFO",
                "start_time": self.start_datetime.timestamp(),
                "speed": self.speed
            }))
        super().start()

    def stop(self):
        self._terminate.set()

    def run(self):
        first_run = True
        while not self._terminate.is_set():
            self._step += 1
            self.logger.debug(f"Step: {self._step}")
            start_time = time.time()
            # Forward Time according to speed and start date
            self._advance_time()
            self.logger.debug(f"Simulation Time: {self.sim_time.strftime('%Y-%m-%d %H:%M:%S')}")
            # Update values of loads and generators
            for element_type in ["load", "sgen"]:
                stop_step = self.stop_config.get(element_type, False)
                if stop_step is True:
                    continue
                elif stop_step is not False:
                    if type(stop_step) == float:
                        passed_time = self._get_sim_time_passed()
                        if stop_step < passed_time:
                            self.logger.info(f"Stopping profiles for {element_type} after {passed_time} seconds")
                            self.stop_config[element_type] = True
                    elif type(stop_step) == int:
                        if stop_step < self._step:
                            self.logger.info(f"Stopping profiles for {element_type} after {stop_step} steps")
                            self.stop_config[element_type] = True
                            continue

                for index, row in self.power_grid[element_type].iterrows():
                    value = self._profile_provider.get_value(element_type, index, self.sim_time)
                    if value is None:
                        continue
                    lw = False
                    if element_type == "sgen":
                        lw = True

                    self.coord_client.update_value(table=element_type,
                                                   column="p_mw",
                                                   index=index,
                                                   value=str(value),
                                                   log_worthy=lw)

            if first_run:
                self.coord_client.trigger_event(PROFILES_READY)
            end_time = time.time()
            runtime = end_time - start_time
            diff = max(0.0, self.interval - runtime)
            first_run = False
            time.sleep(diff)

    def _get_sim_time_passed(self):
        real_time_passed = time.time() - self.start_timestamp
        sim_time_passed = real_time_passed * self.speed
        return sim_time_passed

    def _advance_time(self):
        sim_time_passed = self._get_sim_time_passed()
        self.sim_time = self.start_datetime + datetime.timedelta(seconds=sim_time_passed)
