"""
Module for loading profiles of grid devices. The profiles are given as time
series: for each specified device the value per time step is given.
The time series must have a specified format.
Each time series is given as csv where the columns are the different devices
and the rows are the time steps. The column lables are the indices of the devices
in the dataframe within the pandapower net. The filename has following format:
XXX-element_type-column.csv where XXX has no meaning, element type denotes the
target element type (e.g., load, static_generator) and column the target column (e.g., p_mw,
q_mvar). The values of the profile are absolut values that are directly sent
to the power simulator.
"""
import datetime
import json
import logging
import threading
import time
from pathlib import Path
from typing import Union, Optional, Dict, List, Callable

import pyprctl
from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import StaticGenerator, Load, Storage
from powerowl.layers.powergrid.elements.enums.static_generator_type import StaticGeneratorType
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.powergrid.profiles.profile_calculator import ProfileCalculator
from wattson.powergrid.profiles.profile_loader_factory import ProfileLoaderFactory, ProfileLoaderFactory
from wattson.time import WattsonTime, WattsonTimeType
from wattson.util import get_logger


# noinspection PyMethodMayBeStatic
class ProfileLoader(threading.Thread):
    def __init__(self, grid_model: 'PowerGridModel', apply_updates_callback: Optional[Callable[[List[Dict]], None]],
                 wattson_time: dict, profiles: dict, seed: int = 0, noise: str = "0", interval: float = 1,
                 interpolate: Union[bool, str] = False, stop: Union[Dict, float, int, bool] = False,
                 profile_path: Optional[str] = None, profile_dir: str = "default_profiles", scenario_path: Optional[Path] = None,
                 activate_none_profiles: bool = False):

        super().__init__()

        self._apply_updates_callback = apply_updates_callback

        self.logger = get_logger("WattsonProfiles")
        self.logger.setLevel(logging.INFO)

        self._wattson_time_config = wattson_time
        self._wattson_client: Optional[WattsonClient] = None
        self._wattson_time: Optional[WattsonTime] = None
        self._create_wattson_time()

        self.ready_event = threading.Event()

        self.grid_model = grid_model

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

        self.logger.debug(f"Stop Config: {repr(self.stop_config)}")

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

        normalized_profiles = ProfileLoaderFactory.load_profiles(
            grid_model=self.grid_model,
            profiles=profiles,
            seed=seed,
            noise=noise,
            interpolate=interpolate_parts[0],
            step_size_sec=step_size_sec,
            step_interpolation_type=step_type,
            base_dir=base_path,
            scenario_path=scenario_path
        )
        self.logger.info(f"Loaded Profiles")
        self._profile_calculator = ProfileCalculator(
            grid_model=self.grid_model,
            profiles=normalized_profiles,
            seed=seed,
            noise=noise,
            interpolate=interpolate_parts[0],
            step_size_sec=step_size_sec,
            step_interpolation_type=step_type,
            base_dir=base_path,
            activate_none_profiles=activate_none_profiles
        )
        self.logger.info(f"Created calculator")

        self.interval = interval
        self._terminate = threading.Event()

    def _create_wattson_time(self):
        wattson_time_mode = self._wattson_time_config.get("mode", "standalone")
        wattson_time_speed = self._wattson_time_config.get("speed", None)
        wattson_time_datetime = self._wattson_time_config.get("start_datetime", None)
        wattson_time_ref_wall = time.time()
        if wattson_time_datetime is None:
            wattson_time_ref_sim = None
        else:
            wattson_time_ref_sim = datetime.datetime.strptime(wattson_time_datetime, "%Y-%m-%d %H:%M:%S").timestamp()

        if wattson_time_mode == "standalone":
            if wattson_time_speed is None:
                wattson_time_speed = 1
            self._wattson_time = WattsonTime(
                wall_clock_reference=wattson_time_ref_wall,
                sim_clock_reference=wattson_time_ref_sim,
                speed=wattson_time_speed
            )
        else:
            self._wattson_client = WattsonClient(client_name="profile-loader", namespace="auto")
            self._wattson_client.start()
            self._wattson_client.require_connection()
            if wattson_time_mode in ["fork", "sync"]:
                if wattson_time_mode == "fork":
                    self._wattson_time = self._wattson_client.get_wattson_time(enable_synchronization=False)
                elif wattson_time_mode == "sync":
                    self._wattson_time = self._wattson_client.get_wattson_time(enable_synchronization=True)

                if wattson_time_speed is not None:
                    self._wattson_time.set_speed(wattson_time_speed)
                if wattson_time_ref_sim is not None:
                    self._wattson_time.set_sim_clock_reference(wattson_time_ref_sim)

    def start(self):
        # Enable profiles
        for element_type in ["sgen", "load", "storage"]:
            for grid_element in self.grid_model.get_elements_by_type(element_type):
                grid_element.get_config("profile_enabled").set_value(True)
        super().start()

    def stop(self):
        self._terminate.set()
        if self._wattson_client is not None:
            self._wattson_client.stop()

    def run(self):
        pyprctl.set_name("W/PG/Prof")
        first_run = True
        while not self._terminate.is_set():
            self._step += 1
            self.logger.debug(f"Step: {self._step}")
            start_time = time.time()
            # Forward Time according to speed and start date
            self.logger.debug(f"Simulation Time: {self._wattson_time.to_local_datetime(WattsonTimeType.SIM).strftime('%Y-%m-%d %H:%M:%S')}")
            # Update values of loads and generators

            updates = []

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

                for element in self.grid_model.get_elements_by_type(element_type):
                    for dimension in ["active_power", "reactive_power"]:
                        value = self._profile_calculator.get_value(element, self._wattson_time.to_local_datetime(WattsonTimeType.SIM), dimension=dimension)
                        if value is None:
                            continue
                        value_context = GridValueContext.CONFIGURATION
                        value_name = f"{dimension}_profile_percentage"
                        if isinstance(element, StaticGenerator):
                            # self.logger.info(f"{element.get_identifier()}: {dimension}={value}")
                            generator_type = element.get_property_value("generator_type").get_clear_type()
                            if generator_type in [StaticGeneratorType.PHOTOVOLTAIC, StaticGeneratorType.WIND]:
                                # value_context = GridValueContext.PROPERTY
                                value_name = f"{dimension}_limit"
                                # Apply scaling
                                max_power = None
                                if dimension == "active_power":
                                    max_power = element.get_maximum_active_power()
                                elif dimension == "reactive_power":
                                    max_power = element.get_maximum_reactive_power()
                                if max_power is None:
                                    self.logger.warning(f"Cannot update {element.get_identifier()} - no maximum active power found")
                                    continue
                                value = max_power * value
                            else:
                                value_name = f"{dimension}_profile_percentage"
                                value = 100 * value
                        elif isinstance(element, Load):
                            # self.logger.info(f"{element.get_identifier()}: {dimension}={value}")
                            value_name = f"{dimension}_profile_percentage"
                            value = 100 * value
                        elif isinstance(element, Storage):
                            # self.logger.info(f"{element.get_identifier()}: {dimension}={value}")
                            value_name = f"{dimension}_profile_percentage"
                            value = 100 * value
                            # Do not actually apply this
                            continue

                        updates.append(
                            {
                                "element": element,
                                "value_context": value_context,
                                "value_name": value_name,
                                "value": value
                            }
                        )

            self._apply_updates(updates)

            if first_run:
                self.logger.info(f"Initial profiles applied")
                self.ready_event.set()
            end_time = time.time()
            runtime = end_time - start_time
            diff = max(0.0, self.interval - runtime)
            first_run = False
            self._terminate.wait(diff)

    def _get_sim_time_passed(self):
        return self._wattson_time.passed_sim_clock_seconds()

    def _apply_updates(self, updates: List[Dict]):
        if self._apply_updates_callback is not None:
            self._apply_updates_callback(updates)
