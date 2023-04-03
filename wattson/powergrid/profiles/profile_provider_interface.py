import datetime
from pathlib import Path
from typing import Optional, Union, Tuple

import numpy as np
import pandapower
import pandas as pd

from wattson.powergrid.profiles.interpolation import Interpolation
from wattson.powergrid.profiles.seasoned_profile_provider import SeasonedProfileProvider
from wattson.powergrid.profiles.simbench_profile_provider import SimbenchProfileProvider
from wattson.util import get_logger, translate_value


class PowerProfileProviderInterface:
    def __init__(
        self,
        power_grid: pandapower.pandapowerNet,
        profiles: dict,
        seed: int = 0,
        noise: str = "0",
        interpolate: Union[bool, str] = False,
        logger=None,
        step_size_sec: int = 300,
        step_interpolation_type: str = "linear",
        base_dir: Optional[Path] = None,
    ):

        self.logger = (
            logger.getChild("Provider")
            if logger is not None
            else get_logger("PowerProfileProvider", "PowerProfileProvider")
        )

        self._default_classes = {
            "load": {
                "res": "Haushalt",
                "ci": "Gewerbe allgemein",
                "_default": "Haushalt",
            },
            "sgen": {"_default": "PV"},
        }

        self._base_dir = base_dir
        if self._base_dir is None:
            self._base_dir = Path(__file__).parent.joinpath("default_profiles")
        self.profiles = profiles
        self.power_grid = power_grid
        self.seed = seed
        self.noise = noise
        if not interpolate:
            self.interpolate = False
            self.step_size_sec = False
            self.step_interpolation_type = False
        else:
            self.interpolate = interpolate
            self.step_size_sec = step_size_sec
            self.step_interpolation_type = step_interpolation_type

        self.date_format = "%m-%d"

        self._base_values = {}
        self._interpolation_cache = {}
        self._load_profiles()
        self._store_base_values()
        np.random.seed(self.seed)

    def is_key_and_value_invalid(self, key, value):
        if key not in self._profiles:
            self.logger.info(f"Profiles for type {key} not (yet) supported")
            return True
        if value is None or value is False:
            self.logger.info(f"Disabling profiles for type {key}")
            return True
        return False

    def _load_profiles(self):
        # TODO: More components?
        self._profiles = {"load": None, "sgen": None}

        for key, value in self.profiles.items():
            if self.is_key_and_value_invalid(key, value):
                continue

            file = self.get_file(key, value)
            if not file.exists():
                self.logger.warning(
                    f"Profile path not found: {file.absolute().__str__()}"
                )
                continue

            self.logger.info(f"Loading {key} profile from {file.absolute().__str__()}")
            if file.suffix == ".json":
                provider = SeasonedProfileProvider(
                    self.power_grid,
                    self.profiles,
                    self.seed,
                    self.noise,
                    self.interpolate,
                    self.logger,
                    self.step_size_sec,
                    self.step_interpolation_type,
                    self._base_dir,
                    file,
                    key,
                )
                self._profiles[key] = provider.load_profile()
            elif file.suffix == ".csv":
                provider = SimbenchProfileProvider(
                    self.power_grid,
                    self.profiles,
                    self.seed,
                    self.noise,
                    self.interpolate,
                    self.logger,
                    self.step_size_sec,
                    self.step_interpolation_type,
                    self._base_dir,
                    file,
                    key,
                )
                self._profiles[key] = provider.load_profile()
            else:
                self.logger.error(f"File {file.name} is not supported")

    def get_value(
        self, element_type, element_index, date_time, dimension: str = "p"
    ) -> Optional[float]:
        element_profile = self._default_classes[element_type]["_default"]
        if "profile" in self.power_grid[element_type]:
            element_profile = self.power_grid[element_type].at[element_index, "profile"]

        if element_type not in self._profiles or self._profiles[element_type] is None:
            return None
        profile = self._profiles[element_type].get(element_profile)
        if profile is None:
            return None
        value = None
        try:
            value = self._get_weighted_value(
                date_time, profile, dimension, (element_type, element_profile)
            )
            value = self._scale_value(element_type, element_index, value)
            value = self._add_noise(element_type, element_profile, value)
        finally:
            return value

    def _scale_value(self, element_type, element_index, value, dimension: str = "p"):
        # Scale the value according to the element's specification (i.e., max power)
        ref_value = self._base_values[element_type][element_index][dimension]
        return ref_value * value

    def _add_noise(self, element_type, element_class, value):
        if isinstance(self.noise, dict):
            noise = self.noise.get(element_type)
            if isinstance(noise, dict):
                noise = noise.get(element_class)
        else:
            noise = self.noise

        if isinstance(noise, str) and len(noise) > 1:
            if noise[-1] == "%":
                percentage = float(noise[:-1])
                noise = (percentage / 100) * value
            else:
                value = translate_value(value, "p_mw")
        else:
            noise = 0

        if noise is not None:
            return np.random.normal(value, noise)
        return value

    def _get_weighted_value(
        self,
        date_time: datetime.datetime,
        profile: dict,
        dimension: str,
        cache_key: Tuple,
    ) -> float:
        day_str = date_time.strftime(self.date_format)
        data = profile[day_str]
        interpolation = self._get_interpolation(
            data, date_time, dimension, cache_key + (day_str, dimension)
        )
        return interpolation.interpolate(date_time)

    def _get_interpolation(
        self, data: dict, date_time: datetime.datetime, dimension: str, cache_key: Tuple
    ) -> Interpolation:
        if cache_key in self._interpolation_cache:
            return self._interpolation_cache[cache_key]

        interpolation = Interpolation(
            date_time=date_time,
            logger=self.logger,
            data=data,
            dimension=dimension,
            interpolation_type=self.interpolate
            if self.interpolate in [False, "cubic", "linear", "steps"]
            else False,
            step_size_sec=self.step_size_sec,
            step_interpolation_type=self.step_interpolation_type,
        )
        self._interpolation_cache[cache_key] = interpolation
        return interpolation

    def _store_base_values(self):
        # Store base power values of all elements
        for element_type in ["load", "sgen"]:
            self._base_values[element_type] = {}
            for index, row in self.power_grid[element_type].iterrows():
                self._base_values[element_type][index] = {
                    "p": get_max_p_mw(row),
                    "q": get_max_q_mvar(row),
                }

    def get_file(self, key, value) -> Path:
        if value == "default":
            return self._base_dir.joinpath(f"{key}.json")
        elif value == "simbench":
            if key == "load":
                return self._base_dir.joinpath("LoadProfile.csv")
            elif key == "sgen":
                return self._base_dir.joinpath("RESProfile.csv")
            else:
                return self._base_dir.joinpath(key + ".csv")
        else:
            return Path(value)


def get_max_p_mw(row):
    if max_p_mw_exists(row):
        return row["max_p_mw"]
    if col_exists_and_filled(row, "p_installed"):
        return row["p_installed"]
    if col_exists_and_filled(row, "sn_mva"):
        return row["sn_mva"]
    return row["p_mw"]


def get_max_q_mvar(row):
    if max_p_mw_exists(row):
        return row["max_q_mvar"]
    if col_exists_and_filled(row, "q_installed"):
        return row["q_installed"]
    if col_exists_and_filled(row, "sn_mva"):
        return row["sn_mva"]
    return row["q_mvar"]


def max_q_mvar_exists(row):
    return col_exists_and_filled(row, "max_q_mvar")


def col_exists_and_filled(row, col) -> bool:
    return (
        col in row
        and not pd.isnull(row[col])
        and not np.isnan(row[col])
    )


def max_p_mw_exists(row):
    return col_exists_and_filled(row, "max_p_mw")
