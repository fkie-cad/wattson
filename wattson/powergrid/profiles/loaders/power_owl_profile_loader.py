import datetime
import json
from pathlib import Path
from typing import Union, Optional

import pandas as pd
import yaml
from powerowl.layers.powergrid import PowerGridModel

from wattson.powergrid.profiles.loaders.power_profile_loader import PowerProfileLoader


class PowerOwlProfileLoader(PowerProfileLoader):
    profile_cache = {}

    def __init__(
        self,
        grid_model: PowerGridModel,
        profiles: dict,
        seed: int = 0,
        noise: str = "0",
        interpolate: Union[bool, str] = False,
        logger=None,
        step_size_sec: int = 300,
        step_interpolation_type: str = "linear",
        base_dir: Optional[Path] = None,
        path=None,
        domain=None,
    ):
        super().__init__(
            grid_model,
            profiles,
            seed,
            noise,
            interpolate,
            logger,
            step_size_sec,
            step_interpolation_type,
            base_dir,
        )
        self.folder = path
        self.domain = domain
        self.files = []
        if self.domain in ["load", "storage"]:
            self.files = [self.folder.joinpath(f"{self.domain}.json")]
        elif self.domain in ["sgen", "gen"]:
            self.files = [self.folder.joinpath(f"renewables.json"), self.folder.joinpath(f"powerplants.json")]

    def load_profile(self):
        normalized_profiles = {}
        for file in self.files:
            if not file.exists():
                self.logger.error(f"Could not find file: {file}")
                continue
            with file.open("r") as f:
                self.logger.debug(f"Loading {self.domain} profile from {file.absolute().__str__()}")
                raw_profiles = json.load(f)
                normalized_profiles.update(self._normalize_profiles(raw_profiles))
        return normalized_profiles

    def _normalize_profiles(self, profiles: dict):
        normalized = {}

        for day_string, data in profiles.items():
            for time_string, profile in data.items():
                for full_profile_name, value in profile.items():
                    dimension = self._default_dimension
                    profile_name = full_profile_name
                    if "_pload" in full_profile_name:
                        profile_name = full_profile_name.replace("_pload", "")
                        dimension = "active_power"
                    elif "_qload" in full_profile_name:
                        profile_name = full_profile_name.replace("_qload", "")
                        dimension = "reactive_power"
                    normalized.setdefault(profile_name, {}).setdefault(day_string, {}).setdefault(time_string, {})[dimension] = value

        # Ensure 24:00:00 value
        # Use a leapyear
        reference_date_time = datetime.datetime.strptime("2016-01-01", "%Y-%m-%d")
        for col_name, days in normalized.items():
            for day, entries in days.items():
                entry_date = datetime.datetime.strptime(day + "-2016", self.date_format + "-%Y")
                if "24:00:00" not in entries:
                    reference_date_time = reference_date_time.replace(day=entry_date.day, month=entry_date.month)
                    n_day = reference_date_time + datetime.timedelta(days=1)
                    n_day_key = n_day.strftime(self.date_format)
                    value = days[n_day_key]["00:00:00"]
                    entries["24:00:00"] = value
        return normalized
