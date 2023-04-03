import datetime
from pathlib import Path
from typing import Union, Optional

import pandapower
import pandas as pd

from wattson.powergrid.profiles.profile_provider import PowerProfileProvider


class SimbenchProfileProvider(PowerProfileProvider):
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
        file=None,
        key=None,
    ):
        super(SimbenchProfileProvider, self).__init__(
            power_grid,
            profiles,
            seed,
            noise,
            interpolate,
            logger,
            step_size_sec,
            step_interpolation_type,
            base_dir,
        )
        self.file = file
        self.key = key

    def load_profile(self):
        with self.file.open("r") as f:
            self.logger.info(
                f"Loading {self.key} profile from {self.file.absolute().__str__()}"
            )
            return self._normalize_df_profiles(
                pd.read_csv(f, sep=";", index_col="time")
            )

    def _normalize_df_profiles(self, df: pd.DataFrame):
        normalized = {}
        col_map = {}
        for col in list(df.columns):
            col_name: str = col
            dimension: str = self._default_dimension
            if "_pload" in col:
                dimension = "p"
                col_name = col_name.replace("_pload", "")
            if "_qload" in col:
                dimension = "q"
                col_name = col_name.replace("_qload", "")
            col_map[col] = {"dimension": dimension, "name": col_name}
            normalized[col_name] = {}
        # Translate
        date_time = None
        for index, row in df.iterrows():
            date_time = datetime.datetime.strptime(
                str(index), self.simbench_date_format
            )
            date_str = date_time.strftime(self.date_format)
            time_str = date_time.strftime(self.time_format)
            for col, col_info in col_map.items():
                col_name = col_info["name"]
                dimension = col_info["dimension"]
                value = row[col]
                normalized[col_name].setdefault(date_str, {}).setdefault(time_str, {})[
                    dimension
                ] = value
        # Ensure 24:00:00 value
        for col_name, days in normalized.items():
            for day, entries in days.items():
                date = datetime.datetime.strptime(
                    day + "-2016", self.date_format + "-%Y"
                )
                if "24:00:00" not in entries:
                    date_time = date_time.replace(day=date.day, month=date.month)
                    n_day = date_time + datetime.timedelta(days=1)
                    n_day_key = n_day.strftime(self.date_format)
                    value = days[n_day_key]["00:00:00"]
                    entries["24:00:00"] = value
        return normalized
