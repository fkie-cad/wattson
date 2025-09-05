import datetime
import json
import numbers
from pathlib import Path
from typing import Optional, Union

from powerowl.layers.powergrid import PowerGridModel

from wattson.powergrid.profiles.loaders.power_profile_loader import PowerProfileLoader


class SeasonedProfileLoader(PowerProfileLoader):
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
            file=None,
            key=None,
            domain=None
    ):
        super(SeasonedProfileLoader, self).__init__(
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
        self.file = file
        if self.file is None:
            self.file = Path(__file__).parent.parent.joinpath('default_profiles/load.json')
        self.key = key

    def load_profile(self):
        with self.file.open("r") as f:
            self.logger.debug(f"Loading {self.key} profile from {self.file.absolute().__str__()}")
            profile_meta = json.load(f)
            if self.key in profile_meta:
                profiles = self._normalize_json_profiles(profile_meta[self.key])
                self.logger.debug(f"{list(profiles.keys())} profiles loaded")
                return profiles
            else:
                self.logger.error(f"Cannot find {self.key} profile in file")

    def _normalize_json_profiles(self, profiles: dict):
        # Detect format and rewrite scheme to day_wise
        profile_format = get_profile_format(profiles)
        if profile_format == "seasons":
            return self._normalize_season_profiles(profiles)
        elif profile_format == "day_wise":
            return self._normalize_day_wise_profiles(profiles)
        else:
            raise ValueError(f"Unknown Profile Format: {profile_format}")

    def _get_seasoned_values_for_day(self, profile: dict, date_time: datetime.datetime):
        data = self._get_day_values(date_time, profile)
        return data

    def _get_day_values(
            self, date_time: datetime.datetime, profile: dict, rec: bool = True
    ) -> dict:
        day_cls = _get_day_class(date_time)
        season_weights = _get_season_weights(date_time)
        day_values = _calculate_day_values(profile, day_cls, season_weights)
        if rec:
            next_day_values = self._get_day_values(
                date_time + datetime.timedelta(days=1), profile, rec=False
            )
            if "00:00:00" not in next_day_values:
                raise ValueError(f"Value for 00:00:00 is mandatory!")
            day_values["24:00:00"] = next_day_values["00:00:00"]
        return day_values

    def _normalize_season_profiles(self, profiles: dict):
        normalized = {}
        for element_class, profile in profiles.items():
            normalized[element_class] = {}
            extrema = self._get_extrema(profile)
            if extrema is None:
                error = f"Could not identify extremum value for class '{element_class}'"
                self.logger.warning(error)
                raise ValueError(error)
            # Scale all values
            normalized = self.scale_all_values(normalized, profile, element_class, extrema)
        year = 2016  # Make sure to use a leap year
        current_date = datetime.datetime(year=year, month=1, day=1)
        end_date = datetime.datetime(year=year, month=12, day=31)
        day_wise = {}
        # print(json.dumps(normalized, indent=4))
        while current_date <= end_date:
            day = current_date.strftime(self.date_format)
            for element_class, profile in normalized.items():
                day_wise.setdefault(element_class, {})[
                    day
                ] = self._get_seasoned_values_for_day(profile, current_date)
            current_date = current_date + datetime.timedelta(days=1)
        return day_wise

    def scale_all_values(self, normalized, profile, element_class, extrema):
        for day_time, days in profile.items():
            normalized[element_class][day_time] = {}
            for weekday, seasons in days.items():
                normalized[element_class][day_time][weekday] = {}
                for season, value in seasons.items():
                    scale = 0 if extrema == 0 else 1 / extrema
                    if type(value) == dict:
                        for key in list(value.keys()):
                            value[key] = value[key] * scale
                    else:
                        value = {self._default_dimension: value * scale}
                    normalized[element_class][day_time][weekday][season] = value
        return normalized

    def _normalize_day_wise_profiles(self, profiles: dict):
        normalized = {}
        for element_class, profile in profiles.items():
            normalized[element_class] = {}
            for date_time_str, value in profile.items():
                date_time = datetime.datetime.strptime(
                    date_time_str, self.simbench_date_format
                )
                date = date_time.strftime(self.date_format)
                if date not in normalized[element_class]:
                    normalized[element_class][date] = {}
                time_str = date_time.strftime(self.time_format)
                normalized[element_class][date][time_str] = value
        return normalized

    def _get_extrema(self, element, default=None):
        if isinstance(element, dict):
            maxima = []
            for _, v in element.items():
                rec = self._get_extrema(v, None)
                if rec is not None:
                    maxima.append(rec)
            if len(maxima) > 0:
                return max(maxima)
            return default
        elif isinstance(element, list):
            return max([abs(x) for x in element])
        elif isinstance(element, numbers.Number):
            return abs(element)
        elif isinstance(element, str):
            if element.isnumeric():
                return abs(float(element))
            return default


def _get_day_class(date_time: datetime.datetime) -> str:
    """
    For a given date, determines the day class "weekday", "saturday" or "sunday"

    Args:
        date_time (datetime.datetime):
            The datetime object to determine the day class for

    Returns:
        str: A string representing the day's class
    """
    day = date_time.weekday()
    if day == 6:
        return "sunday"
    if day == 5:
        return "saturday"
    return "weekday"


def get_profile_format(profiles) -> str:
    for element_class, profile in profiles.items():
        keys = list(profile.keys())
        if len(keys) > 0:
            if ":" not in keys[0]:
                return "day_wise"
    return "seasons"


def _get_season_weights(date_time: datetime.datetime) -> dict:
    """
    For a given date, determines how much influence every of the 4 seasons has on this date.
    Each season is 3 months, where Winter starts at december 1st.
    Hence, at most two seasons can influence every single date.
    Time of the day is ignored during the weighting.

    Args:
        date_time (datetime.datetime):
            The datetime to determine the season weights for

    Returns:
        dict: A dict mapping each season to its normalized weight
    """
    dt = datetime.datetime
    year = date_time.year
    seasons = [
        {"name": "autumn", "start": dt(year - 1, 9, 1)},
        {"name": "winter", "start": dt(year - 1, 12, 1)},
        {"name": "spring", "start": dt(year, 3, 1)},
        {"name": "summer", "start": dt(year, 6, 1)},
        {"name": "autumn", "start": dt(year, 9, 1)},
        {"name": "winter", "start": dt(year, 12, 1)},
        {"name": "spring", "start": dt(year + 1, 3, 1)},
        {"name": "summer", "start": dt(year + 1, 6, 1)},
    ]
    weights = {"winter": 0, "spring": 0, "summer": 0, "autumn": 0}

    def _get_season_mid(_seasons, _i) -> datetime.datetime:
        _s = _seasons[_i]
        _n_season = _seasons[_i + 1]
        _season_start = _s["start"]
        _season_end = _n_season["start"]
        _season_delta = _season_end - _season_start
        _season_mid = _season_start + _season_delta / 2
        return _season_mid

    for j, season in enumerate(seasons[1:6]):
        i = j + 1
        n_season = seasons[i + 1]
        season_start = season["start"]
        season_end = n_season["start"]
        if season_start <= date_time < season_end:
            mid = _get_season_mid(seasons, i)
            if date_time == mid:
                weights[season["name"]] = 1
                break

            if date_time < mid:
                o_name = seasons[i - 1]["name"]
                o_mid = _get_season_mid(seasons, i - 1)
            else:
                o_name = seasons[i + 1]["name"]
                o_mid = _get_season_mid(seasons, i + 1)
            dist = abs((date_time - mid).days)
            dist_2 = abs((date_time - o_mid).days)
            day_sum = dist + dist_2
            weights[season["name"]] = (day_sum - dist) / day_sum
            weights[o_name] = (day_sum - dist_2) / day_sum
            break

    return weights


def _calculate_day_values(profile, day_cls, season_weights):
    day_values = {}
    for value_time, weekdays in profile.items():
        date_values = weekdays[day_cls]
        values = {}
        for season, season_val in date_values.items():
            for dimension in season_val.keys():
                values.setdefault(dimension, 0)
                values[dimension] += season_weights[season] * season_val[dimension]
        day_values[value_time] = values
    return day_values
