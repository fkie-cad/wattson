import datetime
import json
import math
import sys
from typing import Union

from scipy.interpolate import interp1d


class Interpolation:
    def __init__(self, date_time: datetime.datetime, logger, data: dict, dimension: str,
                 interpolation_type: Union[bool, str] = False,
                 step_size_sec: int = 300, step_interpolation_type: str = "linear"):

        self.base_time = date_time
        self._dimension = dimension
        self.day_start = self.base_time.replace(hour=0, minute=0, second=0, microsecond=0)
        self._original_data = data
        self.data = self._convert_data()
        self.data_list = self._data_to_sorted_list()
        self._interpolation_type = interpolation_type
        self._step_interpolation_type = step_interpolation_type
        self._step_size_sec = step_size_sec
        self._interpolate = None
        self._logger = logger

    def interpolate(self, date_time: datetime.datetime) -> float:
        if self._interpolate is None:
            self._init_interpolation()
        return self._interpolate(date_time)

    def _convert_data(self) -> dict:
        d = {}
        for t, val in self._original_data.items():
            items = t.split(":")
            if len(items) != 3:
                self._logger.error(f"Invalid profile time {t} does not follow H:m:s scheme")
                continue
            items = [int(x) for x in items]
            point_time = self.day_start + datetime.timedelta(hours=items[0], minutes=items[1], seconds=items[2])
            d[point_time] = val[self._dimension]
        return d

    def _data_to_sorted_list(self) -> list:
        data_list = []
        for dt, val in self.data.items():
            data_list.append({
                "ts": dt.timestamp(),
                "value": val
            })
        return sorted(data_list, key=lambda x: x["ts"])

    def _init_interpolation(self):
        if self._interpolation_type is False:
            self._interpolate = self._no_interpolation()
            return

        if self._interpolation_type in ["cubic", "linear"]:
            self._interpolate = self._default_interpolation(kind=self._interpolation_type)
        elif self._interpolation_type == "steps":
            self._interpolate = self._step_interpolation(step_type=self._step_interpolation_type,
                                                         step_size_sec=self._step_size_sec)

    def _no_interpolation(self):
        def i(d: datetime.datetime) -> float:
            last_val = 0
            ts = d.timestamp()
            for entry in self.data_list:
                if entry["ts"] > ts:
                    break
                else:
                    last_val = entry["value"]
            return last_val

        return i

    def _default_interpolation(self, kind: str):
        x, y = [], []
        for entry in self.data_list:
            # Account for daylight savings...
            if entry["ts"] not in x:
                x.append(entry["ts"])
                y.append(entry["value"])
        f = interp1d(x, y, kind=kind)

        def i(d: datetime.datetime) -> float:
            ts = d.timestamp()
            return f(ts)

        return i

    def _step_interpolation(self, step_type="linear", step_size_sec=300):
        linear = self._default_interpolation(kind=step_type)

        def i(d: datetime.datetime) -> float:
            ts = d.timestamp()
            target = math.floor(ts / step_size_sec) * step_size_sec
            target_dt = datetime.datetime.fromtimestamp(target)
            return linear(target_dt)

        return i
