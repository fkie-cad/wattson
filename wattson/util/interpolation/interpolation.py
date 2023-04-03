import math
from typing import Union, Any

from scipy.interpolate import interp1d

from wattson.util.interpolation.historian import *


class Interpolation:
    def __init__(self, historian: 'Historian', interpolation_type: Union[bool, str] = False,
                 step_size: int = 100, step_interpolation_type: str = "cubic"):

        self._historian = historian
        self._interpolation_type = interpolation_type
        self._step_interpolation_type = step_interpolation_type
        self._step_size = step_size
        self._interpolate = None
        self._data = historian.get_data()

    @property
    def interpolation_type(self):
        return self._interpolation_type

    def interpolate(self, x: Any) -> Any:
        if self._interpolate is None:
            self._init_interpolation()
        return self._interpolate(x)

    def _init_interpolation(self):
        if self._interpolation_type is False:
            self._interpolate = self._no_interpolation()
            return

        if self._interpolation_type in ["cubic", "linear"]:
            self._interpolate = self._default_interpolation(kind=self._interpolation_type)
        elif self._interpolation_type == "steps":
            self._interpolate = self._step_interpolation(step_type=self._step_interpolation_type,
                                                         step_size=self._step_size)

    def _no_interpolation(self):
        def i(x: Any) -> Any:
            last_val = 0
            for x_ref, y in self._data.items():
                if x_ref > x:
                    break
                else:
                    last_val = y
            return last_val

        return i

    def _default_interpolation(self, kind: str):
        x, y = [], []
        for x_hist, y_hist in self._data.items():
            if x_hist not in x:
                x.append(x_hist)
                y.append(y_hist)
        f = interp1d(x, y, kind=kind, fill_value="extrapolate")

        def i(_x: Any) -> Any:
            return f(_x)

        return i

    def _step_interpolation(self, step_type="linear", step_size=100):
        linear = self._default_interpolation(kind=step_type)

        def i(_x: Any) -> Any:
            target = math.floor(_x / step_size) * step_size
            return linear(target)

        return i
