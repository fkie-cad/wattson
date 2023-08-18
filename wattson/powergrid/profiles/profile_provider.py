from pathlib import Path
from typing import Optional, Union

import pandapower
from powerowl.layers.powergrid import PowerGridModel

from wattson.util import get_logger


class PowerProfileProvider:
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
        self.grid_model = grid_model
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
        self.time_format = "%H:%M:%S"
        self.simbench_date_format = "%d.%m.%Y %H:%M"
        self._default_dimension = "active_power"

        self._day_cache = {}
        self._base_values = {}
        self._interpolation_cache = {}
