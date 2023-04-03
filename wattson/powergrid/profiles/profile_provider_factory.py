from pathlib import Path
from typing import Union, Optional

import pandapower

from wattson.powergrid.profiles.profile_provider_interface import PowerProfileProviderInterface


class ProfileProviderFactory:
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
        self.interface = None
        self.power_grid = power_grid
        self.profiles = profiles
        self.seed = seed
        self.noise = noise
        self.interpolate = interpolate
        self.step_size_sec = step_size_sec
        self.step_interpolation_type = step_interpolation_type
        self.base_dir = base_dir
        self.logger = logger

    def get_interface(self) -> PowerProfileProviderInterface:
        if self.interface:
            return self.interface
        self.interface = PowerProfileProviderInterface(
            power_grid=self.power_grid,
            profiles=self.profiles,
            seed=self.seed,
            noise=self.noise,
            interpolate=self.interpolate,
            step_size_sec=self.step_size_sec,
            step_interpolation_type=self.step_interpolation_type,
            base_dir=self.base_dir,
            logger=self.logger,
        )
        return self.interface

