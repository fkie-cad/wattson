import json
from pathlib import Path
from typing import Union, Optional, Type

from powerowl.layers.powergrid import PowerGridModel
from wattson.powergrid.profiles.loaders.power_owl_profile_loader import PowerOwlProfileLoader
from wattson.powergrid.profiles.loaders.power_profile_loader import PowerProfileLoader

from wattson.powergrid.profiles.loaders.seasoned_profile_loader import SeasonedProfileLoader
from wattson.powergrid.profiles.loaders.simbench_profile_loader import SimbenchProfileLoader


class ProfileLoaderFactory:
    @staticmethod
    def load_profiles(
        grid_model: PowerGridModel,
        profiles: dict,
        seed: int = 0,
        noise: str = "0",
        interpolate: Union[bool, str] = False,
        logger=None,
        step_size_sec: int = 300,
        step_interpolation_type: str = "linear",
        base_dir: Optional[Path] = None,
        scenario_path: Optional[Path] = None,
    ):
        normalized_profiles = {}
        for profile_name, profile in profiles.items():
            if profile is None or profile is False:
                continue
            provider_class: Optional[Type[PowerProfileLoader]] = None
            kwargs = {}
            if profile == "default":
                provider_class = SeasonedProfileLoader
                kwargs["key"] = profile_name
            elif profile == "simbench":
                provider_class = SimbenchProfileLoader
            elif profile.startswith("scenario."):
                provider_class = PowerOwlProfileLoader
                kwargs["path"] = scenario_path.joinpath("profiles")
            provider = provider_class(
                grid_model=grid_model,
                profiles=profiles,
                seed=seed,
                noise=noise,
                interpolate=interpolate,
                step_size_sec=step_size_sec,
                step_interpolation_type=step_interpolation_type,
                base_dir=base_dir,
                logger=logger,
                domain=profile_name,
                **kwargs
            )
            normalized_profiles[profile_name] = provider.load_profile()
        return normalized_profiles

