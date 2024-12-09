import time
from typing import Optional, TYPE_CHECKING

from wattson.time import WattsonTimeType, WattsonTime

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.secondary.weather.weather_simulator import WeatherSimulator


class Weather:
    def __init__(self, weather_simulator: 'WeatherSimulator', wattson_time_type: WattsonTimeType):
        self.weather_simulator = weather_simulator
        self.wattson_time = self.weather_simulator.get_wattson_time()
        self.wattson_time_type = wattson_time_type

    @classmethod
    def get_simulator_type(cls) -> str:
        return "weather"

    def _get_timestamp(self, wattson_time: Optional[WattsonTime] = None) -> float:
        if wattson_time is None:
            wattson_time = self.weather_simulator.get_wattson_time()
            if wattson_time is None:
                return time.time()
        return wattson_time.time(self.wattson_time_type)

    """
    WIND
    """
    def get_wind_percentage(self, wattson_time: Optional[WattsonTime] = None) -> float:
        return self.get_wind_kilometers_per_hour(wattson_time=wattson_time) / self.get_wind_maximum_kilometers_per_hour()

    def get_wind_kilometers_per_hour(self, wattson_time: Optional[WattsonTime] = None) -> float:
        timestamp = self._get_timestamp(wattson_time)
        pass

    def get_wind_maximum_kilometers_per_hour(self) -> float:
        pass

    """
    SUN
    """
    def get_sun_intensity(self, wattson_time: Optional[WattsonTime] = None) -> float:
        pass

    """
    TEMPERATURE
    """
    def get_temperature_degree_celsius(self, wattson_time: Optional[WattsonTime] = None) -> float:
        pass

    """
    CLOUDS
    """
    def get_cloud_intensity(self, wattson_time: Optional[WattsonTime] = None) -> float:
        pass

    """
    DOWNFALL
    """