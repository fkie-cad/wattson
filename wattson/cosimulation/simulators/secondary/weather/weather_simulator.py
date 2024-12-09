from pathlib import Path
from typing import Optional, Union, Type, Set

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.simulators.secondary.weather.messages.wattson_weather_query import WattsonWeatherQuery
from wattson.cosimulation.simulators.secondary.weather.weather import Weather
from wattson.cosimulation.simulators.simulator import Simulator
from wattson.time import WattsonTime, WattsonTimeType


class WeatherSimulator(Simulator):
    def __init__(self, config):
        super().__init__()
        self.config = {}
        self.config.update(config)
        self._wattson_time: Optional[WattsonTime] = None
        self._weather: Optional[Weather] = None

    @classmethod
    def get_simulator_type(cls) -> str:
        return "weather"

    def start(self):
        pass

    def stop(self):
        pass

    def load_scenario(self, scenario_path: Path):
        pass

    def get_simulation_control_clients(self) -> Set[str]:
        return set()

    def get_weather(self) -> Weather:
        if self._weather is None:
            self._weather = Weather(self, WattsonTimeType.SIM)
        return self._weather

    def get_wattson_time(self) -> Optional[WattsonTime]:
        if self._wattson_time is None:
            wattson_client = WattsonClient()
            self._wattson_time = wattson_client.get_wattson_time(enable_synchronization=True)
        return self._wattson_time

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        return isinstance(query, WattsonWeatherQuery)

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        if not self.handles_simulation_query_type(query):
            return None

        return None
