from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.simulators.secondary.weather.weather import Weather


class RemoteWeather(Weather, WattsonRemoteObject):
    def synchronize(self, force: bool = False, block: bool = True):
        pass

