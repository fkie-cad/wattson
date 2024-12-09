import enum


class WattsonWeatherQueryType(str, enum.Enum):
    GET_WEATHER = "get-weather"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
