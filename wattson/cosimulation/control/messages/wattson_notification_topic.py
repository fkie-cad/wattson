import enum


class WattsonNotificationTopic(str, enum.Enum):
    SIMULATION_START = "simulation-start"
    REGISTRATION = "registration"

    EVENTS = "events"
    ASYNC_QUERY_RESOLVE = "async-query-resolve"

    WATTSON_TIME = "wattson-time"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, WattsonNotificationTopic):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
