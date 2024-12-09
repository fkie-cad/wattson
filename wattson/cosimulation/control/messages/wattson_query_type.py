import enum


class WattsonQueryType(str, enum.Enum):
    REGISTRATION = "registration"
    ECHO = "echo"
    GET_TIME = "get-time"
    SET_TIME = "set-time"
    GET_EVENT_STATE = "get-event-state"
    SET_EVENT = "set-event"
    CLEAR_EVENT = "clear-event"
    GET_NOTIFICATION_HISTORY = "get-notification-history"
    SEND_NOTIFICATION = "send-notification"

    SET_CONFIGURATION = "set-configuration"
    GET_CONFIGURATION = "get-configuration"
    RESOLVE_CONFIGURATION = "resolve-configuration"

    GET_MODELS = "get-models"

    HAS_SIMULATOR = "has-simulator"
    GET_SIMULATORS = "get-simulators"

    REQUEST_SHUTDOWN = "request-shutdown"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
