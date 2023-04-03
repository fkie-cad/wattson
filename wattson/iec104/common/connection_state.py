import enum


class ConnectionState(enum.Enum):
    CLOSED = 0
    OPEN = 1
    INTERRO_STARTED = 2
    INTERRO_DONE = 3
    UNKNOWN = 4
    UNATTEMPTED_TO_CONNECT = 5

    @property
    def name(self):
        names = {
            ConnectionState.CLOSED: "CLOSED",
            ConnectionState.OPEN: "OPEN",
            ConnectionState.INTERRO_STARTED: "INTERRO_STARTED",
            ConnectionState.INTERRO_DONE: "INTERRO_DONE",
            ConnectionState.UNKNOWN: "UNKNOWN",
            ConnectionState.UNATTEMPTED_TO_CONNECT: "NO_CONNECTION_ATTEMPTED"
        }
        return names[self]

    @staticmethod
    def from_name(name):
        decoder = {
            "CLOSED": ConnectionState.CLOSED,
            "OPEN": ConnectionState.OPEN,
            "INTERRO_STARTED": ConnectionState.INTERRO_STARTED,
            "INTERRO_DONE": ConnectionState.INTERRO_DONE,
            "UNKNOWN": ConnectionState.UNKNOWN,
        }
        return decoder[name]
