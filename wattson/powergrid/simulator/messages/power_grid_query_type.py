import enum


class PowerGridQueryType(str, enum.Enum):
    SUBSCRIBE_ELEMENT_UPDATE = "subscribe-element-update"
    MEASUREMENT = "measurement"
    CONTROL = "control"

    GET_GRID_VALUE = "get-grid-value"
    GET_GRID_VALUE_VALUE = "get-grid-value-value"
    SET_GRID_VALUE = "set-grid-value"
    SET_GRID_VALUE_SIMPLE = "set-grid-value-simple"
    SET_GRID_VALUE_STATE = "set-grid-value-state"
    GET_GRID_REPRESENTATION = "get-grid-representation"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
