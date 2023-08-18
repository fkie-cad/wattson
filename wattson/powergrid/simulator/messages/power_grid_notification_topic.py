import enum


class PowerGridNotificationTopic(str, enum.Enum):
    ELEMENT_UPDATED = "element-updated"
    GRID_VALUE_CHANGED = "grid-value-changed"
    SIMULATION_STEP_DONE = "simulation-step-done"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
