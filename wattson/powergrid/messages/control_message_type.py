import enum


class ControlMessageType(enum.Enum):
    start = 0
    stop = 1  # future work: pause, resume, and further like time manipulation
    update = 2  # Power Flow Computation finished with changes
    simtime = 3
