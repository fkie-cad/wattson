import numpy as np
from numpy import sin

from wattson.hosts.rtu.logic.attack_logics.attack_functions.function import Function


class SineFunction(Function):
    def __init__(self, options):
        super().__init__(options)

    def handles_value_type(self, value_type):
        return value_type in [float, int]

    def apply(self):
        if self.handles_value_type(type(self.options["reference_value"])):
            for interval in self.intervals:
                start, end = interval
                if end == "inf":
                    end = np.infty
                if float(start) < self.diff_since_start < float(end):
                    return sin(self.diff_since_start * self.options["frequency"]) * self.options["scale"] + self.options["shift"] + self.options["reference_value"]
        return self.options["reference_value"]
