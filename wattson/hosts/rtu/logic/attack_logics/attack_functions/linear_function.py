import numpy as np

from wattson.hosts.rtu.logic.attack_logics.attack_functions.function import Function


class LinearFunction(Function):
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
                    linear_change = (self.diff_since_start / 60) * self.options["speed"] * self.options["reference_value"]
                    return self._apply_direction(self.options["reference_value"], linear_change)
        return self.options["reference_value"]

