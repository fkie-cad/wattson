import numpy as np

from wattson.hosts.rtu.logic.attack_logics.attack_functions.function import Function


class QuadraticFunction(Function):
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
                    duration = end - start
                    a = self.options["speed"] / (60 * duration)
                    quadratic_change = a / 2 * self.diff_since_start ** 2
                    return self._apply_direction(self.options["reference_value"], quadratic_change)
        return self.options["reference_value"]
