import numpy as np

from wattson.hosts.rtu.logic.attack_logics.attack_functions.function import Function


class QuadraticToLinearFunction(Function):
    def __init__(self, options):
        super().__init__(options)

    def handles_value_type(self, value_type):
        return value_type in [float, int]

    def apply(self):
        if self.handles_value_type(type(self.options["reference_value"])):
            for interval in self.intervals:
                start, end = interval
                duration = float(end) - float(start)
                speed = self.options["speed"]
                a = speed / (60 * duration)
                if end == "inf":
                    self.logger.warning("Inf not allowed as end of interval for QuadraticToLinearFunction.")
                    continue
                temp = self.h(self.diff_since_start, a, duration)
                return self._apply_direction(self.options["reference_value"], temp)
        return self.options["reference_value"]

    def g(self, x, a):
        return a/2 * x**2

    def gd(self, x, a, d):
        return a*x if x<=d else a*d

    def h(self, x, a, d):
        return self.g(x, a) if x<=d else self.g(d, a) + self.gd(d, a, d) * (x-d)