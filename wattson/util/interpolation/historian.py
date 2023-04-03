import copy

from wattson.util.interpolation.interpolation import *


class Historian:
    def __init__(self):
        self._history = dict()
        self._sorted = False
        self._interpolations = {}

    def store(self, x, value):
        self._history[x] = value
        self._sorted = False
        self._interpolations = {}

    def get_data(self):
        self.sort()
        return copy.deepcopy(self._history)

    def get_latest_value(self):
        self.sort()
        if len(self._history) == 0:
            return None
        key = list(self._history.keys())[-1]
        return self._history.get(key)

    def sort(self):
        if not self._sorted:
            self._history = dict(sorted(self._history.items()))
            self._sorted = True

    def interpolate(self, x, interpolation_type="cubic", default_value=None):
        if len(self._history) == 0:
            return default_value
        if len(self._history) == 1:
            return self.get_latest_value()
        if interpolation_type not in self._interpolations:
            self._interpolations[interpolation_type] = Interpolation(historian=self,
                                                                     interpolation_type=interpolation_type)
        return self._interpolations[interpolation_type].interpolate(x)
