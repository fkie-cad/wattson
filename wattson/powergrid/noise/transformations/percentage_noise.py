from typing import Any

from wattson.powergrid.noise.transformations.noise import Noise
from wattson.util.random import Random


class PercentageNoise(Noise):
    """
    Applies noise to the given value as a percentage.
    The value is scaled by the given scale with a randomized value resulting from a normal distribution.

    """
    def __init__(self, percentage: float, hard_cap: bool = True):
        self._percentage = percentage
        self._hard_cap = hard_cap
        self._deviation_scale = 2

    def apply(self, value: Any, random_namespace: str = "default") -> Any:
        scale = (self._percentage / 100) * value
        noised_value = Random.normal(value, scale / self._deviation_scale, ns=random_namespace)
        if self._hard_cap:
            noised_value = self.clamp(noised_value, value - scale, value + scale)
        return noised_value
