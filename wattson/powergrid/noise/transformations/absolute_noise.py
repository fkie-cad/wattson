from typing import Any

from wattson.powergrid.noise.transformations.noise import Noise
from wattson.util.random import Random


class AbsoluteNoise(Noise):
    """
    Applies noise to the given value as an absolut value.
    The value where the noise should be applied to is then modified to include
    a normal distributed absolute noise with the scale of the given absolute_scale.
    """
    def __init__(self, absolute_scale: float, hard_cap: bool = True):
        self._absolute_scale = abs(absolute_scale)
        self._hard_cap = hard_cap
        self._deviation_scale = 2

    def apply(self, value: Any, random_namespace: str = "default") -> Any:
        noised_value = Random.normal(value, self._absolute_scale / self._deviation_scale, ns=random_namespace)
        if self._hard_cap:
            noised_value = self.clamp(noised_value, value - self._absolute_scale, value + self._absolute_scale)
        return noised_value
