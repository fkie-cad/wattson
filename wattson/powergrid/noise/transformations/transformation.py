from typing import Any, Optional

from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.powergrid.noise.transformations.noise import Noise


class Transformation:
    def __init__(self, grid_value: GridValue, transform_value: Any, noise: Optional[Noise] = None):
        self._grid_value = grid_value
        self._transform_value = transform_value
        self._noise = noise
        self.transformation_type = "generic"

    @property
    def transform_value(self):
        if self._noise is None:
            return self._transform_value
        return self._noise.apply(self._transform_value)

    def apply(self, iteration: int, value: Any):
        return value

    def matches(self, grid_value: GridValue):
        return self._grid_value.get_identifier() == grid_value.get_identifier()
