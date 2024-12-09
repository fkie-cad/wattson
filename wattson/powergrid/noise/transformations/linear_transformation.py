from typing import Any, Optional

from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.powergrid.noise.transformations.noise import Noise
from wattson.powergrid.noise.transformations.transformation import Transformation


class LinearTransformation(Transformation):
    def __init__(self, grid_value: GridValue, linear_offset: Any, noise: Optional[Noise]):
        super().__init__(grid_value, linear_offset, noise)

    def apply(self, iteration: int, value: Any):
        return value + self.transform_value
