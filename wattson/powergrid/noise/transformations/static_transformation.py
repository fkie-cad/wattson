from typing import Any, Optional

from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.powergrid.noise.transformations.noise import Noise
from wattson.powergrid.noise.transformations.transformation import Transformation


class StaticTransformation(Transformation):
    def __init__(self, grid_value: GridValue, static_value: Any, noise: Optional[Noise]):
        super().__init__(grid_value, static_value, noise)

    def apply(self, iteration: int, value: Any):
        return self.transform_value
