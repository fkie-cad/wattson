import logging
from typing import Optional, Dict, Any

from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from powerowl.layers.powergrid.values.units.parser import Parser
from powerowl.layers.powergrid.values.units.unit import Unit

from wattson.powergrid.noise.transformations.absolute_noise import AbsoluteNoise
from wattson.powergrid.noise.transformations.linear_transformation import LinearTransformation
from wattson.powergrid.noise.transformations.percentage_noise import PercentageNoise
from wattson.powergrid.noise.transformations.product_transformation import ProductTransformation
from wattson.powergrid.noise.transformations.transformation import Transformation


class NoiseManager:
    def __init__(self, power_grid_model: Optional[PowerGridModel], logger: logging.Logger):
        self._grid_model = power_grid_model
        self.logger = logger
        self._static_pre_sim_noise = None
        self._static_post_sim_noise = None
        self._static_measurement_noise = None

        self._pre_sim_noise_rules = {}
        self._post_sim_noise_rules = {}
        self._measurement_noise_rules = {}

    def set_power_grid_model(self, power_grid_model: PowerGridModel):
        self._grid_model = power_grid_model
        self.reset_to_static()

    """
    APPLY NOISE
    """
    def pre_sim_noise(self, simulation_iteration: int, grid_value: GridValue, original_value: Any):
        return self._apply_transformation(self._pre_sim_noise_rules, simulation_iteration, grid_value, original_value)

    def post_sim_noise(self, simulation_iteration: int, grid_value: GridValue, original_value: Any):
        return self._apply_transformation(self._post_sim_noise_rules, simulation_iteration, grid_value, original_value)

    def measurement_noise(self, simulation_iteration: int, grid_value: GridValue, original_value: Any):
        return self._apply_transformation(self._measurement_noise_rules, simulation_iteration, grid_value, original_value)

    def _apply_transformation(self, rule_set: Dict, simulation_iteration: int, grid_value: GridValue, original_value: Any):
        transformation: Transformation = rule_set.get(grid_value.get_identifier())
        if transformation is None:
            return original_value
        transformed_value = transformation.apply(simulation_iteration, original_value)
        # self.logger.info(f"[{simulation_iteration}] Applied {transformation.transformation_type} transformation ({transformation.__class__.__name__}) to "
        #                  f"{grid_value.get_identifier()}: {original_value} -> {transformed_value}")
        return transformed_value

    """
    STATIC CONFIGURATION
    """
    def set_static_noise(self, static_pre_sim_noise: Optional[Dict] = None, static_post_sim_noise: Optional[Dict] = None,
                         static_measurement_noise: Optional[Dict] = None):
        self._static_pre_sim_noise = static_pre_sim_noise
        self._static_post_sim_noise = static_post_sim_noise
        self._static_measurement_noise = static_measurement_noise
        self.reset_to_static()

    def clear(self, include_pre_sim_noise: bool = True, include_post_sim_noise: bool = True, include_measurement_noise: bool = True):
        if include_pre_sim_noise:
            self._pre_sim_noise_rules = {}
        if include_post_sim_noise:
            self._post_sim_noise_rules = {}
        if include_measurement_noise:
            self._measurement_noise_rules = {}

    def reset_to_static(self, include_pre_sim_noise: bool = True, include_post_sim_noise: bool = True, include_measurement_noise: bool = True):
        if include_pre_sim_noise:
            self._pre_sim_noise_rules = self._create_rules(GridValueContext.CONFIGURATION, self._static_pre_sim_noise, "pre_sim")
        if include_post_sim_noise:
            self._post_sim_noise_rules = self._create_rules(GridValueContext.MEASUREMENT, self._static_post_sim_noise, "post_sim")
        if include_measurement_noise:
            self._measurement_noise_rules = self._create_rules(GridValueContext.MEASUREMENT, self._static_measurement_noise, "measurement")

    """
    MANUAL RULES
    """
    def create_pre_sim_transformation(self, grid_value: GridValue, specification: str):
        transformation = self._generate_transformation(grid_value, specification)
        if transformation is not None:
            transformation.transformation_type = "pre_sim"
            self._pre_sim_noise_rules[grid_value.get_identifier()] = transformation

    def create_post_sim_transformation(self, grid_value: GridValue, specification: str):
        transformation = self._generate_transformation(grid_value, specification)
        if transformation is not None:
            transformation.transformation_type = "post_sim"
            self._post_sim_noise_rules[grid_value.get_identifier()] = transformation

    def create_measurement_transformation(self, grid_value: GridValue, specification: str):
        transformation = self._generate_transformation(grid_value, specification)
        if transformation is not None:
            transformation.transformation_type = "measurement"
            self._measurement_noise_rules[grid_value.get_identifier()] = transformation

    """
    RULE GENERATION
    """
    def _create_rules(self, grid_value_context: GridValueContext, config: Optional[Dict], transformation_type: str = "generic") -> Dict[str, Transformation]:
        rules = {}
        if config is None or not isinstance(config, dict):
            return rules
        for element_type, element_config in config.items():
            for element in self._grid_model.get_elements_by_type(element_type):
                for _, grid_value in element.get_grid_values(grid_value_context):
                    for key, specification in element_config.items():
                        if self._grid_value_matches_key(grid_value, key):
                            rule = self._generate_transformation(grid_value, specification)
                            if rule is not None:
                                rule.transformation_type = transformation_type
                                rules[grid_value.get_identifier()] = rule
                            break
        return rules

    def _generate_transformation(self, grid_value: GridValue, rule_specification: str) -> Optional[Transformation]:
        try:
            value, scale, unit = Parser.parse(rule_specification)
            if unit == Unit.NONE:
                self.logger.error(f"No unit found for rule specification: {rule_specification} ({grid_value.get_identifier()})")
                return None
            if unit == Unit.PERCENT:
                # Noisy percentage offset
                value = scale.to_base(value)
                return ProductTransformation(grid_value=grid_value, factor=1, noise=PercentageNoise(percentage=value))
            else:
                # Noisy linear offset
                value = scale.to_scale(value, grid_value.scale)
                return LinearTransformation(grid_value=grid_value, linear_offset=0, noise=AbsoluteNoise(absolute_scale=value))
        except Exception as e:
            self.logger.error(f"Invalid noise rule specification for {grid_value.get_identifier()}: {e=}")
        return None

    @staticmethod
    def _grid_value_matches_key(grid_value: GridValue, key: str):
        return key == grid_value.name or grid_value.name.startswith(key) or f"_{key}" in grid_value.name
