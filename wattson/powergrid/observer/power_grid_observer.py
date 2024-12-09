import logging
import threading
from typing import Set, Any, Optional

import numpy as np
from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext

from wattson.analysis.events.event_level import EventLevel
from wattson.analysis.events.event_observer import EventObserver
from wattson.util import get_logger


class PowerGridObserver(EventObserver):
    def __init__(self, power_grid_model: PowerGridModel, *,
                 auto_init_thresholds: bool = True,
                 auto_observe: bool = True,
                 logger: Optional[logging.Logger] = None,
                 preferred_value_context: GridValueContext = GridValueContext.MEASUREMENT,
                 allow_value_context_fallback: bool = True
                 ):
        super().__init__()
        self._power_grid_model = power_grid_model
        self.logger = logger or get_logger("PowerGridObserver")

        self._value_context = preferred_value_context
        self._allow_fallback = allow_value_context_fallback

        self._lock = threading.RLock()

        self.default_thresholds = {
            "bus_no_voltage": 0.3,
            "bus_low_voltage": 0.95,
            "bus_under_voltage": 0.90,
            "bus_high_voltage": 1.05,
            "bus_over_voltage": 1.1,

            "line_high_load_percentage": 90,
            "line_over_load_percentage": 100,
            "line_severe_over_load_percentage": 120,

            "transformer_high_load_percentage": 90,
            "transformer_over_load_percentage": 100,
            "transformer_severe_over_load_percentage": 120,
        }

        self._last_group_state = {}
        self._thresholds = {}
        self._observed_grid_values: Set[GridValue] = set()

        if auto_init_thresholds:
            self._initialize_default_thresholds()
        if auto_observe:
            self.observe()

    def set_allow_context_fallback(self, allow_fallback: bool):
        self._allow_fallback = allow_fallback

    def set_preferred_value_context(self, grid_value_context: GridValueContext):
        if grid_value_context not in [GridValueContext.MEASUREMENT, GridValueContext.ESTIMATION]:
            raise ValueError("Invalid GridValueContext for preferred context. Only MEASUREMENT and ESTIMATION allowed.")
        self._value_context = grid_value_context

    def _get_fallback_context(self):
        if self._value_context == GridValueContext.MEASUREMENT:
            return GridValueContext.ESTIMATION
        return GridValueContext.MEASUREMENT

    def get_element_value_with_fallback(self, element: GridElement, value_name: str, alternative_value_context: Optional[GridValueContext] = None):
        grid_value = self.get_element_grid_value_with_fallback(element, value_name, alternative_value_context)
        if grid_value is None:
            return None
        return grid_value.get_value()

    def get_element_grid_value_with_fallback(self, element: GridElement, value_name: str, alternative_value_context: Optional[GridValueContext] = None):
        if alternative_value_context is not None:
            # Force grid value context
            return element.get(value_name, alternative_value_context)
        # Get primary value
        try:
            grid_value = element.get(value_name, self._value_context)
            if self._allow_fallback:
                value = grid_value.get_value()
                if value is None or np.isnan(value):
                    # Attempt Fallback
                    try:
                        return element.get(value_name, self._get_fallback_context())
                    except KeyError:
                        return grid_value
            return grid_value
        except KeyError:
            # Fallback?
            if self._allow_fallback:
                try:
                    return element.get(value_name, self._get_fallback_context())
                except KeyError:
                    return None
        return None

    def observe(self):
        for grid_value in self._observed_grid_values:
            grid_value.add_on_set_callback(self._check_thresholds)
            self._check_thresholds(grid_value, None, grid_value.get_value())

    def _check_all_thresholds(self):
        for grid_value in self._observed_grid_values:
            self._check_thresholds(grid_value, None, grid_value.get_value())

    def _check_thresholds(self, grid_value: GridValue, old_value: Any, new_value: Any):
        with self._lock:
            grid_element = grid_value.get_grid_element()
            group_defaults = {}
            if grid_element.get_identifier() in self._thresholds:
                for threshold_definition in self._thresholds[grid_element.get_identifier()]:
                    context_data = {
                        "grid_element": grid_element,
                        "grid_value": grid_value,
                        "value": new_value
                    }

                    threshold_group = threshold_definition.get("group", "default")
                    group_defaults.setdefault(threshold_group, {
                        "level": None,
                        "group": threshold_group,
                        "scope": "power-grid",
                        "context": grid_element.get_identifier(),
                        "data": context_data
                    })

                    # Already matching threshold found
                    if group_defaults.get(threshold_group).get("key") is not None:
                        continue

                    # Threshold matches
                    if threshold_definition["threshold"](grid_element):
                        group_defaults[threshold_group] = {
                            "scope": "power-grid",
                            "context": grid_element.get_identifier(),
                            "data": context_data,
                            "key": threshold_definition["key"],
                            "group": threshold_group,
                            "name": threshold_definition["name"],
                            "description": f"{threshold_definition['name']} at {grid_element.get_identifier()}",
                            "level": threshold_definition["event_level"]
                        }

            for group, event in group_defaults.items():
                last_group_key = self._last_group_state.get(grid_element.get_identifier(), {}).get(group)
                self._last_group_state.setdefault(grid_element.get_identifier(), {})[group] = event.get("key", None)

                if event.get("key") is None:
                    if last_group_key is not None:
                        # Resolved
                        self.trigger("resolve", **event)
                elif event.get("key") != last_group_key:
                    # New state
                    if last_group_key is not None:
                        # Old state has been resolved / invalidated
                        self.trigger("invalidate", **event)
                    prefix = grid_element.prefix
                    self.trigger("change", **event)
                    self.trigger(f"{prefix}.{group}", **event)
                    self.trigger(f"{prefix}.{event['key']}", **event)

    def _initialize_default_thresholds(self):
        t = self.default_thresholds

        #
        # Buses
        #
        for bus in self._power_grid_model.get_buses():
            self._observed_grid_values.add(bus.get_measurement("voltage"))
            self._observed_grid_values.add(bus.get_estimation("voltage"))

            self._thresholds[bus.get_identifier()] = [
                {
                    "key": "no_voltage",
                    "group": "voltage",
                    "name": "Bus no voltage",
                    "event_level": EventLevel.ALERT,
                    "threshold": lambda b: np.isnan(self.get_element_value_with_fallback(b, "voltage"))
                },
                {
                    "key": "no_voltage",
                    "group": "voltage",
                    "name": "Bus no voltage",
                    "event_level": EventLevel.ALERT,
                    "threshold": lambda b: self.get_element_value_with_fallback(b, "voltage") <= t.get("bus_no_voltage")
                },
                {
                    "key": "under_voltage",
                    "group": "voltage",
                    "name": "Bus under voltage",
                    "event_level": EventLevel.SEVERE,
                    "threshold": lambda b: self.get_element_value_with_fallback(b, "voltage") <= t.get("bus_under_voltage")
                },
                {
                    "key": "low_voltage",
                    "group": "voltage",
                    "name": "Bus low voltage",
                    "event_level": EventLevel.WARNING,
                    "threshold": lambda b: t.get("bus_under_voltage") < self.get_element_value_with_fallback(b, "voltage") <= t.get("bus_low_voltage")
                },
                {
                    "key": "high_voltage",
                    "group": "voltage",
                    "name": "Bus high voltage",
                    "event_level": EventLevel.WARNING,
                    "threshold": lambda b: t.get("bus_high_voltage") <= self.get_element_value_with_fallback(b, "voltage") < t.get("bus_over_voltage")
                },
                {
                    "key": "over_voltage",
                    "group": "voltage",
                    "name": "Bus over voltage",
                    "event_level": EventLevel.SEVERE,
                    "threshold": lambda b: t.get("bus_over_voltage") <= self.get_element_value_with_fallback(b, "voltage")
                }
            ]

        #
        # Lines
        #
        for line in self._power_grid_model.get_lines():
            self._observed_grid_values.add(line.get_measurement("loading"))
            self._observed_grid_values.add(line.get_estimation("loading"))
            self._thresholds[line.get_identifier()] = [
                {
                    "key": "high_load",
                    "group": "loading",
                    "name": "Line high load",
                    "event_level": EventLevel.WARNING,
                    "threshold": lambda _line: t.get("line_high_load_percentage") < self.get_element_value_with_fallback(_line, "loading") <= t.get("line_over_load_percentage")
                },
                {
                    "key": "over_load",
                    "group": "loading",
                    "name": "Line over load",
                    "event_level": EventLevel.SEVERE,
                    "threshold": lambda _line: t.get("line_over_load_percentage") < self.get_element_value_with_fallback(_line, "loading") <= t.get("line_severe_over_load_percentage")
                },
                {
                    "key": "severe_over_load",
                    "group": "loading",
                    "name": "Line severe over load",
                    "event_level": EventLevel.ALERT,
                    "threshold": lambda _line: t.get("line_severe_over_load_percentage") < self.get_element_value_with_fallback(_line, "loading")
                }
            ]

        #
        # Transformers
        #
        for transformer in self._power_grid_model.get_elements_by_type("trafo"):
            self._observed_grid_values.add(transformer.get_measurement("loading"))
            self._observed_grid_values.add(transformer.get_estimation("loading"))
            self._thresholds[transformer.get_identifier()] = [
                {
                    "key": "high_load",
                    "group": "loading",
                    "name": "Transformer high load",
                    "event_level": EventLevel.WARNING,
                    "threshold": lambda _trafo: t.get("transformer_high_load_percentage") < self.get_element_value_with_fallback(_trafo, "loading") <= t.get("transformer_over_load_percentage")
                },
                {
                    "key": "over_load",
                    "group": "loading",
                    "name": "Transformer over load",
                    "event_level": EventLevel.SEVERE,
                    "threshold": lambda _trafo: t.get("transformer_over_load_percentage") < self.get_element_value_with_fallback(_trafo, "loading") <= t.get("transformer_severe_over_load_percentage")
                },
                {
                    "key": "severe_over_load",
                    "group": "loading",
                    "name": "Transformer severe over load",
                    "event_level": EventLevel.ALERT,
                    "threshold": lambda _trafo: t.get("transformer_severe_over_load_percentage") < self.get_element_value_with_fallback(_trafo, "loading")
                }
            ]
