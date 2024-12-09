import time
from pathlib import Path
from typing import Any

import yaml

from powerowl.layers.powergrid.elements import Bus, Line, Switch
from powerowl.layers.powergrid.values.grid_value import GridValue
from powerowl.layers.powergrid.values.grid_value_context import GridValueContext
from wattson.datapoints.providers.power_grid_provider import PowerGridProvider
from wattson.hosts.rtu.rtu_logic import RTULogic




class CircuitBreakerSafetyLogic(RTULogic):
    def __init__(self, rtu: 'RTU', **kwargs):
        super().__init__(rtu, **kwargs)
        self.config = yaml.load(Path(__file__).with_name(self.config_file).open("r"), yaml.Loader)
        self.start_delay = self.config["start_delay"]
        self._safety_enabled = self.config["safety_enabled"]
        self._bus_constraints = {}
        self._switch_constraints = {}
        self._pandapower_provider = None
        self.grid_element_identifier_to_identifier = {}
        self.identifier_to_grid_element_identifier = {}
        self.tracked_measurement_identifiers = []
        self.line_to_switch = {}
        self.grid_element_to_switches = {}
        self.grid_element_to_lines = {}
        self.grid_element_to_busses = {}
        self.switch_id_to_switch = {}
        self.switch_to_line = {}

    def on_start(self):
        super().on_start()
        time.sleep(self.start_delay)
        power_grid_provider: PowerGridProvider = self.rtu.manager.get_provider("POWER_GRID")

        for identifier, data in self.rtu.manager.data_points.items():
            for provider in data["providers"]:
                for entry in data["providers"][provider]:
                    context = entry["provider_data"]["context"]
                    grid_element = entry["provider_data"]["grid_element"]
                    element_type, id = grid_element.split(".")
                    attribute = entry["provider_data"]["attribute"]
                    if element_type == "bus":
                        bus = power_grid_provider.remote_power_grid_model.get_element_by_identifier(
                            grid_element)
                        self.grid_element_to_busses[grid_element] = bus
                        self.grid_element_identifier_to_identifier[f"{grid_element}.{context}.{attribute}"] = identifier
                    elif element_type == "switch":
                        if grid_element not in self._safety_enabled:
                            continue
                        switch = power_grid_provider.remote_power_grid_model.get_element_by_identifier(grid_element)
                        line = switch.get_property_value("element")
                        self.grid_element_to_lines[line.get_identifier()] = line
                        self.grid_element_to_switches[grid_element] = switch
                        self.line_to_switch[line.get_identifier()] = grid_element
                        self.switch_to_line[grid_element] = line.get_identifier()
                        self.switch_id_to_switch[grid_element] = switch
                        line_identifier = self._get_line_identifier_for_line_object(line)
                        if not line_identifier:
                            self.logger.info("Could not get line identifier.")
                            continue
                        self.grid_element_identifier_to_identifier[
                            f"{line.get_identifier()}.MEASUREMENT.current"] = line_identifier

        self.identifier_to_grid_element_identifier = {
            v: k for k, v in self.grid_element_identifier_to_identifier.items()
        }
        self._add_tracked_identifiers_for_busses(self.grid_element_to_busses)
        self._add_tracked_identifiers_for_lines(self.grid_element_to_lines)

        self.logger.info("Tracked identifiers:")
        for identifier in self.tracked_measurement_identifiers:
            self.logger.info(f"{identifier} -> {self.identifier_to_grid_element_identifier[identifier]}")

        if len(self._switch_constraints) == 0:
            self.logger.info(f"No switches controlled by this RTU - stopping logic")
            return

    def _get_line_identifier_for_line_object(self, line):
        for identifier, data in self.rtu.manager.data_points.items():
            for provider in data["providers"]:
                for entry in data["providers"][provider]:
                    context = entry["provider_data"]["context"]
                    grid_element = entry["provider_data"]["grid_element"]
                    attribute = entry["provider_data"]["attribute"]
                    if grid_element == line.get_identifier() and context == "MEASUREMENT" and attribute == "current":
                        return identifier

    def _add_tracked_identifiers_for_busses(self, grid_element_to_busses):
        for grid_element, element in grid_element_to_busses.items():
            element: Bus
            maximum_voltage = element.get_property_value("maximum_voltage")
            minimum_voltage = element.get_property_value("minimum_voltage")
            self.tracked_measurement_identifiers.append(
                self.grid_element_identifier_to_identifier[f"{grid_element}.MEASUREMENT.voltage"])
            self._bus_constraints[grid_element] = {"max": maximum_voltage, "min": minimum_voltage}

    def _add_tracked_identifiers_for_lines(self, grid_element_to_lines):
        for grid_element, line in grid_element_to_lines.items():
            max_current = line.get_property_value("maximum_current")
            self.tracked_measurement_identifiers.append(
                self.grid_element_identifier_to_identifier[f"{line.get_identifier()}.MEASUREMENT.current"])
            self._switch_constraints[self.line_to_switch[grid_element]] = max_current

    def on_stop(self):
        super().on_stop()

    def handles_get_value(self, identifier) -> bool:
        if identifier in self.tracked_measurement_identifiers:
            return True
        return False

    def handle_get_value(self, identifier) -> Any:
        grid_element_identifier = self.identifier_to_grid_element_identifier[identifier]
        value = self.rtu.manager.get_value(identifier)
        self.logger.info(f"{grid_element_identifier} has {value=}.")
        element_type, element_id, context, attribute = grid_element_identifier.split(".")
        if element_type == "bus":
            constraints = self._bus_constraints[f"{element_type}.{element_id}"]
            if value > constraints["max"]:
                self.logger.info("We are above the max allowed value.")
                self._handle_bus_overvoltage(f"{element_type}.{element_id}")
            if value < constraints["min"]:
                self.logger.info("We are below the min allowed value.")
                self._handle_bus_under_voltage(f"{element_type}.{element_id}")
        elif element_type == "line":
            constraints = self._switch_constraints[self.line_to_switch[f"{element_type}.{element_id}"]]
            if value > constraints:
                self.logger.info("We are above the max allowed value.")
                self._open_switch(self.line_to_switch[f"{element_type}.{element_id}"])
        return self.rtu.manager.get_value(identifier)

    def _handle_bus_under_voltage(self, bus):
        mode = self.config["modes"]["under_voltage"]
        if mode == "all":
            for switch_id in self.switch_id_to_switch.keys():
                self._open_switch(switch_id)
        elif mode == "lowest":
            switch = self._find_switch_with_lowest_current_for_bus(bus)
            self._open_switch(switch)

    def _find_switch_with_lowest_current_for_bus(self, bus):
        switch_with_lowest_current = None
        lowest_current = 0
        for switch_id, switch in self.switch_id_to_switch.items():
            switch: Switch
            self.logger.info(switch.get_measurement_value("is_closed"))
            if not switch.get_measurement_value("is_closed"):
                continue
            line: Line = self.grid_element_to_lines[self.switch_to_line[switch_id]]
            from_bus = line.get_property_value("from_bus")
            to_bus = line.get_property_value("to_bus")
            if bus == from_bus.get_identifier():
                current_to = line.get_measurement_value("current_to")
                if current_to < lowest_current:
                    lowest_current = current_to
                    switch_with_lowest_current = switch_id
            elif bus == to_bus.get_identifier():
                current_from = line.get_measurement_value("current_from")
                if current_from < lowest_current:
                    lowest_current = current_from
                    switch_with_lowest_current = switch_id
            else:
                self.logger.error("Bus is not one of the busses from line?")
        self.logger.info(f"Returning {switch_with_lowest_current} with {lowest_current=}.")
        return switch_with_lowest_current

    def _handle_bus_overvoltage(self, bus):
        mode = self.config["modes"]["over_voltage"]
        if mode == "all":
            for switch_id in self.switch_id_to_switch.keys():
                self._open_switch(switch_id)
        elif mode == "highest":
            switch = self._find_switch_with_highest_current_for_bus(bus)
            self._open_switch(switch)

    def _find_switch_with_highest_current_for_bus(self, bus):
        switch_with_highest_current = None
        highest_current = 0
        for switch_id, switch in self.switch_id_to_switch.items():
            switch: Switch
            self.logger.info(switch.get_measurement_value("is_closed"))
            if not switch.get_measurement_value("is_closed"):
                continue
            line: Line = self.grid_element_to_lines[self.switch_to_line[switch_id]]
            from_bus: Bus = line.get_property_value("from_bus")
            to_bus: Bus = line.get_property_value("to_bus")
            if bus == from_bus.get_identifier():
                current_from = line.get_measurement_value("current_from")
                if current_from > highest_current:
                    highest_current = current_from
                    switch_with_highest_current = switch_id
            elif bus == to_bus.get_identifier():
                current_to = line.get_measurement_value("current_to")
                if current_to > highest_current:
                    highest_current = current_to
                    switch_with_highest_current = switch_id
            else:
                self.logger.error("Bus is not one of the busses from line?")
        self.logger.info(f"Returning {switch_with_highest_current} with {highest_current=}.")
        return switch_with_highest_current

    def _open_switch(self, switch_id):
        if switch_id not in self.switch_id_to_switch:
            return
        switch = self.switch_id_to_switch[switch_id]
        grid_value: GridValue = switch.get_config("closed")
        grid_value.set_value(False)
        switch.set("closed", GridValueContext.CONFIGURATION, grid_value)
        self.logger.info(f"Opened switch {switch_id}.")
