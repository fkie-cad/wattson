from builtins import str

import networkx as nx
from wattson.datapoints.interface import DataPointValue
from wattson.powergrid.server.coord_logic_interface import CoordinatorLogicInterface
from wattson.powergrid.server.coord_server import CoordinationServer
from powerowl.power_owl import PowerOwl


class CircuitBreakerLogic(CoordinatorLogicInterface):
    def __init__(self, coordinator: 'CoordinationServer', args):
        super().__init__(coordinator, args)
        self.bus_index_to_thresholds = {}
        self.lines_to_thresholds = {}
        self.bus_index_to_switches = {}
        self.switch_index_to_info = {}
        self.bus_index_to_loads = {}
        self.bus_index_to_generators = {}
        self.owl = None


    def setup(self, net):
        self._extract_switch_data(net)
        self.owl = PowerOwl.from_pandapower(net)
        self.owl.derive_ous()
        self.owl.derive_network()
        self.owl.graph.layout()

    def post_sim_transform(self, net):
        self._extract_load_and_generator_data(net)
        success = self._handle_res_bus(net)
        if not success:
            return self._handle_res_line(net)
        return success

    def _extract_switch_data(self, net):
        for index, row in net["switch"].iterrows():
            bus = row["bus"]
            element_type = row["et"]
            element = row["element"]
            element_table = {
                "b": "res_bus",
                "l": "res_line",
                "t": "res_trafo",
                "t3": "res_trafo3w"
            }.get(element_type)
            element_current = self._get_element_current(net, bus, element, element_type, row)

            self.switch_index_to_info[index] = {
                "bus": bus,
                "bus_voltage": f"res_bus.{bus}.vm_pu",
                "element_type": element_type,
                "element_table": element_table,
                "element_id": element,
                "element_current": element_current,
                "alarm_state_u": "normal",
                "alarm_state_i": "normal"
            }
            self._get_thresholds_for_bus(bus, row)
            self.bus_index_to_switches.setdefault(bus, set()).add(index)

    def _get_thresholds_for_bus(self, index, bus):
        self.bus_index_to_thresholds[index] = {
            "u>": bus["u>"] if "u>" in bus else None,
            "u>>": bus["u>>"] if "u>>" in bus else None,
            "u<": bus["u<"] if "u<" in bus else None,
            "u<<": bus["u<<"] if "u<<" in bus else None
        }
        self._overwrite_bus_thresholds_with_args(index)
        if not self.bus_index_to_thresholds[index]["u>>"] or not self.bus_index_to_thresholds[index]["u<<"]:
            # not all non-optional thresholds were given, put bus on whitelist
            self.args["whitelist"]["busses"].append(index)

    def _overwrite_bus_thresholds_with_args(self, bus):
        if str(bus) in self.args["thresholds"]["busses"].keys():
            for threshold, value in self.args["thresholds"]["busses"][str(bus)].items():
                self.bus_index_to_thresholds[bus][threshold] = value

    def _get_element_current(self, net, bus, element, element_type, row):
        element_current = None
        if element_type in ["l", "t"]:
            if element_type == "l":
                element_current = f"res_line.{element}.i_ka"
                self._get_thresholds_for_line(element, row)
            elif element_type == "t":
                trafo_hv = net["trafo"].at[element, "hv_bus"]
                side = "hv" if trafo_hv == bus else "lv"
                element_current = f"res_trafo.{element}.i_{side}_ka"
        return element_current

    def _get_thresholds_for_line(self, index, line):
        self.lines_to_thresholds[index] = {
            "i>": line["i>"] if "i>" in line else None,
            "i>>": line["i>>"] if "i>>" in line else None,
            "unit": "kA"
        }
        self._overwrite_line_thresholds_from_args(index)
        if not all(self.lines_to_thresholds[index]):
            # some thresholds were not given, put line on whitelist
            self.args["whitelist"]["lines"].append(index)

    def _overwrite_line_thresholds_from_args(self, line):
        if str(line) in self.args["thresholds"]["lines"]:
            for threshold, value in self.args["thresholds"]["lines"][str(line)].items():
                self.lines_to_thresholds[line][threshold] = value

    def _extract_load_and_generator_data(self, net):
        for index, bus in net["bus"].iterrows():
            self.bus_index_to_loads[index] = {}
            for load_index, load in net["load"].iterrows():
                if load["bus"] == index:
                    self.bus_index_to_loads[index].update({load_index: load})
            self.bus_index_to_generators[index] = {}
            for gen_index, row in net["sgen"].iterrows():
                if row["bus"] == index:
                    self.bus_index_to_generators[index].update({gen_index: row})


    def _handle_res_bus(self, net):
        buses_with_problems = self._find_buses_with_problematic_state(net)
        for bus, data in buses_with_problems.items():
            if bus in self.args["whitelist"]["busses"]:
                continue
            self.log(f"Found problem at bus {bus}.")
            state = buses_with_problems[bus]["state"]
            if state == "over":
                success = self.handle_bus_overvoltage(net, bus, state)
                if success:
                    return True
            if state == "high":
                return self.handle_bus_high_voltage(net, bus)
            if state == "under":
                success = self.handle_bus_undervoltage(net, bus, state)
                if success:
                    return True
        return False

    def handle_bus_high_voltage(self, net, bus):
        percentage = 50.0
        return self.reduce_generator(net, bus, percentage)

    def reduce_generator(self, net, bus, percentage):
        generators = self.bus_index_to_generators[bus]
        for index, generator in generators.items():
            if generator["in_service"]:
                self.log(f"Reducing power generated from generator {index} at bus {bus} by {percentage} %.")
                net["sgen"].at[index, "p_mw"] = net["sgen"].at[index, "p_mw"] * (percentage / 100)
                net["sgen"].at[index, "q_mvar"] = net["sgen"].at[index, "q_mvar"] * (percentage / 100)
                return True
        return False

    def _find_buses_with_problematic_state(self, net):
        buses_with_problems = {}
        for index, bus_row in net["res_bus"].iterrows():
            current_voltage = bus_row["vm_pu"]
            deviation = abs(1 - current_voltage)
            state, value = self._calculate_state(prefix="u", s_id=index, value=current_voltage, net=net)
            if state in ["under", "over", "high"]:
                buses_with_problems.update({index: {"deviation": deviation, "state": state}})
        return _sort_dict_after_values(buses_with_problems, "deviation")

    def handle_bus_overvoltage(self, net, bus, state):
        success = self._disable_generator_at_bus(net, bus)
        return True if success else self.find_switch_to_open(net, bus, state)

    def _disable_generator_at_bus(self, net, switch_bus):
        generators = self.bus_index_to_generators[switch_bus]
        for index, generator in generators.items():
            if generator["in_service"]:
                self.log(f"Disabling generator {index} at bus {switch_bus}.")
                net["sgen"].at[index, "in_service"] = False
                return True
        return False

    def handle_bus_undervoltage(self, net, bus, state):
        success = self._disable_load_at_bus(net, bus)
        return True if success else self.find_switch_to_open(net, bus, state)

    def _disable_load_at_bus(self, net, bus):
        loads = self.bus_index_to_loads[bus]
        sorted_loads = self._sort_loads_by_power(loads)
        for index, load in sorted_loads.items():
            if load["in_service"]:
                self.log(f"Disabling load {load['name']} at bus {bus}.")
                net["load"].at[index, "in_service"] = False
                return True
        return False

    def _sort_loads_by_power(self, loads):
        index_to_load = {}
        for index, load in loads.items():
            index_to_load[index] = {"power": load["p_mw"]}
        sorted_indexes = _sort_dict_after_values(index_to_load, "power")
        sorted_loads = {}
        for index in sorted_indexes.keys():
            sorted_loads[index] = loads[index]
        return sorted_loads

    def find_switch_to_open(self, net, bus, state):
        lines = self._find_problem_lines_for_bus(net, bus, state)
        if len(lines) == 0:
            self.log(f"No switches found for bus {bus}.")
            return False
        lines_by_length = _sort_dict_after_values(lines, "length")
        if len(lines_by_length.keys()) == 1:
            self._open_switch(net, list(lines_by_length.values())[0]["switch"])
            return True
        elif len(lines_by_length.keys()) > 1 and list(lines_by_length.values())[0]["length"] == list(lines_by_length.values())[1]["length"]:
            # unclear which to choose, use power criteria
            return self._open_switch_with_highest_bus_power(net, lines)
        else:
            self._open_switch(net, list(lines_by_length.values())[0]["switch"])
            return True

    def _find_problem_lines_for_bus(self, net, bus, state):
        lines = {}
        switches = list(self.bus_index_to_switches[bus])
        for switch in switches:
            if net["switch"].at[switch, "closed"]:
                line_id = int(self.switch_index_to_info[switch]["element_current"].split(".")[1])
                for index, line in net["res_line"].iterrows():
                    to_bus = net["line"].at[index, "to_bus"]
                    from_bus = net["line"].at[index, "from_bus"]
                    other_bus = to_bus if to_bus != bus else from_bus
                    power = line["i_ka"]
                    if index == line_id and (to_bus == bus or from_bus == bus):
                        if self._is_switch_closed_on_line_at_bus(net, other_bus, index):
                            if (state == "over" and line["i_from_ka"] >= line["i_to_ka"]) or (state == "under" and line["i_from_ka"] <= line["i_to_ka"]):
                                path_length = self.get_smallest_distance_from_external_grids(other_bus)
                                lines[index] = {"line": line, "other_bus": other_bus, "power": power, "switch": switch, "length": path_length}
        return lines

    def _is_switch_closed_on_line_at_bus(self, net, bus, line_index):
        switches = list(self.bus_index_to_switches[bus])
        for switch in switches:
            line_id = int(self.switch_index_to_info[switch]["element_current"].split(".")[1])
            if line_id == line_index:
                return net["switch"].at[switch, "closed"]

    def get_smallest_distance_from_external_grids(self, bus):
        ext_grid_nodes = [node for node in self.owl.graph.get_layer("power-grid").graph if "external_grid" in node]
        node_by_lengths = {}
        for ext_grid in ext_grid_nodes:
            path_length = nx.shortest_paths.shortest_path_length(
                self.owl.graph.get_layer("power-grid").graph, ext_grid, f"bus.{bus}"
            )
            node_by_lengths[ext_grid] = {"length": path_length}
        return list(_sort_dict_after_values(node_by_lengths, "length", False).values())[0]["length"]

    def _open_switch(self, net, switch):
        self.log(f"Opening switch {switch}.")
        net["switch"].at[switch, "closed"] = False

    def _open_switch_with_highest_bus_power(self, net, lines):
        bus_by_power = _sort_dict_after_values(lines, "power")
        if len(bus_by_power.keys()) > 1:
            if bus_by_power[list(bus_by_power.keys())[0]]["power"] == bus_by_power[list(bus_by_power.keys())[1]]["power"]:
                # unclear which to choose, try next bus
                return False
        self._open_switch(net, bus_by_power[list(bus_by_power.keys())[0]]["switch"])
        return True

    def _handle_res_line(self, net):
        lines_with_problems = self._find_lines_with_problems(net)
        for index, line_data in lines_with_problems.items():
            if index in self.args["whitelist"]["lines"]:
                continue
            # open switch at bus where power comes from
            self.log(f"Found problem at line {index}.")
            if line_data["current"] > 0:
                success = self._try_to_open_switch_at_line(net, line_data['from_bus'], line_data['to_bus'], index)
                if success:
                    return True
                continue
            else:
                success = self._try_to_open_switch_at_line(net, line_data['to_bus'], line_data['from_bus'], index)
                if success:
                    return True
                continue
        return False
    
    def _try_to_open_switch_at_line(self, net, first_bus, second_bus, index):
        self.log(f"Trying to open switch at bus {first_bus}.")
        if not self._open_switch_at_line(net, first_bus, index):
            self.log(f"Found no switch to open.")
            self.log(f"Trying to open switch at bus {second_bus}.")
            if not self._open_switch_at_line(net, second_bus, index):
                self.log(f"Found no switch to open. Continuing with next line.")
                return False
        return True

    def _find_lines_with_problems(self, net):
        lines_with_problems = {}
        for index, row in net["res_line"].iterrows():
            state, value = self._calculate_state(prefix="i", s_id=index, value=row["i_ka"], net=net)
            if state in ["under", "over"]:
                lines_with_problems.update({index: {
                    "current": row["i_ka"],
                    "load": row["loading_percent"],
                    "state": state,
                    "to_bus": net["line"].at[index, "to_bus"],
                    "from_bus": net["line"].at[index, "from_bus"]
                }})
        return _sort_dict_after_values(lines_with_problems, "load")

    def _open_switch_at_line(self, net, bus, index):
        switches = list(self.bus_index_to_switches[bus])
        for switch in switches:
            line_id = int(self.switch_index_to_info[switch]["element_current"].split(".")[1])
            if index == line_id:
                self._open_switch(net, switch)
                return True
        return False

    def _calculate_state(self, prefix: str, s_id: int, value: DataPointValue, net):
        if prefix == "i":
            return self._calculate_line_state(net, s_id, value, prefix)
        else:
            # calculate bus data
            data = self.bus_index_to_thresholds[s_id]
            under = data.get(f"{prefix}<<")
            low = data.get(f"{prefix}<")
            high = data.get(f"{prefix}>")
            over = data.get(f"{prefix}>>")
            if under is not None and value <= under:
                return "under", under
            elif low is not None and value <= low:
                return "low", low
            elif over is not None and value >= over:
                return "over", over
            elif high is not None and value >= high:
                return "high", high
            else:
                return "normal", value


    def _calculate_line_state(self, net, s_id, value, prefix):
        data = self.lines_to_thresholds[s_id]
        high = data.get(f"{prefix}>")
        over = data.get(f"{prefix}>>")
        unit = data.get("unit")
        if unit == "%":
            max_i_ka = net["line"].at[s_id, "max_i_ka"]
            percentage = (value / max_i_ka) * 100
            if over is not None and percentage >= 100 + over:
                return "over", over
            elif high is not None and percentage >= 100 + high:
                return "high", high
            else:
                return "normal", value
        elif unit == "kA":
            if over is not None and value >= over:
                return "over", over
            elif high is not None and value >= high:
                return "high", high
            else:
                return "normal", value

    def log(self, message):
        self.coordinator.logger.info(f"\n[CircuitBreakerLogic]:\n{message}\n")

def _sort_dict_after_values(d: dict, value_key, high_to_low=True) -> dict:
    return {k: v for k, v in sorted(d.items(), key=lambda i: i[1][value_key], reverse=high_to_low)}
