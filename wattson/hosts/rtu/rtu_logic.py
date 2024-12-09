from abc import ABC

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from wattson.hosts.rtu import RTU


class RTULogic(ABC):
    def __init__(self, rtu: 'RTU', **kwargs):
        self.rtu = rtu
        self.config_file = kwargs["config_file"] if "config_file" in kwargs else None
        self.config = {}
        self.identifier_to_functions = {}
        self.identifier_to_grid_id = {}
        self.logger = self.rtu.logger.getChild(self.__class__.__name__)
        self.logger.info("Instantiating RTU Logic")

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def configure(self):
        pass

    def handles_set_value(self, identifier, value) -> bool:
        """
        Returns whether this logic scripts wants to handle the set operation for the given data point and value.
        If true, handle_set_value is called by the managing RTU afterward.
        @param identifier: The data point's identifier
        @param value: The value to set
        @return: Whether this logic wants to handle the set operation
        """
        return False

    def handle_set_value(self, identifier, value) -> bool:
        """
        Handles the setting of the data point value instead of the default handler.
        @param identifier: The data point's identifier
        @param value: The value to set
        @return: Whether the value has been set successfully
        """
        return False

    def handles_get_value(self, identifier) -> bool:
        """
        Returns whether this logic scripts wants to handle the get operation for the given data point.
        If true, handle_get_value is called by the managing RTU afterward.
        @param identifier: The data point's identifier
        @return: Whether this logic wants to handle the set operation
        """
        return False

    def handle_get_value(self, identifier) -> Any:
        """
        Handles the retrieval of the data point value instead of the default handler.
        @param identifier: The data point's identifier
        @return: The value of the data point
        """
        return None

    def _get_id_to_grid_id(self):
        default_functions = self.config["default_functions"]
        del (self.config["default_functions"])
        overall_grid_ids_to_functions = {}
        for grid_id, functions in self.config.copy().items():
            if not functions["functions"]:
                functions = default_functions
            else:
                functions = functions["functions"]
            parts = grid_id.split(".")

            if len(parts) == 1:
                grid_ids_to_functions = self._replace_solo_attribute(self.rtu.coa, parts[0], functions)
            else:
                # len(parts) == 4
                grid_ids_to_functions = self._replace_wildcards(grid_id, self.rtu.coa, parts,
                                                                functions)
            overall_grid_ids_to_functions.update(grid_ids_to_functions)
        for grid_element_identifier in overall_grid_ids_to_functions:
            parts = grid_element_identifier.split(".")
            grid_element = f"{parts[0]}.{parts[1]}"
            context = parts[2]
            attribute = parts[3]
            for datapoint in self.rtu.manager.data_points.values():
                for provider in datapoint["providers"]:
                    for entry in datapoint["providers"][provider]:
                        if entry["provider_data"]["context"] == context and entry["provider_data"][
                            "grid_element"] == grid_element and entry["provider_data"][
                            "attribute"] == attribute:
                            self.identifier_to_grid_id[datapoint["identifier"]] = grid_element_identifier
                            self.identifier_to_functions[datapoint["identifier"]] = \
                                overall_grid_ids_to_functions[
                                    grid_element_identifier]
        overall_grid_ids_to_functions["default_functions"] = default_functions
        self.config = overall_grid_ids_to_functions

    def _replace_solo_attribute(self, coa, solo_attr, functions):
        new_data = {}
        for dp in self.rtu.manager.data_points.values():
            for provider in dp["providers"]:
                for entry in dp["providers"][provider]:
                    if entry["provider_data"]["attribute"] == solo_attr:
                        new_grid_element_identifier = f"{entry['provider_data']['grid_element']}.{entry['provider_data']['context']}.{entry['provider_data']['attribute']}"
                        self.identifier_to_grid_id[dp["identifier"]] = new_grid_element_identifier
                        new_data[new_grid_element_identifier] = functions
        return new_data

    def _replace_type_wildcards(self, coa, index, context, attribute,
                                functions):
        new_data = {}
        for dp in self.rtu.manager.data_points.values():
            for provider in dp["providers"]:
                for entry in dp["providers"][provider]:
                    element_type = entry['provider_data']['grid_element'].split(".")[0]
                    if index == entry['provider_data']['grid_element'].split(".")[1] or index == "*":
                        if context == entry['provider_data']['context']:
                            if attribute == entry['provider_data']['attribute'] or attribute == "*":
                                new_grid_element_identifier = f"{element_type}.{index}.{context}.{attribute}"
                                new_data[new_grid_element_identifier] = functions
        return new_data

    def _replace_id_wildcards(self, coa, e_type, context, attribute,
                              functions):
        new_data = {}
        for dp in self.rtu.manager.data_points.values():
            for provider in dp["providers"]:
                for entry in dp["providers"][provider]:
                    element_id = entry['provider_data']['grid_element'].split(".")[1]
                    if e_type == entry['provider_data']['grid_element'].split(".")[0] or e_type == "*":
                        if context == entry['provider_data']['context']:
                            if attribute == entry['provider_data']['attribute'] or attribute == "*":
                                new_grid_element_identifier = f"{e_type}.{element_id}.{context}.{attribute}"
                                new_data[new_grid_element_identifier] = functions
        return new_data

    def _replace_attribute_wildcard(self, coa, e_type, e_index, context,
                                    functions):
        new_data = {}
        for dp in self.rtu.manager.data_points.values():
            for provider in dp["providers"]:
                for entry in dp["providers"][provider]:
                    _type, ind = entry["provider_data"]["grid_element"].split(".")
                    if _type == e_type and ind == e_index and entry["provider_data"]["context"] == context:
                        attr = entry['provider_data']['attribute']
                        new_grid_element_identifier = f"{e_type}.{e_index}.{context}.{attr}"
                        new_data[new_grid_element_identifier] = functions
        return new_data

    def _replace_wildcards(self, grid_element_identifier, coa, parts,
                           functions):
        e_type, e_index, context, attribute = parts
        return_data = {}
        if e_type == "*":
            type_ids_to_functions = self._replace_type_wildcards(coa, e_index, context, attribute, functions)
            if e_index == "*":
                for grid_id in type_ids_to_functions:
                    e_type, e_index, context, attribute = grid_id.split(".")
                    index_grid_ids = self._replace_id_wildcards(coa, e_type, context,
                                                                attribute, functions)
                    if attribute == "*":
                        for g_id in index_grid_ids:
                            e_type, e_index, context, attribute = g_id.split(".")
                            return_data.update(
                                self._replace_attribute_wildcard(coa, e_type, e_index, context,
                                                                 functions))
                    else:
                        return_data.update(index_grid_ids)
            else:
                if attribute == "*":
                    for g_id in type_ids_to_functions:
                        e_type, e_index, context, attribute = g_id.split(".")
                        return_data.update(
                            self._replace_attribute_wildcard(coa, e_type, e_index, context,
                                                             functions))
                else:
                    return_data.update(type_ids_to_functions)
        else:
            if e_index == "*":
                index_grid_ids = self._replace_id_wildcards(coa, e_type,
                                                            context, attribute, functions)
                if attribute == "*":
                    for g_id in index_grid_ids:
                        e_type, e_index, context, attribute = g_id.split(".")
                        return_data.update(
                            self._replace_attribute_wildcard(coa, e_type, e_index, context,
                                                             functions))
                else:
                    return_data.update(index_grid_ids)
            else:
                if attribute == "*":
                    return_data.update(
                        self._replace_attribute_wildcard(coa, e_type, e_index,
                                                         context, functions))
                else:
                    return_data.update({grid_element_identifier: functions})
        return return_data
