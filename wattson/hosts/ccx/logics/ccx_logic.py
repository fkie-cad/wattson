from abc import ABC
from pathlib import Path
from time import time

import yaml

from wattson.hosts.ccx.logics.logic_return_action import LogicReturnAction


class CCXLogic(ABC):
    def __init__(self, ccx, **kwargs):
        self.ccx = ccx
        self.logger = self.ccx.logger.getChild(self.__class__.__name__)
        self.logger.info("Instantiating CCX Logic")
        self.config_file = kwargs.pop("config_file", None)
        if not self.config_file:
            raise ValueError("No config file provided for CCXLogic.")
        parts = self.config_file.split("/")
        _file = parts[-1]
        parts = parts[:-1]
        _dir = "/".join(parts)[1:]
        path = (Path(__file__).parent / _dir) / _file
        self.config = yaml.load(path.open("r"), Loader=yaml.Loader)
        self.start_time = None
        self.start_delay = self.config["start_delay"]
        del self.config["start_delay"]
        self.coa_ioa_to_id: dict[str, str] = {}
        self.id_to_coa_ioa = {}

    def start(self):
        self.start_time = time()

    def stop(self):
        pass

    def apply(self, event_type: str, args) -> LogicReturnAction:
        pass

    def replace_wildcards(self):
        default_functions = self.config.pop("default_functions")
        expanded_config = {}
        for coa, data in self.config.copy().items():
            for item in data:
                dps = item["datapoints"]
                for dp in dps:
                    expanded_config[dp] = {}
                    expanded_config = self.replace_type_wildcard(dp, coa, item, expanded_config)
                    expanded_config = self.replace_id_wildcard(coa, expanded_config, item)
                    expanded_config = self.replace_attribute_wildcard(coa, expanded_config, item)
        for coa_ioa, _id in self.coa_ioa_to_id.items():
            self.id_to_coa_ioa[_id] = coa_ioa
        for identifier, data in expanded_config.copy().items():
            if not data["options"]["functions"]:
                data["options"]["functions"] = default_functions
        return expanded_config

    def replace_attribute_wildcard(self, coa, expanded_config, item):
        new_expanded_config = {}
        for grid_element_identifier, data in expanded_config.copy().items():
            if "*" not in grid_element_identifier:
                new_expanded_config[grid_element_identifier] = data
                continue
            parts = grid_element_identifier.split(".")
            e_type, e_index, context, attribute = parts
            if attribute == "*":
                for _id, dp in self.ccx.data_points.items():
                    c, i = dp["identifier"].split(".")
                    if c == str(coa):
                        for provider in dp["providers"]:
                            for entry in dp["providers"][provider]:
                                _type, ind = entry["provider_data"]["grid_element"].split(".")
                                if _type == e_type and ind == e_index and entry["provider_data"]["context"] == context:
                                    attr = entry['provider_data']['attribute']
                                    new_grid_element_identifier = f"{e_type}.{e_index}.{context}.{attr}"
                                    new_expanded_config[new_grid_element_identifier] = {"options": item["options"]}
                                    self.coa_ioa_to_id[dp["identifier"]] = new_grid_element_identifier
            else:
                new_expanded_config[grid_element_identifier] = data
        return new_expanded_config

    def replace_id_wildcard(self, coa, expanded_config, item):
        new_expanded_config = {}
        for grid_element_identifier, data in expanded_config.copy().items():
            if "*" not in grid_element_identifier:
                new_expanded_config[grid_element_identifier] = data
                continue
            parts = grid_element_identifier.split(".")
            e_type, e_index, context, attribute = parts
            if e_index == "*":
                for _id, dp in self.ccx.data_points.items():
                    c, i = dp["identifier"].split(".")
                    if c == str(coa):
                        for provider in dp["providers"]:
                            for entry in dp["providers"][provider]:
                                element_id = entry['provider_data']['grid_element'].split(".")[1]
                                if e_type == entry['provider_data']['grid_element'].split(".")[0] or e_type == "*":
                                    if context == entry['provider_data']['context']:
                                        if attribute == entry['provider_data']['attribute'] or attribute == "*":
                                            new_grid_element_identifier = f"{e_type}.{element_id}.{context}.{attribute}"
                                            new_expanded_config[new_grid_element_identifier] = {
                                                "options": item["options"]}
                                            self.coa_ioa_to_id[dp["identifier"]] = new_grid_element_identifier
            else:
                new_expanded_config[grid_element_identifier] = data
        return new_expanded_config

    def replace_type_wildcard(self, dp, coa, item, expanded_config):
        parts = dp.split(".")
        e_type, e_index, context, attribute = parts
        if "*" not in dp:
            expanded_config[dp] = {"options": item["options"]}
            for _id, _dp in self.ccx.data_points.items():
                c, i = _dp["identifier"].split(".")
                if c == str(coa):
                    for provider in _dp["providers"]:
                        for entry in _dp["providers"][provider]:
                            element_type = entry['provider_data']['grid_element'].split(".")[0]
                            if element_type == e_type:
                                if e_index == entry['provider_data']['grid_element'].split(".")[1]:
                                    if context == entry['provider_data']['context']:
                                        if attribute == entry['provider_data']['attribute']:
                                            new_grid_element_identifier = f"{element_type}.{e_index}.{context}.{attribute}"
                                            self.coa_ioa_to_id[_dp["identifier"]] = new_grid_element_identifier
            return expanded_config
        if e_type == "*":
            for _id, _dp in self.ccx.data_points.items():
                c, i = _dp["identifier"].split(".")
                if c == str(coa):
                    for provider in _dp["providers"]:
                        for entry in _dp["providers"][provider]:
                            element_type = entry['provider_data']['grid_element'].split(".")[0]
                            if e_index == entry['provider_data']['grid_element'].split(".")[
                                1] or e_index == "*":
                                if context == entry['provider_data']['context']:
                                    if attribute == entry['provider_data']['attribute'] or attribute == "*":
                                        new_grid_element_identifier = f"{element_type}.{e_index}.{context}.{attribute}"
                                        expanded_config[new_grid_element_identifier] = {"options": item["options"]}
                                        self.coa_ioa_to_id[_dp["identifier"]] = new_grid_element_identifier
        return expanded_config
