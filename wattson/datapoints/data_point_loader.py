import fnmatch
import logging
from pathlib import Path
from typing import Optional, Any, Union

import yaml

from powerowl.layers.network.configuration.data_point_type import DataPointType
from powerowl.performance.function_performance import FunctionPerformance
from wattson.util.log import get_logger


class DataPointLoader:
    def __init__(self, data_point_main_file_path: Path, logger: Optional[logging.Logger] = None):
        self.path = data_point_main_file_path.parent
        self.data_point_main_file_path = data_point_main_file_path
        self.logger = logger
        if logger is None:
            self.logger = get_logger(self.__class__.__name__, level=logging.INFO)
        #self.logger.setLevel(logging.DEBUG)

    def _load_from_file(self, file: Path):
        pass

    def get_data_points(self):
        try:
            with self.data_point_main_file_path.open("r") as f:
                datapoints_tmp = yaml.load(f, Loader=yaml.CLoader)
            datapoints_tmp = datapoints_tmp.get("datapoints", datapoints_tmp)
            datapoints = {}

            ## Expand Datapoints that are references (for MTUs)
            datapoints_mixed = {}

            # Map types to identifiers to objects.
            reference_map = {}
            # First: Create Datapoint Map
            for host, dps in datapoints_tmp.items():
                datapoints[host] = []
                datapoints_mixed[host] = []
                rcbs = []

                if isinstance(dps, str):
                    try:
                        with self.path.joinpath(dps).open("r") as f:
                            dps = yaml.load(f, Loader=yaml.CLoader)
                        dps = dps.get("datapoints", dps)[host]
                    except Exception as e:
                        if self.logger is not None:
                            self.logger.error(f"Could not load referenced file {dps}: {e}")
                        dps = []

                if isinstance(dps, list):
                    for dp in dps:
                        if isinstance(dp, dict):
                            identifier = dp["identifier"]
                            dp["type"] = dp.get("type", DataPointType.DATA_POINT)

                            if dp["type"] == DataPointType.DATA_POINT:
                                datapoints[host].append(dp)
                                datapoints_mixed[host].append(dp)
                                if "providers" not in dp:
                                    dp["providers"] = {}

                            reference_map.setdefault(dp["type"], {})[identifier] = dp
                        else:
                            datapoints_mixed[host].append(dp)
                else:
                    if self.logger is not None:
                        self.logger.warning("Unknown Datapoint format")
                    continue

            def expand_references(_element: Any, _parent: Optional[Union[dict, list]] = None, key: Any = None):
                if isinstance(_element, (int, float, bool, type(None))):
                    # Nothing to expand
                    return
                if isinstance(_element, str):
                    if _element.startswith("$ref$"):
                        # Expansion required
                        if _parent is None:
                            raise ValueError(f"String value require valid parent object for expansion")
                        _, _, _type, _identifier = _element.split("$")
                        if _type not in reference_map:
                            raise KeyError(f"Unknown reference type {_type}")
                        if _identifier not in reference_map[_type]:
                            raise KeyError(f"Unknown reference identifier {_identifier} for type {_type}")
                        _parent[key] = reference_map[_type][_identifier]
                        return
                    # Non $ref$ string: nothing to expand
                    return

                if isinstance(_element, dict):
                    for _key, _value in _element.items():
                        expand_references(_value, _element, _key)
                    return

                if isinstance(_element, list):
                    for i in range(len(_element)):
                        expand_references(_element[i], _element, i)
                    return

                raise ValueError(f"Unknown element type to expand: {type(_element)}")

            # Expand explicit references
            for data_point_type, typed_map in reference_map.items():
                for identifier, data_point in typed_map.items():
                    expand_references(data_point)


            # TODO: do this recursively and more generic.
            # create reference map (type -> (identifier -> resolved entities of that type))
            # if $ref is found, resolve.
            # if datapoint_map[match]["type"] == datapoint then line 73
            # add datapoint_map[match]type$datapoint_map[match]identifier to reference map

            # Third: Look into the data points and replace potential references to, e.g., report control blocks
            # (iec61850) with actual report control blocks.
            """
            for host, dps in datapoints.items():
                for idx, dp in enumerate(dps):
                    if self.logger is not None:
                        self.logger.debug(f"Handling rcb expansion in dp {dp['identifier']}")
                        self.logger.debug(f"Dp is {dp}")

                    rcbs_expanded = []

                    if "report_control_blocks" in dp["protocol_data"]:
                        # The current point is in an rcb.
                        if self.logger is not None:
                            self.logger.debug(f"DP {dp['identifier']} has report_control_blocks {dp['protocol_data']['report_control_blocks']}")

                        for rcb in dp["protocol_data"]["report_control_blocks"]:
                            if isinstance(rcb, str):
                                # rcb is not yet expanded
                                if not rcb.startswith("$ref$"):
                                    raise Exception(f"Invalid reference in dp {dp['identifier']} to rcb {rcb}.  Reference format must start with '$ref$'")

                                # rcb_identifier[0] == ""
                                # rcb_identifier[1] == "ref"
                                # rcb_identifier[2] == "61850-rcb"
                                # rcb_identifier[3] == `rcb identifier`
                                rcb_identifier: list[str] = rcb.split("$")

                                if self.logger is not None:
                                    self.logger.debug(f"Split rcb identifier is {rcb_identifier}, shrinking to {rcb_identifier[3]}.")
                                rcb_identifier: str = rcb_identifier[3]

                                rcb_expanded: dict = reference_map["61850-rcb"].get(rcb_identifier, None)
                                if rcb_expanded is None:
                                    if self.logger is not None:
                                        self.logger.error(
                                            f"Trying to expand {rcb_identifier}, but it does not exist in reference map!")
                                    raise Exception(f"Invalid reference in dp {dp['identifier']} to rcb {rcb}.")

                                rcbs_expanded.append(rcb_expanded)
                            elif isinstance(rcb, dict):
                                rcbs_expanded.append(rcb)
                            else:
                                raise Exception("rcb is neither a string nor a dict")

                    # We have expanded all rcbs and kept all rcbs that were already expanded (for some reason).
                    # Now, replace the list of report control blocks in the data point with the expanded list.
                    # If there were no rcbs in the current data point, the empty list will be placed in the data point.
                    datapoints[host][idx]["protocol_data"]["report_control_blocks"] = rcbs_expanded
                    if self.logger is not None:
                        self.logger.debug(f"DP is now {datapoints[host][idx]}")
            """

            # Second: Use Map to replace potential reference lists with actual datapoints
            for host, dps in datapoints_mixed.items():
                if isinstance(dps, list):
                    if host not in datapoints:
                        datapoints[host] = []
                    for dp in dps:
                        if isinstance(dp, str):
                            datapoint_map = reference_map.get("datapoint", {})

                            if "*" in dp:
                                with FunctionPerformance.manual_measure("fnmatch"):
                                    matches = fnmatch.filter(datapoint_map.keys(), dp)
                            else:
                                with FunctionPerformance.manual_measure("simple-match"):
                                    matches = []
                                    if dp in datapoint_map:
                                        matches.append(dp)
                            if len(matches) == 0:
                                if self.logger is not None:
                                    self.logger.warning(f"Invalid data point reference '{dp}' for host '{host}'")
                                continue
                            for match in matches:
                                datapoints[host].append(datapoint_map[match])

        except Exception:
            raise RuntimeError("Invalid Scenario configuration: Cannot read datapoints.yml")

        # TODO: assume dp["type"] = "datapoint", return only data points.
        #       Handle RCBs as references, put the RCB information into the data point (under key report_control_blocks)
        #       In RTU, iterate over points, if RCB already exists add point to respective data set,
        #       otherwise create rcb and data set and add point afterwards.
        return datapoints
