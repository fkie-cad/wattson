import fnmatch
import logging
from pathlib import Path
from typing import Optional

import yaml


class DataPointLoader:
    def __init__(self, scenario_path: Path, logger: Optional[logging.Logger] = None):
        self.path = scenario_path
        self.logger = logger

    def _load_from_file(self, file: Path):
        pass

    def get_data_points(self):
        try:
            with self.path.joinpath("datapoints.yml").open("r") as f:
                datapoints_tmp = yaml.load(f, Loader=yaml.FullLoader)["datapoints"]
            datapoints = {}

            ## Expand Datapoints that are references (for MTUs)
            datapoint_map = {}
            datapoints_mixed = {}
            # First: Create Datapoint Map
            for host, dps in datapoints_tmp.items():
                datapoints[host] = []
                datapoints_mixed[host] = []
                if isinstance(dps, str):
                    try:
                        with self.path.joinpath(dps).open("r") as f:
                            dps = yaml.load(f, Loader=yaml.FullLoader)["datapoints"][host]
                    except Exception as e:
                        if self.logger is not None:
                            self.logger.error(f"Could not load referenced file {dps}: {e}")
                        dps = []

                if isinstance(dps, list):
                    for dp in dps:
                        if isinstance(dp, dict):
                            identifier = dp["identifier"]
                            if "providers" not in dp:
                                dp["providers"] = {}
                            datapoint_map[identifier] = dp
                            datapoints[host].append(dp)
                            datapoints_mixed[host].append(dp)
                        else:
                            datapoints_mixed[host].append(dp)
                else:
                    if self.logger is not None:
                        self.logger.warning("Unknown Datapoint format")
                    continue

            # Second: Use Map to replace potential reference lists with actual datapoints
            for host, dps in datapoints_mixed.items():
                if isinstance(dps, list):
                    if host not in datapoints:
                        datapoints[host] = []
                    for dp in dps:
                        if isinstance(dp, str):
                            matches = fnmatch.filter(datapoint_map.keys(), dp)
                            if len(matches) == 0:
                                if self.logger is not None:
                                    self.logger.warning(f"Invalid data point reference '{dp}' for host '{host}'")
                                continue
                            for match in matches:
                                datapoints[host].append(datapoint_map[match])
        except Exception:
            raise RuntimeError("Invalid Scenario configuration: Cannot read datapoints.yml")
        return datapoints
