from wautorunner.scenario.scenario import Scenario
from pathlib import Path
from fnmatch import fnmatch
import pandas as pd
import json, os, glob, datetime, logging


class ExperimentAnalyzer:
    """Analyzes the emuation results generated from a specific scenario."""

    MEASURES = {
        "bus": ["voltage"],
        "line": ["loading"]
    }

    CONFIGS = {
        "switch": ["closed"]
    }

    def __init__(self, working_dir: Path, scenario: Scenario):
        self.logger = logging.getLogger("ExperimentAnalyzer")
        self.working_dir = working_dir
        self.scenario = scenario
        self.measures: pd.DataFrame = pd.DataFrame(columns=["timestamp", "element-type", "element-id", "measure-name", "measure"])
        self.configurations: pd.DataFrame = pd.DataFrame(columns=["timestamp", "element-type", "element-id", "config-name", "state"])
        self._loadResults()

    def genTraces(self, dt: int) -> dict:
        if self.measures.empty:
            raise ValueError("No measures data available.")
        
        self.measures.sort_index(ascending=True, inplace=True)
        self.configurations.sort_index(ascending=True, inplace=True)

        startTime = self.measures["timestamp"].min()
        endTime = self.measures["timestamp"].max()
        timeBins = pd.date_range(start=startTime, end=endTime, freq=f"{dt}S")

        percAbnormalBusses = []
        percOverloadedLines = []

        for i in range(len(timeBins) - 1):
            binStart = timeBins[i]
            binEnd = timeBins[i + 1]
            measuresInTimeBin = self.measures[(self.measures["timestamp"] >= binStart) & (self.measures["timestamp"] < binEnd)]
            configurationsInTimeBin = self.configurations[(self.configurations["timestamp"] >= binStart) & (self.configurations["timestamp"] < binEnd)]

            if not measuresInTimeBin.empty:
                busVoltagesInTimeBin = measuresInTimeBin[(measuresInTimeBin["element-type"] == "bus") & (measuresInTimeBin["measure-name"] == "voltage")].sort_values(by="element-id")
                lineLoadingsInTimeBin = measuresInTimeBin[(measuresInTimeBin["element-type"] == "line") & (measuresInTimeBin["measure-name"] == "loading")]
                busVoltagesInTimeBin = busVoltagesInTimeBin.groupby(["element-id", "measure-name"]).agg({"measure": "mean"})
                lineLoadingsInTimeBin = lineLoadingsInTimeBin.groupby(["element-id", "measure-name"]).agg({"measure": "mean"})
                numAbnormalBusses = len(busVoltagesInTimeBin[(busVoltagesInTimeBin["measure"] < 0.95) | (busVoltagesInTimeBin["measure"] > 1.05)])
                numOverloadedLines = len(lineLoadingsInTimeBin[lineLoadingsInTimeBin["measure"] > 100])
                numBusses = len(busVoltagesInTimeBin)
                numLines = len(lineLoadingsInTimeBin)

                percAbnormalBusses.append(numAbnormalBusses / numBusses if numBusses > 0 else 0)
                percOverloadedLines.append(numOverloadedLines / numLines if numLines > 0 else 0)

                self.logger.debug(f"Time bin {i} - {binStart} to {binEnd}")
                for index, row in busVoltagesInTimeBin.iterrows():
                    self.logger.debug(f"Bus {index} - {row['measure']}")
                for index, row in lineLoadingsInTimeBin.iterrows():
                    self.logger.debug(f"Line {index} - {row['measure']}")
                self.logger.debug(f"Number of abnormal busses: {numAbnormalBusses} / {numBusses}")
                self.logger.debug(f"Number of overloaded lines: {numOverloadedLines} / {numLines}")
            else:
                self.logger.debug(f"Time bin {i} - {binStart} to {binEnd}")
                self.logger.debug("No measures in this time bin.")
                
            if not configurationsInTimeBin.empty:
                switchConfigsInTimeBin = configurationsInTimeBin[configurationsInTimeBin["element-type"] == "switch"]
                switchConfigsInTimeBin = switchConfigsInTimeBin.groupby(["element-id", "config-name"]).agg({"state": "mean"})
                switchConfigsInTimeBin["state"] = switchConfigsInTimeBin["state"].apply(lambda x: True if x > 0.5 else False)
                self.logger.debug(f"Time bin {i} - {binStart} to {binEnd}")
                for index, row in switchConfigsInTimeBin.iterrows():
                    self.logger.debug(f"Switch {index} - {row['state']}")
            else:
                self.logger.debug(f"No configurations in time bin {i}.")


        return {
            "percAbnormalBusses": percAbnormalBusses,
            "percOverloadedLines": percOverloadedLines
        }

    def _loadResults(self):
        matchingDirs = [dir for dir in os.listdir(self.working_dir) if fnmatch(dir, f"{self.scenario.getName()}*")]
        matchingDirs.sort(reverse=False)
        scenarioWorkingDir = self.working_dir.joinpath(matchingDirs[-1])
        gridExportsPath: Path = scenarioWorkingDir.joinpath("power_grid_exports") 
        exportFiles = [gridExportsPath.joinpath(fileStr) for fileStr in glob.glob("power_grid_*.json", root_dir=gridExportsPath)]

        measures_data = []
        configs_data = []

        for filePath in exportFiles:
            with open(filePath, "r") as file:
                data: dict = json.load(file)
                timestamp = datetime.datetime.fromtimestamp(data.get("timestamp"))
                values: dict = data.get("values", {})

                # Process measures
                for element_type, measure_names in ExperimentAnalyzer.MEASURES.items():
                    for measure_name in measure_names:
                        for key, value in values.items():
                            if fnmatch(key, f"{element_type}.*.MEASUREMENT.{measure_name}"):
                                element_id = key.split(".")[1]
                                measures_data.append({
                                    "timestamp": timestamp,
                                    "element-type": element_type,
                                    "element-id": element_id,
                                    "measure-name": measure_name,
                                    "measure": value
                                })

                # Process configurations
                for element_type, config_names in ExperimentAnalyzer.CONFIGS.items():
                    for config_name in config_names:
                        for key, value in values.items():
                            if fnmatch(key, f"{element_type}.*.CONFIGURATION.{config_name}"):
                                element_id = key.split(".")[1]
                                configs_data.append({
                                    "timestamp": timestamp,
                                    "element-type": element_type,
                                    "element-id": element_id,
                                    "config-name": config_name,
                                    "state": value
                                })

        # Initialize DataFrames
        self.measures = pd.DataFrame(measures_data)
        self.configurations = pd.DataFrame(configs_data)

        
        
        
