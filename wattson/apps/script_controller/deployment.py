import json
import pandapower
import time
import threading
from typing import Dict, List

import yaml

from wattson.apps.gui import WebGui
from wattson.deployment import PythonDeployment

from wattson.apps.script_controller import ScriptControllerApp


class ScriptControllerDeployment(PythonDeployment):
    def __init__(self, configuration):
        super().__init__(configuration)
        self.restart_event = threading.Event()

        self.config = configuration
        self.net = self.load_powernet("powernet")
        # Get PandaPower Network - sorry for the encoding
        """dicts = yaml.load(
            bytes.fromhex(self.config["powernet"]).decode("utf-8"),
            Loader=yaml.FullLoader
        )
        self.net = pandapower.from_json_string(json.dumps(dicts))"""
        # here, the passed datapoints is a dict of dicts of dicts: COA ->
        # [index_per_coa -> row]
        #datapoints_dict: Dict[int, Dict[int, Dict[str, Union[bool, int, float, str]]]] = self.config["datapoints"]
        datapoints_dict: Dict[str, List] = self.config["datapoints"]
        # create a DataFrame from an iterable that chains all the rows
        #datapoints = itertools.chain.from_iterable(datapoints_dict[coa].values() for coa in datapoints_dict)
        self.datapoints = datapoints_dict
        #self.datapoints = pandas.DataFrame(datapoints)
        self.mtu_ip = self.config["mtu_ip"]
        self.management_ip = self.config["ip"]
        self.coord_ip = self.config["coordinator_ip"]
        self.scripts = self.config.get("scripts", [])
        self.scenario_path = self.config.get("scenario_path")
        self.export_config = self.config.get("export_config", {})
        self.export_config["folder"] = self.artifacts_dir.joinpath("controller-export")
        try:
            pandapower.runpp(self.net)
            print("Done")
        except Exception as e:
            print(f"{e}")
        self.statistics = self.config.get("statistics", {})

    def restart(self):
        self.restart_event.set()

    def start(self):
        app = ScriptControllerApp(host_ip=self.management_ip, mtu_ip=self.mtu_ip, coordinator_ip=self.coord_ip,
                                  datapoints=self.datapoints, grid=self.net, scripts=self.scripts,
                                  scenario_path=self.scenario_path, export_config=self.export_config,
                                  statistics_config=self.statistics)
        app.start()


