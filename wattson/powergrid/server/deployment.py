from pathlib import Path

from wattson.deployment import PythonDeployment
import pandapower
import time
import yaml, json
from wattson.powergrid.server.coord_server import CoordinationServer


class DefaultCoordinator(PythonDeployment):
    def __init__(self, configuration: dict):
        super().__init__(configuration)
        self.config = configuration
        self.pnet = self.load_powernet("powernet")
        
        """pnetyaml = bytes.fromhex(self.config["powernet"]).decode("utf-8")
        dicts = yaml.load(
            pnetyaml,
            Loader=yaml.FullLoader
        )
        #for key in dicts.keys():
        #    dicts[key] = pandas.DataFrame.from_dict(dicts[key], "index")

        #self.pnet = pandapower.from_dict_of_dfs(dicts)
        self.pnet = pandapower.from_json_string(json.dumps(dicts))"""
        self.nodes = self.config["rtu_ids"] + self.config["mtu_ids"]
        self.coas = self.config["rtu_coas"] + self.config["mtu_coas"]
        scenario_path = Path(self.config["scenario_path"])
        self.ip = self.config["ip"]
        if "config" not in self.config:
            self.config["config"] = {}
        ups = 1 / self.config["config"].get("export_interval", 1)
        self.config["config"]["export_root"] = self.artifacts_dir.joinpath("powergrid")

        statistics = self.config.get("statistics")

        self.coord = CoordinationServer(
            self.pnet,
            ip_address=self.ip,
            coas=self.coas,
            nodes=self.nodes,
            scenario_path=scenario_path,
            grid_config=self.config["config"].get("grid_config", False),
            config=self.config["config"],
            main_pid=self.config.get("main_pid", -1),
            ups=ups,
            statistics=statistics,
            profile_loader_exists=self.config.get("profile_loader_exists", False)
        )
        return

    def start(self):
        self.coord.start()
        self.coord.join()

    def stop(self):
        self.coord.stop()
