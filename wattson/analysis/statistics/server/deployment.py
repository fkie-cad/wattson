from pathlib import Path
from typing import Dict

from wattson.analysis.statistics.server.statistic_server import StatisticServer
from wattson.services.deployment import PythonDeployment


class StatisticServerDeployment(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self.config = configuration

        self.node_id = self.config["nodeid"]
        self.ip_address = self.config["ip"]
        statistics = self.config["statistics"]

        self.powernet = self.load_powernet("powernet")
        self.datapoints = self.config["datapoints"]
        self.network = self.config["network"]

        self.max_size = statistics.get("max_size", None)
        self.target_folder = Path(statistics.get("folder", "."))
        self.server = None
        return

    def start(self):
        self.server = StatisticServer(
            ip=self.ip_address,
            max_size=self.max_size,
            target_folder=self.target_folder,
            power_net=self.powernet,
            data_points=self.datapoints,
            network=self.network
        )
        self.server.start()
        self.server.join()

    def stop(self):
        self.server.stop()
