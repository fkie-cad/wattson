import time
from typing import Optional

from wattson.hosts.ccx import ControlCenterExchangeGateway
from wattson.services.deployment import PythonDeployment


class CCXDeployment(PythonDeployment):
    def __init__(self, configuration: dict):
        super().__init__(configuration)
        self.config = configuration
        self.node_id = self.config.get("node_id", None)
        data_points = self.config.get("data_points", [])
        data_point_dict = {data_point["identifier"]: data_point for data_point in data_points}
        self.config["data_points"] = data_point_dict
        self.ccx: Optional[ControlCenterExchangeGateway] = None
        if isinstance(self.config["logics"], dict):
            self.config["logics"] = self.config["logics"].get(self.node_id, [])
        else:
            self.config["logics"] = {}

    def start(self):
        self.ccx = ControlCenterExchangeGateway(**self.config)
        self.ccx.start()
        while True:
            time.sleep(60)

    def stop(self):
        self.ccx.stop()
