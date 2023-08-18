import threading
from typing import Dict, List

from wattson.apps.script_controller import ScriptControllerApp
from wattson.services.deployment import PythonDeployment
from wattson.util import get_logger


class ScriptControllerDeployment(PythonDeployment):
    def __init__(self, configuration):
        super().__init__(configuration)
        self.restart_event = threading.Event()

        self.config = configuration
        datapoints_dict: Dict[str, List] = self.config["datapoints"]
        self.datapoints = datapoints_dict
        self.mtu_ip = self.config["mtu_ip"]
        self.management_ip = self.config["ip"]

        self.wattson_client_query_socket = self.config.get("wattson_client_query_socket")
        self.wattson_client_publish_socket = self.config.get("wattson_client_publish_socket")

        self.scripts = self.config.get("scripts", [])
        self.scenario_path = self.config.get("scenario_path")
        self.export_config = self.config.get("export_config", {})
        if "folder" not in self.export_config:
            self.export_config["folder"] = self.artifacts_dir
        self.statistics = self.config.get("statistics", {})

        self.logger = get_logger("Ctrl", "Ctrl")

    def restart(self):
        self.restart_event.set()

    def start(self):
        self.logger.info("Starting ScriptControllerApp")
        app = ScriptControllerApp(host_ip=self.management_ip,
                                  mtu_ip=self.mtu_ip,
                                  wattson_client_query_socket=self.wattson_client_query_socket,
                                  wattson_client_publish_socket=self.wattson_client_publish_socket,
                                  datapoints=self.datapoints,
                                  scripts=self.scripts,
                                  scenario_path=self.scenario_path, export_config=self.export_config,
                                  statistics_config=self.statistics)
        app.start()