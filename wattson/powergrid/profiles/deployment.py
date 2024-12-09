import json
import time
from typing import Dict

import pandapower
import yaml

from wattson.powergrid.profiles.profile_provider import ProfileLoaderFactory
from wattson.services.deployment import PythonDeployment


class ProfileLoaderDeployment(PythonDeployment):
    def __init__(self, configuration: Dict):
        self.config = configuration
        self.coordinator_ip = self.config["coordinator_ip"]
        # Get PandaPower Network - sorry for the encoding
        dicts = yaml.load(
            bytes.fromhex(self.config["powernet"]).decode("utf-8"),
            Loader=yaml.FullLoader
        )
        self.net = pandapower.from_json_string(json.dumps(dicts))

        self.profile_config = {
            "profiles": {
                "load": None,
                "sgen": None
            },
            "profile_path": None,
            "profile_dir": "default_profiles",
            "seed": 0,
            "noise": "1%",
            "interval": 5,
            "interpolate": "cubic",
            "speed": 1.0,
            "start_datetime": False,
            "stop": False
        }
        for key in self.profile_config.keys():
            if key in self.config:
                self.profile_config[key] = self.config[key]

    def start(self):
        self.profile_loader = ProfileLoaderFactory(
            self.coordinator_ip,
            self.net,
            **self.profile_config
        )
        self.profile_loader.start()
        while True:
            time.sleep(60)
