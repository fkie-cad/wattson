from typing import Dict
import time

import pandas

from wattson.deployment import PythonDeployment
from wattson.hosts.rtu.rtu import RTU


class GenericDeployment(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._launch = configuration
        self._config = configuration.get("config")

    def start(self):
        pass
