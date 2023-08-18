from typing import Dict

from wattson.services.deployment import PythonDeployment


class GenericDeployment(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._launch = configuration
        self._config = configuration.get("config")

    def start(self):
        pass
