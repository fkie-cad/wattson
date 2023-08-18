import codecs
import json
import pickle
import subprocess
from typing import Type, TYPE_CHECKING, List, Optional, Callable

from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.wattson_service import WattsonService
from wattson.services.deployment import PythonDeployment

if TYPE_CHECKING:
    from wattson.services.configuration.service_configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonPythonService(WattsonService):
    def __init__(self, service_class: Type[PythonDeployment], service_configuration: 'ServiceConfiguration',
                 network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.service_class = service_class

    def get_start_command(self) -> List[str]:
        if self.config_file is None or self.network_node is None:
            return []
        return [self.network_node.get_python_executable(), "-m", "wattson.services.deployment",
                self.network_node.get_hostname(),
                str(self.get_current_guest_configuration_file_path().absolute())]

    def get_stdout(self):
        return self.get_log_handle()

    def get_stderr(self):
        return subprocess.STDOUT

    def write_configuration_file(self, configuration: dict):
        pickled_configuration_string = codecs.encode(pickle.dumps(configuration), "base64").decode()
        deployment_config = {
            "hostid": self.network_node.id,
            "hostname": self.network_node.get_hostname(),
            "module": str(self.service_class.__module__),
            "class": str(self.service_class.__name__),
            "config": pickled_configuration_string
        }
        with self.config_file.get_current().open("w") as f:
            json.dump(deployment_config, f)
