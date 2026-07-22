import codecs
import json
import pickle
import subprocess
from typing import Type, TYPE_CHECKING, List, Optional, Callable

from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.wattson_service import WattsonService
from wattson.services.deployment import PythonDeployment
from wattson.util.json.pickle_encoder import PickleEncoder

if TYPE_CHECKING:
    from wattson.services.configuration.service_configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonProcessService(WattsonService):
    def __init__(self, process_cmd: List[str], service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self._start_command = process_cmd

    def get_start_command(self) -> List[str]:
        return self._start_command

    def get_stdout(self):
        return self.get_log_handle()

    def get_stderr(self):
        return subprocess.STDOUT

    def write_configuration_file(self, configuration: dict, refresh_config: bool = False):
        return
