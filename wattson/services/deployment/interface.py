import abc
import os
import signal
import sys
from typing import Dict, Type

import pandapower as pandapower
from powerowl.layers.powergrid import PowerGridModel

from wattson.util.random import Random
from wattson.util.log import get_logger
from pathlib import Path


class PythonDeployment(abc.ABC):
    def __init__(self, configuration: Dict):
        self.config = configuration
        self.artifacts_dir = Path(self.config.get("artifacts_dir", ""))
        random_seed = configuration.get("random_seed", 0)
        self._deployment_logger = get_logger("Wattson", "Deployment")
        if random_seed == 0:
            self._deployment_logger.warning("Wattson.Random: Random seed is 0")
        self._deployment_logger.info(f"Setting random base seed to {random_seed}")
        self._deployment_logger.info(f"Using Wattson module: {Path(__file__).resolve().parent.parent.absolute()}")
        Random.set_base_seed(random_seed)
        self._original_handlers = {
            "sigint": signal.getsignal(signal.SIGINT),
            "sigterm": signal.getsignal(signal.SIGTERM)
        }
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def load_powernet(self, key):
        json_net = bytes.fromhex(self.config[key]).decode("utf-8")
        return pandapower.from_json_string(json_net)

    def load_power_grid(self, key, power_grid_class: Type[PowerGridModel] = PowerGridModel) -> PowerGridModel:
        json_grid = self.config[key]
        grid = power_grid_class()
        grid.from_primitive_dict(json_grid)
        return grid

    def _exit_gracefully(self, *args):
        self._deployment_logger.info("Received exit signal. Stopping...")
        self.stop()
        self._deployment_logger.info("Resetting signal handlers...")
        signal.signal(signal.SIGINT, self._original_handlers["sigint"])
        signal.signal(signal.SIGTERM, self._original_handlers["sigterm"])
        self._deployment_logger.info("Stopping process...")
        os.kill(os.getpid(), signal.SIGTERM)
        raise SystemExit()

    @abc.abstractmethod
    def start(self):
        raise NotImplementedError()

    def init_cli(self, cli_config):
        pass

    def start_cli(self):
        pass

    def stop(self):
        pass
