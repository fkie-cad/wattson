import sys
from pathlib import Path

from wattson.deployment import PythonDeployment
from wattson.util import get_logger
from wattson.util.misc import dynamic_load_method_from_file


class DefaultDeployment(PythonDeployment):
    def __init__(self, configuration):
        super().__init__(configuration)

        self.logger = get_logger("Wattson", "DefaultDeployment")

        if "powernet" in self.config:
            self.config["powernet"] = self.load_powernet("powernet")

        self.script = self.config.get("script")
        self.scenario_path = Path(self.config.get("scenario_path", "."))
        if self.script is not None:
            self.config.pop("script")
            self.script = Path(self.script)
            if not self.script.is_absolute():
                self.script = self.scenario_path.joinpath(self.script)

    def start(self):
        if self.script is None:
            self.logger.warning("No Script defined.")
            sys.exit(1)
        main_method = dynamic_load_method_from_file(self.script, "main")
        main_method(self.config)

    def stop(self):
        raise KeyboardInterrupt()
