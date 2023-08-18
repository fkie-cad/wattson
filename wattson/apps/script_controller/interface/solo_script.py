from wattson.apps.script_controller.interface import Script
from abc import abstractmethod


class SoloScript(Script):
    def get_start_time(self):
        return 0

    @abstractmethod
    def run(self):
        pass

    def stop(self):
        pass
