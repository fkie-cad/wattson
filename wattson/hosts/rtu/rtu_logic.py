from abc import ABC

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.hosts.rtu import RTU


class RTULogic(ABC):
    def __init__(self, rtu: 'RTU', **kwargs):
        self.rtu = rtu
        self.config = kwargs
        self.logger = self.rtu.logger.getChild(self.__class__.__name__)
        self.logger.info("Instantiating RTU Logic")

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def configure(self):
        pass
