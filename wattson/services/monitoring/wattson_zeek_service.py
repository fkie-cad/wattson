import subprocess
from time import sleep
from typing import Optional, Callable

from wattson.services.deployment import PythonDeployment
from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger


class WattsonZeekService(WattsonService):

    def __init__(self, service_configuration, network_node):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("WattsonZeek", "WattsonZeek")
        self.interface = self._service_configuration["interface"]
        self.inter_startup_delay = self._service_configuration["inter_startup_delay_s"]

    def start(self):
        super().start()
        self.network_node.popen(["sysctl", "-w", "vm.max_map_count=1677720"], shell=True)
        self.network_node.popen(["/usr/bin/mongod", "--config", "/etc/mongod.conf", "--fork"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["su", "opensearch", "-c", "'/usr/share/opensearch/bin/opensearch'"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["/usr/share/graylog-server/bin/graylog-server"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["filebeat", "setup", "-e"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["filebeat", "-e"], shell=True)
        sleep(self.inter_startup_delay)
        p = self.network_node.popen(["zeek", "-i", self.interface], shell=True)
        self._process = p
        return p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.logger.error("Stop command not (yet) implemented for ZEEK service.")
        return success
