import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Callable

from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonElkService(WattsonService):
    def start(self):
        super().start()
        start_file = Path("/usr/local/bin/start.sh")
        if not self.network_node.file_exists(start_file):
            self.network_node.logger.error(f"Cannot start ELK service as not start script can be found")
            return False
        self.network_node.set_sysctl("vm.max_map_count", 262144)
        p = self.network_node.popen([str(start_file.absolute())], shell=True, stdout=self.get_stdout(), stderr=self.get_stderr())
        self._process = p
        return p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["service", "elasticsearch", "stop"])
        self.network_node.exec(["service", "logstash", "stop"])
        self.network_node.exec(["service", "kibana", "stop"])
        return success

    def get_stderr(self):
        return self.get_stdout()

    def get_stdout(self):
        return self.get_log_handle()
