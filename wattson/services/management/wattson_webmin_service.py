import subprocess
from typing import List, Optional, Callable, TYPE_CHECKING

from wattson.services.wattson_service import WattsonService

if TYPE_CHECKING:
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonWebminService(WattsonService):
    def get_start_command(self) -> List[str]:
        return ["/etc/webmin/start"]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        code, _ = self.network_node.exec(["/etc/webmin/stop"])
        if async_callback is not None:
            async_callback(self)
        return code == 0

    def get_stdout(self):
        return subprocess.DEVNULL

    def get_stderr(self):
        return self.get_stdout()
