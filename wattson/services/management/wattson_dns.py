from wattson.services.wattson_service import WattsonService

from typing import Optional, Callable


class WattsonDnsService(WattsonService):
    def start(self):
        super().start()
        p = self.network_node.popen(["service", "named", "restart"])
        self._process = p
        return p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["service", "named", "stop"])
        return success
