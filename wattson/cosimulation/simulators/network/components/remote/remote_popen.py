import signal
import subprocess
import time
from typing import Optional, TYPE_CHECKING

from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode


class RemotePopen(WattsonRemoteObject):
    """Represents a process running on a remote node."""
    def __init__(self, remote_node: 'RemoteNetworkNode', pid: int):
        # There is no super class call by design!
        self._remote_node = remote_node
        self._pid = pid

        self._state = None

        self.logger = get_logger(f"{self.__class__.__name__}.{self._remote_node.entity_id}.{self._pid}")

    def error(self, code: int, error_string: Optional[str] = None):
        self._state = {
            "stdout": None,
            "stderr": error_string,
            "return_code": code
        }

    @property
    def wattson_client(self):
        return self._remote_node.wattson_client

    @property
    def state(self):
        if self._state is None:
            return {}
        return self._state

    @property
    def return_code(self):
        return self.state.get('return_code')

    @property
    def return_output(self):
        return self.state.get('stdout')
    
    def _decode_output(self, output):
        if output is None:
            return []
        try:
            return output.decode("utf-8").split("\n")
        except Exception:
            return []
    
    @property
    def return_output_list(self):
        return self._decode_output(self.return_output)
    
    @property
    def return_error_list(self):
        return self._decode_output(self.return_error)        
        
    @property
    def return_error(self):
        return self.state.get('stderr')

    def synchronize(self, force: bool = False, block: bool = True):
        if self._state is not None and not force:
            return True
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.PROCESS_ACTION,
            query_data={
                "entity_id": self._remote_node.entity_id,
                "action": "synchronize",
                "pid": self._pid,
            }
        )
        response = self.wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            self.logger.error(f"Failed to synchronize ({error=})")
            return False
        self._state = response.data
        return True

    def poll(self) -> int | None:
        if self.return_code is None:
            if not self.synchronize(force=True, block=True):
                self.logger.error(f"Synchronization failed - assuming an error. Returning -2")
                return -2
        return self.return_code

    def wait(self, timeout=None, _interval: float = 0.5):
        start_time = time.perf_counter()
        while self.poll() is None:
            if timeout is not None:
                wait_time = time.perf_counter() - start_time
                if wait_time > timeout:
                    raise subprocess.TimeoutExpired(f"Timed out waiting for process {self._pid}")
            time.sleep(_interval)
        return self.return_code

    def communicate(self, input=None, timeout=None):
        if input is not None:
            raise NotImplementedError("RemotePopen.communicate() not implemented with input")
        self.wait(timeout)
        return self.return_output, self.return_error

    def send_signal(self, sig):
        pass

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

    @property
    def pid(self):
        return self._pid

    @property
    def returncode(self):
        return self.return_code
