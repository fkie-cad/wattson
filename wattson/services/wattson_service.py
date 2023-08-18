import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional, TYPE_CHECKING, List, Callable, Dict

from wattson.cosimulation.exceptions import ServiceException
from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.configuration.configuration_expander import ConfigurationExpander
from wattson.services.configuration.configuration_store import ConfigurationStore
from wattson.services.service_priority import ServicePriority
from wattson.services.wattson_remote_service_representation import WattsonRemoteServiceRepresentation
from wattson.services.wattson_service_interface import WattsonServiceInterface

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
    from wattson.services.configuration.service_configuration import ServiceConfiguration


class WattsonService(WattsonServiceInterface):
    """
    A WattsonService is a service or process that runs on a WattsonNetworkHost.
    Essentially, the WattsonService wraps a Popen object to handle rotating log files
    and abstract from Paths.
    """
    _gid: int = 0
    _instances: Dict[int, 'WattsonService'] = {}

    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        self._service_configuration = service_configuration
        self.id = WattsonService._gid
        self.name = service_configuration.get("name", f"{self.__class__.__name__}")
        WattsonService._gid += 1
        self.network_node: 'WattsonNetworkNode' = network_node
        self._artifacts: List[ArtifactRotate] = []
        self._process: Optional[subprocess.Popen] = None
        self._run_iteration = 0

        self.autostart = service_configuration.get("autostart", True)
        self.autostart_delay = service_configuration.get("autostart_delay", 0)
        if self.autostart_delay is None:
            self.autostart_delay = 0

        self._priority: ServicePriority = ServicePriority.from_service_priority(self, service_configuration.priority)
        self.dir: Optional[Path] = None
        self.guest_dir: Optional[Path] = None
        self.config_file: Optional[ArtifactRotate] = None
        self._killed: bool = False
        self._enable_monitoring = False
        self._monitoring_process: Optional[subprocess.Popen] = None
        WattsonService._instances[self.id] = self

        self.log_file: Optional[ArtifactRotate] = None
        self._log_handle = None

    def ensure_working_directory(self):
        if self.dir is None:
            self.dir = self.network_node.get_host_folder()
            self.guest_dir = self.network_node.get_guest_folder()
        if self.config_file is None:
            self.config_file = ArtifactRotate(
                self.dir.joinpath(f"{self.network_node.entity_id}-service-{self.id}.config")
            )
            self._artifacts.append(self.config_file)
        if self.log_file is None:
            self.log_file = ArtifactRotate(self.dir.joinpath(f"{self.network_node.entity_id}-service-{self.id}.log"))
            self._artifacts.append(self.log_file)

    def get_start_command(self) -> List[str]:
        return []

    def get_extra_arguments(self) -> Dict:
        return {}

    def get_priority(self) -> ServicePriority:
        return self._priority

    def get_stdout(self):
        """
        @return: The file descriptor to use as stdout
        """
        return sys.stdout

    def get_stderr(self):
        """
        @return: The file descriptor to use as stderr
        """
        return sys.stderr

    def get_log_handle(self):
        # self._clear_log_handle()
        self._log_handle = self.log_file.get_current().open("w")
        return self._log_handle

    def __repr__(self):
        return f"{self.network_node.entity_id} // {self.id}"

    def delay_start(self, delay_seconds: float):
        if delay_seconds == 0:
            return self.start()

        def delayed_start():
            self.network_node.logger.info(f"Service {self.id} starts in {int(delay_seconds)} s")
            time.sleep(delay_seconds)
            self.network_node.logger.info(f"Service {self.id} starts now")
            self.start()

        t = threading.Thread(target=delayed_start)
        t.start()

    def start(self) -> bool:
        self.ensure_working_directory()
        self._killed = False
        if self.is_running():
            return True
        # Artifact Rotation
        for artifact in self._artifacts:
            artifact.rotate()
        # Write configuration
        self.write_configuration()
        self.network_node.logger.debug(f"Starting Service {self.id}")
        self.network_node.logger.debug(" ".join(self.get_start_command()))

        if len(self.get_start_command()) == 0:
            return True

        def pre_exec_function():
            # Detach from process group to ignore signals sent to main process
            os.setpgrp()

        self._process = self.network_node.popen(
            self.get_start_command(),
            stdout=self.get_stdout(),
            stderr=self.get_stderr(),
            preexec_fn=pre_exec_function,
            **self.get_extra_arguments()
        )
        if self._enable_monitoring:
            pid = self._process.pid
            self._monitoring_process = self.network_node.popen(
                f"strace -f -p {pid} -e trace=%process",
                stdout=self.get_stdout(),
                stderr=self.get_stderr(),
                preexec_fn=pre_exec_function
            )
        return True

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        if not self.is_running():
            if async_callback is not None:
                async_callback(self)
            return True
        self._process.send_signal(signal.SIGTERM)
        if async_callback is None:
            return self._sync_wait(wait_seconds=wait_seconds, auto_kill=auto_kill)
        else:
            t = threading.Thread(target=self._sync_wait, args=(wait_seconds, auto_kill, async_callback))
            t.start()
        return not self.is_running()

    def _sync_wait(self, wait_seconds: float, auto_kill: bool, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        try:
            self.wait(wait_seconds)
            self._killed = False
            self._clear_log_handle()
            if async_callback is not None:
                async_callback(self)
            return True
        except subprocess.TimeoutExpired:
            killed = False
            if auto_kill:
                killed = self.kill()
                self.wait(wait_seconds)
            if async_callback is not None:
                async_callback(self)
            self._clear_log_handle()
            return killed

    def _clear_log_handle(self):
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None

    def kill(self) -> bool:
        if not self.is_running():
            return True
        self._process.send_signal(signal.SIGKILL)
        self._killed = True
        return True

    def is_killed(self) -> bool:
        return self._killed

    def restart(self) -> bool:
        return self.stop() and self.start()

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def get_process(self) -> Optional[subprocess.Popen]:
        """
        @return: The process associated with this service or None if the service is not running.
        """
        return self._process

    def get_pid(self) -> Optional[int]:
        """
        @return: The PID of the service process or None if the service is not running.
        """
        process = self.get_process()
        return process.pid if process is not None else None

    def poll(self) -> Optional[int]:
        """
        Polls the service process.
        @return: None or the return code of the process.
        """
        return self.get_process().poll()

    def wait(self, timeout: Optional[float] = None) -> int:
        if not self.is_running():
            if self._process is None:
                raise ServiceException("Non-started service cannot be waited for")
            return self._process.returncode
        return self._process.wait(timeout=timeout)

    def write_configuration_file(self, configuration: dict):
        self.ensure_working_directory()
        with self.config_file.get_current().open("w") as f:
            json.dump(configuration, f)

    def get_current_guest_configuration_file_path(self) -> Path:
        self.ensure_working_directory()
        return self.guest_dir.joinpath(self.config_file.get_current().relative_to(self.dir))

    def write_configuration(self):
        configuration_store = self.network_node.network_emulator.get_configuration_store()
        if configuration_store is None:
            configuration_store = ConfigurationStore()
        expander = ConfigurationExpander(configuration_store=configuration_store)
        expanded_configuration = expander.expand_node_configuration(self.network_node, self._service_configuration)
        self.write_configuration_file(expanded_configuration)

    @staticmethod
    def get_instance(service_id) -> 'WattsonService':
        service = WattsonService._instances.get(service_id)
        if service is None:
            raise ServiceException(f"Service {service_id} not found")
        return service

    def to_remote_representation(self) -> WattsonRemoteServiceRepresentation:
        return WattsonRemoteServiceRepresentation({
            "service_id": self.id,
            "name": self.name,
            "is_running": self.is_running(),
            "is_killed": self.is_killed(),
            "pid": self.get_pid(),
            "priority": self.get_priority().to_remote_representation(),
            "command": self.get_start_command()
        })
