import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional, TYPE_CHECKING, List, Callable, Dict, Union

from wattson.cosimulation.exceptions import ServiceException
from wattson.services.artifact_rotate import ArtifactRotate
from wattson.services.configuration.configuration_expander import ConfigurationExpander
from wattson.services.configuration.configuration_store import ConfigurationStore
from wattson.services.service_priority import ServicePriority
from wattson.services.wattson_remote_service_representation import WattsonRemoteServiceRepresentation
from wattson.services.wattson_service_interface import WattsonServiceInterface
from wattson.util.performance.performance_decorator import performance_assert

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
        self._artifact_path: Optional[Path] = None
        self._artifacts_by_name: Dict[str, ArtifactRotate] = {}
        self._artifacts: List[ArtifactRotate] = []
        self._process: Optional[subprocess.Popen] = None
        self._run_iteration = 0

        self.autostart = service_configuration.get("autostart", True)
        self.autostart_delay = service_configuration.get("autostart_delay", 0)
        if self.autostart_delay is None:
            self.autostart_delay = 0

        self._priority: ServicePriority = ServicePriority.from_service_priority(self, service_configuration.priority)
        self.working_directory: Optional[Path] = None
        self.guest_working_directory: Optional[Path] = None
        self.config_file: Optional[ArtifactRotate] = None
        self._killed: bool = False
        self._enable_monitoring = False
        self._monitoring_process: Optional[subprocess.Popen] = None
        WattsonService._instances[self.id] = self

        self.log_file: Optional[ArtifactRotate] = None
        self._log_handle = None

    def ensure_working_directory(self):
        if self.working_directory is None:
            self.working_directory = self.network_node.get_host_folder()
            self.guest_working_directory = self.network_node.get_guest_folder()

    def ensure_artifacts(self):
        self.ensure_working_directory()
        if self.config_file is None:
            self.config_file = ArtifactRotate(
                self.working_directory.joinpath(f"{self.network_node.entity_id}-service-{self.id}.config")
            )
            self._artifacts.append(self.config_file)
        if self.log_file is None:
            self.log_file = ArtifactRotate(self.working_directory.joinpath(f"{self.network_node.entity_id}-service-{self.id}.log"))
            self._artifacts.append(self.log_file)

    @property
    def artifact_path(self):
        if self._artifact_path is None:
            self._artifact_path = self.working_directory
        return self._artifact_path

    def get_artifact(self, filename: str, is_folder: bool = False) -> ArtifactRotate:
        self.ensure_artifacts()
        if filename in self._artifacts_by_name:
            return self._artifacts_by_name[filename]
        # artifact = ArtifactRotate(Path("/tmp").joinpath(f"wattson_{self.network_node.system_name}_{filename}"), is_folder=is_folder)
        artifact = ArtifactRotate(self.artifact_path.joinpath(f"wattson_{self.network_node.system_name}_{filename}"), is_folder=is_folder)
        self._artifacts_by_name[filename] = artifact
        return artifact

    def get_artifact_paths(self) -> dict[str, Path]:
        artifacts = {}
        for artifact in self._artifacts:
            artifacts[artifact.get_base_name()] = artifact.get_current()
        return artifacts

    # hier call methode einbauen
    def call(self, method, **kwargs):
        raise RuntimeError("Basic WattsonService has no call method.")

    def callable_methods(self) -> dict[str, dict]:
        return {}

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

    def update(self):
        """
        Update the service state (is_running, ...)
        @return:
        """
        pass

    def start(self, refresh_config: bool = False) -> bool:
        self.ensure_artifacts()
        self._killed = False
        if self.is_running():
            return True
        # Artifact Rotation
        if refresh_config:
            for artifact in self._artifacts:
                artifact.rotate()
        # Write configuration
        self.write_configuration(refresh_config=refresh_config)

        self.network_node.logger.debug(f"Starting Service {self.id}")
        self.network_node.logger.debug(" ".join(self.get_start_command()))

        if len(self.get_start_command()) == 0:
            self.create_scripts()
            return True

        def pre_exec_function():
            # Detach from process group to ignore signals sent to main process
            os.setpgrp()

        self._process = self.network_node.popen(
            self.get_start_command(),
            stdout=self.get_stdout(),
            stderr=self.get_stderr(),
            preexec_fn=pre_exec_function,
            cwd=str(self.guest_working_directory.absolute()),
            **self.get_extra_arguments()
        )

        self.create_scripts()

        self.poll_in_thread(self._process)

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

    def restart(self, refresh_config: bool = False) -> bool:
        return self.stop() and self.start(refresh_config=refresh_config)

    @performance_assert(0.3)
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def get_process(self) -> Optional[subprocess.Popen]:
        """
        @return: The process associated with this service or None if the service is not running.
        """
        return self._process

    @performance_assert(0.1)
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

    def write_configuration_file(self, configuration: dict, refresh_config: bool = False) -> None:
        self.ensure_artifacts()
        if not refresh_config and not self.config_file.is_empty():
            return
        with self.config_file.get_current().open("w") as f:
            json.dump(configuration, f)

    def get_current_guest_configuration_file_path(self) -> Path:
        self.ensure_artifacts()
        return self.guest_working_directory.joinpath(self.config_file.get_current().relative_to(self.working_directory))

    @performance_assert(0.2)
    def expand_configuration(self):
        configuration_store = self.network_node.network_emulator.get_configuration_store()
        if configuration_store is None:
            configuration_store = ConfigurationStore()
        expander = ConfigurationExpander(configuration_store=configuration_store)
        return expander.expand_node_configuration(self.network_node, self._service_configuration)

    def update_service_configuration(self, configuration: Union[Dict, 'ServiceConfiguration']):
        self._service_configuration.update(configuration)

    def write_configuration(self, refresh_config: bool = False):
        expanded_configuration = self.expand_configuration()
        self.write_configuration_file(expanded_configuration, refresh_config=refresh_config)

    def get_start_script_content(self) -> Optional[List[str]]:
        return None

    def get_stop_script_content(self) -> Optional[List[str]]:
        return None

    def create_scripts(self):
        start_script_content = self.get_start_script_content()
        stop_script_content = self.get_stop_script_content()
        restart_script_content = []
        if start_script_content is not None:
            file_name = f"start_{self.name.lower().replace(' ', '-')}-{self.id}.sh"
            start_script_file = self.working_directory.joinpath(file_name)
            start_script_host_file = self.guest_working_directory.joinpath(file_name)
            restart_script_content.append(str(start_script_host_file.absolute()))
            with start_script_file.open("w") as f:
                f.write("\n".join(start_script_content))
            start_script_file.chmod(0o777)

        if stop_script_content is not None:
            file_name = f"stop_{self.name.lower().replace(' ', '-')}-{self.id}.sh"
            stop_script_file = self.working_directory.joinpath(file_name)
            stop_script_host_file = self.guest_working_directory.joinpath(file_name)
            restart_script_content.insert(0, str(stop_script_host_file.absolute()))
            with stop_script_file.open("w") as f:
                f.write("\n".join(stop_script_content))
            stop_script_file.chmod(0o777)

        if len(restart_script_content) > 0:
            file_name = f"restart_{self.name.lower().replace(' ', '-')}-{self.id}.sh"
            restart_script_file = self.working_directory.joinpath(file_name)
            with restart_script_file.open("w") as f:
                f.write("\n".join(restart_script_content))
            restart_script_file.chmod(0o777)

    @staticmethod
    def get_instance(service_id) -> 'WattsonService':
        service = WattsonService._instances.get(service_id)
        if service is None:
            raise ServiceException(f"Service {service_id} not found")
        return service

    @performance_assert(0.5)
    def to_remote_representation(self) -> WattsonRemoteServiceRepresentation:
        return WattsonRemoteServiceRepresentation({
            "service_id": self.id,
            "name": self.name,
            "is_running": self.is_running(),
            "is_killed": self.is_killed(),
            "pid": self.get_pid(),
            "priority": self.get_priority().to_remote_representation(),
            "command": self.get_start_command(),
            "configuration": self._service_configuration.to_dict(),
            "expanded_configuration": self.expand_configuration().to_dict(),
            "callable_methods": self.callable_methods(),
            "artifact_paths": self.get_artifact_paths(),
            "original_class": self.__class__
        })

    def poll_in_thread(self, process: subprocess.Popen):
        def do_poll_until_terminated(_process: subprocess.Popen):
            _process.wait()
        thread = threading.Thread(target=do_poll_until_terminated, args=(process, ), daemon=True)
        thread.start()
