from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager

from wattson.topology.process.process_info import ProcessInfo


class WattsonProcess(ABC):
    def __init__(self, process_info: ProcessInfo, manager: 'NetworkManager', **kwargs):
        self.process_info: 'ProcessInfo' = process_info
        self.manager: 'NetworkManager' = manager

    def do_autostart(self):
        return self.process_info.deploy_info.get("autostart", True)

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self, max_wait_s: float = 5):
        pass

    @abstractmethod
    def join(self, max_wait_s: float = None):
        pass

    @abstractmethod
    def kill(self):
        pass

    @abstractmethod
    def is_running(self) -> bool:
        pass

    @abstractmethod
    def get_log_file(self) -> Path:
        pass

    def get_pid(self) -> Optional[int]:
        return None

    @staticmethod
    def is_docker_host(host: dict):
        return "deploy" in host and "type" in host["deploy"] and host["deploy"]["type"] == "docker"

    @staticmethod
    def get_process(host: dict, host_config: dict, manager: 'NetworkManager', pid: int = 0,
                    prepare_only: bool = False) -> 'WattsonProcess':

        if "deploy" not in host:
            raise ValueError("Missing Process Deployment Information")
        process_info = WattsonProcess.get_process_info(pid, host, host_config, manager)

        if host["deploy"]["type"] == "python":
            from wattson.topology.process.python_process import PythonProcess
            return PythonProcess(process_info=process_info, manager=manager, prepare_only=prepare_only)
        elif host["deploy"]["type"] == "docker":
            from wattson.topology.process.docker_process import DockerProcess
            return DockerProcess(process_info=process_info, manager=manager)
        else:
            raise ValueError(f"Unknown Deployment Type: {host['deploy']['type']}")

    @staticmethod
    def get_process_info(pid, host, host_config, manager):
        deploy = host["deploy"] if "deploy" in host else {}
        return ProcessInfo(pid=pid, host_id=host["id"], host=host, host_config=host_config,
                           deploy_info=deploy, directory=manager.host_manager.get_host_directory(host))
