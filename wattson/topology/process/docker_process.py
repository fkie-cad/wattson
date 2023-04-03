import json
import subprocess
from pathlib import Path
from signal import SIGTERM
from subprocess import Popen, STDOUT
from typing import Union, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager
    from wattson.topology.process.process_info import ProcessInfo

from wattson.topology.process.wattson_process import WattsonProcess


class DockerProcess(WattsonProcess):
    def __init__(self, process_info: 'ProcessInfo', manager: 'NetworkManager'):
        super().__init__(process_info, manager)
        self._proc: Optional[Popen] = None
        self._log_dir = manager.host_manager.get_host_directory(process_info.host).joinpath("logs")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_id = -1

    def start(self):
        if self.is_running():
            return False
        return self._init_docker()

    def stop(self, max_wait_s: float = 5):
        deploy = self.process_info.deploy_info
        if "stop_command" in deploy:
            stop_command = deploy["stop_command"]
            host = self.process_info.host
            net_host = self.manager.host_manager.get_net_host(host["id"])
            p = net_host.popen(stop_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            p.wait(max_wait_s)
        if self._proc is not None:
            self._proc.send_signal(SIGTERM)
            self._proc.wait(max_wait_s)
            if self._proc.poll() is not None:
                self._proc.kill()

    def join(self, max_wait_s: float = None):
        if not self.is_running():
            return True
        if self._proc is not None:
            self._proc.wait(max_wait_s)
        return not self.is_running()

    def kill(self):
        if self.is_running():
            self._proc.kill()
        return not self.is_running()

    def is_running(self) -> bool:
        if self._proc is None:
            return False
        return self._proc.poll() is None

    def get_pid(self) -> Optional[int]:
        if not self.is_running():
            return None
        return self._proc.pid

    def get_log_file(self) -> Path:
        hostname = self.manager.host_manager.get_hostname(self.process_info.host)
        return self._log_dir.joinpath(f"{hostname}_p{self.process_info.pid}_{self._log_id}.log")

    def _init_docker(self):
        config = self.process_info.host_config
        deploy = self.process_info.deploy_info
        self._write_config(config)
        self._start_process_in_docker(deploy)

    def _write_config(self, config):
        host = self.process_info.host
        config_dir = self.manager.host_manager.get_host_directory(host)
        config_file = config_dir.joinpath("config.json")
        with config_file.open("w") as f:
            f.write(json.dumps(config))
        return config_file

    def _start_process_in_docker(self, deploy):
        host = self.process_info.host
        if "start_command" in deploy:
            start_command = deploy["start_command"]
            net_host = self.manager.host_manager.get_net_host(host["id"])
            self._log_id += 1
            logfile = self.get_log_file().open("w")
            self._proc = net_host.popen(start_command, shell=True, stdout=logfile, stderr=STDOUT)
        else:
            print(f"Docker for Host {host['id']} has not start_command")

    def _get_container_name(self, host: Union[str, dict]):
        return f"mn.{self.manager.host_manager.get_hostname(host)}"
