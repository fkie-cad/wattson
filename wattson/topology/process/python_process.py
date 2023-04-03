import json
import os
import sys
from pathlib import Path
from signal import SIGTERM
from subprocess import STDOUT, Popen, TimeoutExpired
from typing import TYPE_CHECKING, Optional

from wattson.util.namespace import Namespace

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager
    from wattson.topology.process.process_info import ProcessInfo

from wattson.topology.process.wattson_process import WattsonProcess


class PythonProcess(WattsonProcess):
    """
    Provides an interface for the NetworkManager to control a Python-based process running on a Mininet host.
    """

    def __init__(self, process_info: 'ProcessInfo', manager: 'NetworkManager' = None, prepare_only: bool = False):
        super().__init__(process_info, manager)
        self._proc: Optional[Popen] = None
        self._log_dir = manager.host_manager.get_host_directory(process_info.host).joinpath("logs")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_id = -1
        self._prepare_only = prepare_only
        self._log_handle = None

    def start(self):
        if self.is_running():
            return False
        return self._init_process()

    def stop(self, max_wait_s: float = 5):
        if self._proc is not None:
            self._proc.send_signal(SIGTERM)
            try:
                max_wait_s = self.process_info.host.get("shutdown_wait", max_wait_s)
                if self.process_info.host.get("type") == "field":
                    max_wait_s = 0.05
                self._proc.wait(max_wait_s)
            except TimeoutExpired:
                hostname = self.manager.host_manager.get_hostname(self.process_info.host)
                print(f"Process of {hostname} did not exit after {max_wait_s}s, killing it...")
                self._proc.kill()
            self._clear_log_handle()

    def join(self, max_wait_s: float = None):
        if not self.is_running():
            return True
        if self._proc is not None:
            self._proc.wait(max_wait_s)
        return not self.is_running()

    def kill(self):
        if self.is_running():
            self._proc.kill()
        self._clear_log_handle()
        return not self.is_running()

    def is_running(self) -> bool:
        if self._proc is None:
            return False
        return self._proc.poll() is None

    def get_log_file(self) -> Path:
        hostname = self.manager.host_manager.get_hostname(self.process_info.host)
        return self._log_dir.joinpath(f"{hostname}_p{self.process_info.pid}_{self._log_id}.log")

    def get_pid(self) -> Optional[int]:
        if not self.is_running():
            return None
        return self._proc.pid

    def _init_process(self):
        config = self.process_info.host_config
        deploy = self.process_info.deploy_info
        host = self.process_info.host

        launch_config = {
            "hostid": self.process_info.host_id,
            "hostname": self.manager.host_manager.get_hostname(host),
            "module": deploy["module"],
            "class": deploy["class"],
            "config": config
        }

        config_file = self._python_deploy_prepare_config(launch_config)
        if self._prepare_only:
            return config_file
        wrapped_cmd = self._prepare_python_deploy_command(config_file)
        self._start_python_deploy_process(wrapped_cmd)
        return True

    def _python_deploy_prepare_config(self, launch_config) -> Path:
        host = self.process_info.host
        config_dir = self.manager.host_manager.get_host_directory(host)
        config_file = config_dir.joinpath("config.json")
        with config_file.open("w") as f:
            f.write(json.dumps(launch_config))
        return config_file

    def _prepare_python_deploy_command(self, config_file):
        hid = self.process_info.host_id
        cmd = f"{sys.executable} -m wattson.deployment {hid} {config_file.absolute()}"
        return cmd

    def _clear_log_handle(self):
        if self._log_handle is not None:
            try:
                self._log_handle.close()
            finally:
                self._log_handle = None

    def _start_python_deploy_process(self, cmd):
        self._clear_log_handle()
        host = self.process_info.host
        #namespace = self.manager.get_network_namespace(host)
        self._log_id += 1
        logfile = self.get_log_file().open("w", buffering=1)
        self._log_handle = logfile
        #self._proc = net_host.popen(cmd, shell=True, stdout=logfile, stderr=STDOUT)
        #self._proc = namespace.popen(cmd, shell=True, stderr=logfile, stdout=logfile, preexec_fn=os.setpgrp)
        self._proc = self.manager.deployment.popen(
            host, cmd, shell=True, stderr=logfile, stdout=logfile, preexec_fn=os.setpgrp
        )
