import json
import os
from pathlib import Path
from signal import SIGTERM
from subprocess import STDOUT, Popen
from typing import TYPE_CHECKING, Optional

from wattson.util.misc import get_console_and_shell

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager
    from wattson.topology.process.process_info import ProcessInfo

from wattson.topology.process.wattson_process import WattsonProcess


class ScreenProcess(WattsonProcess):
    """
    Provides an interface for the NetworkManager to control a screen process running on a Mininet host.
    """
    screen_id = 0

    def __init__(self, process_info: 'ProcessInfo', manager: 'NetworkManager' = None, screen_name: Optional[str] = None):
        super().__init__(process_info, manager)
        self._proc: Optional[Popen] = None
        self._log_dir = manager.host_manager.get_host_directory(process_info.host).joinpath("logs")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_id = -1
        self._screen_id = ScreenProcess.screen_id
        ScreenProcess.screen_id += 1
        self._screen_name = screen_name
        if self._screen_name is None:
            hostname = self.manager.host_manager.get_hostname(self.process_info.host)
            self._screen_name = f"{hostname}-s{self._screen_id}"

    def get_screen_id(self) -> str:
        return self._screen_name

    def start(self):
        if self.is_running():
            return False
        return self._init_process()

    def stop(self, max_wait_s: float = 5):
        if self._proc is not None:
            self._terminate_screen(self._screen_name, max_wait_s)
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

    def get_log_file(self) -> Path:
        hostname = self.manager.host_manager.get_hostname(self.process_info.host)
        return self._log_dir.joinpath(f"{hostname}_p{self.process_info.pid}_screen_{self._screen_id}_{self._log_id}.log")

    def get_pid(self) -> Optional[int]:
        if not self.is_running():
            return None
        return self._proc.pid

    def _init_process(self):
        self._start_screen_process(self._screen_name)
        return True

    def _terminate_screen(self, screen_id: str, max_wait_s: float):
        cmd = ["screen", "-XS", screen_id, "quit"]
        host = self.process_info.host
        hostname = self.manager.host_manager.get_hostname(host)
        net_host = self.manager._net.get(hostname)
        p = net_host.popen(cmd, shell=True)
        try:
            p.wait(max_wait_s)
        except TimeoutError:
            p.kill()

    def _start_screen_process(self, screen_id: str):
        self._log_id += 1
        host = self.process_info.host
        hostname = self.manager.host_manager.get_hostname(host)
        net_host = self.manager._net.get(hostname)
        logfile = self.get_log_file().open("w")
        pid = os.getpid()
        _, shell = get_console_and_shell(pid, allow_missing=True)
        if shell is None:
            print(f"No valid shell could be found")
            return False
        cwd: str = host.get("cwd", ".")
        if cwd.startswith("/"):
            cwd = str(Path(cwd).absolute())
        else:
            cwd = str(self.manager.path.joinpath(cwd).absolute())
        cmd = ["screen", "-S", f"{screen_id}", "-dm", shell]
        self._proc = net_host.popen(cmd, shell=True, stdout=logfile, stderr=STDOUT, cwd=cwd)
