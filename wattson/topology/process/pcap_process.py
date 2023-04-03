import json
import os
import shlex
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from signal import SIGTERM
from subprocess import STDOUT, Popen
from typing import TYPE_CHECKING, Optional

from wattson.topology import network_utils

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager
    from wattson.topology.process.process_info import ProcessInfo

from wattson.topology.process.wattson_process import WattsonProcess


class PcapProcess(WattsonProcess):
    """
    Provides an interface for the NetworkManager to control a Python-based process running on a Mininet host.
    """

    def __init__(self, process_info: 'ProcessInfo', manager: 'NetworkManager' = None,
                 interface: str = "any", auto_restart: bool = False):
        super().__init__(process_info, manager)
        self._proc: Optional[Popen] = None
        self._log_dir = manager.host_manager.get_host_directory(process_info.host).joinpath("logs")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_id = -1
        self._pcap_dir = self._log_dir.joinpath("pcaps")
        self._auto_restart = auto_restart
        host = self.process_info.host
        if WattsonProcess.is_docker_host(host):
            self._pcap_dir = Path("/wattson_tmp")
        else:
            self._pcap_dir.mkdir(parents=True, exist_ok=True)
        self._pcap_id = -1
        self._interface = interface
        self._is_switch = self.process_info.host["type"] == "switch"
        self._watchdog = None
        self._watchdog_event = threading.Event()

    def start(self):
        if self.is_running():
            return False
        return self._init_process()

    def stop(self, max_wait_s: float = 5):
        if self._proc is not None:
            self._watchdog_event.set()
            self._watchdog = None
            self._proc.send_signal(SIGTERM)
            self._proc.wait(max_wait_s)
            if self._proc.poll() is not None:
                self._proc.kill()
            if self._is_switch:
                # Cleanup Interface
                self._clear_span_interface()
                #cmd = f"ip link delete {self._interface}"
                #self.manager.exec_with_output(cmd)

    def join(self, max_wait_s: float = None):
        if not self.is_running():
            return True
        if self._proc is not None:
            self._proc.wait(max_wait_s)
        return not self.is_running()

    def kill(self):
        if self.is_running():
            self._watchdog_event.set()
            self._watchdog = None
            self._proc.kill()
        return not self.is_running()

    def is_running(self) -> bool:
        if self._proc is None:
            return False
        return self._proc.poll() is None

    def get_log_file(self) -> Path:
        hostname = self.manager.host_manager.get_hostname(self.process_info.host)
        return self._log_dir.joinpath(f"{hostname}_p{self.process_info.pid}_{self._interface}_{self._pcap_id}.log")

    def get_pcap_file(self) -> Path:
        hostname = self.manager.host_manager.get_hostname(self.process_info.host)
        return self._pcap_dir.joinpath(f"{hostname}_{self._interface}_{self._pcap_id}.pcap")

    def get_pid(self) -> Optional[int]:
        if not self.is_running():
            return None
        return self._proc.pid

    def _watchdog_loop(self):
        while not self._watchdog_event.is_set():
            if not self.is_running():
                # Process died
                print(f"PCAP process of {self.process_info.host['id']} died, restarting")
                self._start_process()
            time.sleep(1)

    def _init_process(self):
        self._pcap_id += 1
        if self._is_switch:
            self._create_span_interface()
            # cmd = f"ovs-tcpdump -i {bridge} --span --mirror-to {tap} --dump-cmd tshark -n -w {self.get_pcap_file().absolute().__str__()}"
            # self._start_pcap_process(cmd)
        return self._start_process()

    def _start_process(self):
        tshark_path = shutil.which("tshark")
        tcpdump_path = shutil.which("tcpdump")
        if tshark_path is None and tcpdump_path is None:
            print(f"[ERROR] Neither tshark nor tcpdump could be found")
            return False
        if tcpdump_path is not None:
            binary = tcpdump_path
            args = "-n -K"
        else:
            args = "-n"
            binary = tshark_path
            self.get_pcap_file().touch(mode=0o777)
        cmd = f"{binary} -i {self._interface} -w {self.get_pcap_file().absolute().__str__()} {args}"
        if not self._start_pcap_process(cmd):
            if self._is_switch:
                self._clear_span_interface()
            return False
        if self._auto_restart:
            self._watchdog_event.clear()
            if self._watchdog is None:
                self._watchdog = threading.Thread(target=self._watchdog_loop)
                self._watchdog.start()
        return True

    def _start_pcap_process(self, cmd):
        def preexec_function():
            # Ignore the SIGINT signal by setting the handler to the standard
            # signal handler SIG_IGN.
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        host = self.process_info.host
        hostname = self.manager.host_manager.get_hostname(host)
        net_host = self.manager.get_mininet().get(hostname)
        logfile = self.get_log_file().open("w")
        try:
            if self._is_switch:
                cmd = shlex.split(cmd)
                self._proc = subprocess.Popen(cmd, stdout=logfile, stderr=STDOUT, preexec_fn=os.setpgrp)
            else:
                self._proc = self.manager.get_network_namespace(host).popen(cmd, stdout=logfile, stderr=STDOUT,
                                                                            preexec_fn=os.setpgrp)
            return True
        except FileNotFoundError as e:
            print(f"[ERROR] Could not execute {cmd}")
            print(f"{e=}")
            return False

    def _create_span_interface(self):
        if not self._is_switch:
            return False
        bridge = self.manager.ghn(self.process_info.host)
        # cmd = f"ip link delete {self._interface}"
        # self.manager.exec_with_output(cmd, True)
        cmd = f"ip link add {self._interface} type dummy"
        self.manager.exec_with_output(cmd, True)
        if not network_utils.wait_for_interface(self._interface, 10):
            print(f"[WARNING] Interface {self._interface} not found after 10 seconds")
        cmd = f"ip link set dev {self._interface} up"
        self.manager.exec_with_output(cmd, True)
        if not network_utils.wait_for_interface(self._interface, 10):
            print(f"[WARNING] Interface {self._interface} not found after 10 seconds")
        # cmd = f"ovs-vsctl add-port {bridge} {self._interface}"
        # self.manager.exec_with_output(cmd, True)
        cmd = " ".join([
            f"ovs-vsctl add-port {bridge} {self._interface}",
            f"-- --id=@p get port {self._interface}",
            f"-- --id=@m create mirror name=wattson select-all=true output-port=@p",
            f"-- set bridge {bridge} mirrors=@m",
        ])
        self.manager.exec_with_output(cmd, True)

    def _clear_span_interface(self):
        if not self._is_switch:
            return False
        bridge = self.manager.ghn(self.process_info.host)
        cmd = f"ovs-vsctl clear bridge {bridge} mirrors"
        self.manager.exec_with_output(cmd, True)
        # cmd = f"ovs-vsctl del-port {bridge} {self._interface}"
        # self.manager.exec_with_output(cmd, True)
        cmd = f"ip link delete {self._interface}"
        self.manager.exec_with_output(cmd, True)
