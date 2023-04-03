import logging
import shlex
import subprocess
import ctypes
import os
import sys
from pathlib import Path
from typing import Optional, Tuple, List

import wattson.util


class Namespace:
    NAMESPACE_PATH_VAR: Path = Path("/var/run/netns/")
    NAMESPACE_PATH_RUN: Path = Path("/run/netns/")

    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        self.name = name
        self.id = None
        self.logger = logger
        if self.logger is None:
            self.logger = wattson.util.get_logger(f"Namespace", "Namespace").getChild(name)

    def create(self, clean: bool = True) -> bool:
        """
        Creates a new networking namespace with the specified name
        :param clean: Whether to try to clean any existing namespace with the same name
        :return:
        """
        if clean:
            self.clean()
        return self._exec(f"ip netns add {self.name}")[0]

    def clean(self) -> bool:
        """
        Cleans uo the networking namespace
        :return:
        """
        return self._exec(f"ip netns delete {self.name}")[0]

    def thread_attach(self):
        """
        Moves the calling thread to this networking namespace
        :return:
        """
        libc = ctypes.CDLL("libc.so.6")
        ns_file = Namespace.NAMESPACE_PATH_VAR.joinpath(self.name)
        ns_fd = os.open(ns_file.__str__(), os.O_RDONLY)
        libc.setns(ns_fd, 0)

    def process_attach(self, pid: Optional[int] = None):
        """
        Moves the given process to this networking namespace.
        If no PID is given, moves the current process.
        :param pid: The PID of the process to attach. None for current process
        :return:
        """
        raise NotImplementedError("Moving processes between namespaces is not (yet) supported")

    def exec(self, command) -> Tuple[bool, List[str]]:
        """
        Executes a command in the network namespace
        :param command:
        :return:
        """
        cmd = f"ip netns exec {self.name} {command}"
        return self._exec(cmd)

    def popen(self, cmd, **kwargs) -> subprocess.Popen:
        """
        Spawns a new process in the network namespace
        :param cmd: The cmd to execute
        :param kwargs:
        :return: Popen
        """
        netns_cmd = f"ip netns exec {self.name} "
        stdout = kwargs.get("stdout", sys.stdout)
        stderr = kwargs.get("stderr", sys.stderr)
        kwargs.pop("stderr", None)
        kwargs.pop("stdout", None)
        use_shlex = kwargs.pop("shlex", True)
        shell = kwargs.pop("shell", False)
        if shell:
            cmd = f"{os.environ['SHELL']} -c '{cmd}'"
        cmd = netns_cmd + cmd
        if use_shlex:
            cmd = shlex.split(cmd)
        return subprocess.Popen(cmd, stdout=stdout, stderr=stderr, **kwargs)

    def exists(self) -> bool:
        """
        Checks whether the namespace exists
        :return: True if the namespace exists
        """
        # Just execute a dummy operation
        return self.exec("echo")[0]

    def loopback_up(self) -> bool:
        """
        Sets the loopback interface in the namespace up
        :return:
        """
        return self.exec("ip link set dev lo up")[0]

    @staticmethod
    def _exec(cmd, stdout=None, stderr=None) -> Tuple[bool, List[str]]:
        if stdout is None:
            stdout = subprocess.PIPE
        if stderr is None:
            stderr = subprocess.STDOUT
        p = subprocess.Popen(shlex.split(cmd), stdout=stdout, stderr=stderr, universal_newlines=True)
        output, error = p.communicate()
        lines = []
        for line in output.splitlines():
            lines.append(str(line))
        return p.returncode == 0, lines

    def from_pid(self, pid: Optional[int] = None, clean: bool = False) -> bool:
        if pid is None:
            pid = os.getpid()
        if self.exists():
            if clean:
                self.clean()
            else:
                return False
        run_link = self.NAMESPACE_PATH_RUN.joinpath(self.name)
        cmd = f"touch {run_link}"
        if not self._exec(cmd)[0]:
            return False
        ns_ref = Path("/proc").joinpath(str(pid)).joinpath("ns/net")
        cmd = f"mount -o bind {ns_ref.__str__()} {run_link.__str__()}"
        if not self._exec(cmd)[0]:
            return False
        return True

    @staticmethod
    def get_namespaces():
        namespace_names = Namespace._exec(f"ip netns list")[1]
        namespaces = [Namespace(n) for n in namespace_names]
        return namespaces
