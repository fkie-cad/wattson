import json
import logging
import multiprocessing.pool
import pwd
import queue
import shlex
import shutil
import subprocess
import ctypes
import os
import sys
import threading
from pathlib import Path
from typing import Optional, Tuple, List, Union, Callable, Any

import wattson.util


class Namespace:
    NAMESPACE_PATH_VAR: Path = Path("/var/run/netns/")
    NAMESPACE_PATH_RUN: Path = Path("/run/netns/")

    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        self.name = name
        self.id = None
        self._attached_fd = None
        self.logger = logger
        if self.logger is None:
            self.logger = wattson.util.get_logger(f"Namespace", "Namespace").getChild(name)
        self._worker_threads: List[threading.Thread] = []
        self._max_worker_threads = 100
        self._call_pool: Optional[multiprocessing.pool.ThreadPool] = None
        self._call_terminate_event = threading.Event()

    def __del__(self):
        if self._attached_fd is not None:
            os.close(self._attached_fd)
        if self._call_pool is not None:
            self._call_pool.close()

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
        Cleans up the networking namespace
        :return:
        """
        succ = self._exec(f"ip netns delete {self.name}")[0]
        self._exec(f"rm -r /etc/netns/{self.name}")
        return succ

    def thread_attach(self):
        """
        Moves the calling thread to this networking namespace
        :return:
        """
        libc = ctypes.CDLL("libc.so.6")
        ns_file = Namespace.NAMESPACE_PATH_VAR.joinpath(self.name)
        if self._attached_fd is None:
            self._attached_fd = os.open(ns_file.__str__(), os.O_RDONLY)
        libc.setns(self._attached_fd, 0)

    def process_attach(self, pid: Optional[int] = None):
        """
        Moves the given process to this networking namespace.
        If no PID is given, moves the current process.
        :param pid: The PID of the process to attach. None for current process
        :return:
        """
        if pid is None:
            pid = os.getpid()
        raise NotImplementedError("Moving processes between namespaces is not (yet) supported")

    def exec(self, command: Union[str, List[str]]) -> Tuple[bool, List[str]]:
        """
        Executes a command in the network namespace
        :param command:
        :return:
        """
        if isinstance(command, list):
            command = " ".join(command)
        cmd = f"ip netns exec {self.name} {command}"
        return self._exec(cmd)

    def popen(self, cmd: Union[str, List[str]], **kwargs) -> subprocess.Popen:
        """
        Spawns a new process in the network namespace
        :param cmd: The cmd to execute
        :param kwargs:
        :return: Popen
        """
        netns_cmd = kwargs.pop("wrap_command", f"ip netns exec {self.name}")
        stdout = kwargs.pop("stdout", sys.stdout)
        stderr = kwargs.pop("stderr", sys.stderr)
        if isinstance(cmd, list):
            cmd = " ".join(cmd)

        as_user = kwargs.pop("as_user", None)
        as_user_cmd = " "
        if as_user is not None:
            as_user_cmd = f" sudo -Eu {as_user} "
        env = kwargs.pop("env", os.environ.copy())

        if as_user is not None:
            pw_record = pwd.getpwnam(as_user)
            env["HOME"] = pw_record.pw_dir
            env["LOGNAME"] = pw_record.pw_name
            env["USER"] = pw_record.pw_name

        kwargs["env"] = env

        use_shlex = kwargs.pop("shlex", True)
        shell = kwargs.pop("shell", False)
        if shell:
            cmd = f"{os.environ['SHELL']} -c '{cmd}'"
        cmd = netns_cmd + as_user_cmd + cmd
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
        ret, _ = self.exec("ip link set dev lo up")
        return ret == 0

    def _exec(self, cmd, stdout=None, stderr=None) -> Tuple[bool, List[str]]:
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

    def _call_worker(self):
        self.thread_attach()

    def call(self, function: Callable, arguments: Optional[Tuple] = None) -> Any:
        """
        Executes a function in this namespace.
        @param function: The function (callable) to run
        @param arguments: (Optional) parameters to pass to the function
        @return: The return value of the called function
        """

        if self._call_pool is None:
            self._call_pool = multiprocessing.pool.ThreadPool(processes=self._max_worker_threads)

        if arguments is None:
            arguments = []

        result = None

        def wrapper(*_arguments):
            self.thread_attach()
            return function(*_arguments)

        return self._call_pool.apply(wrapper, args=arguments)

    def from_pid(self, pid: Optional[int] = None, clean: bool = False) -> bool:
        if pid is None:
            pid = os.getpid()
        if self.exists():
            if clean:
                self.clean()
            else:
                self.logger.error("Namespace already exists")
                return False
        # Create /run/netns folder setup
        if not self.NAMESPACE_PATH_RUN.exists():
            dummy_ns = Namespace("wattson-dummy")
            dummy_ns.create()
            dummy_ns.clean()

        run_link = self.NAMESPACE_PATH_RUN.joinpath(self.name)
        cmd = f"touch {run_link}"
        success, out = self._exec(cmd)
        if not success:
            self.logger.error("\n".join(out))
            return False
        cmd = f"chmod 0 {run_link}"
        success, out = self._exec(cmd)
        if not success:
            self.logger.error("\n".join(out))
            return False
        ns_ref = Path("/proc").joinpath(str(pid)).joinpath("ns/net")
        cmd = f"mount -o bind {ns_ref.__str__()} {run_link.__str__()}"
        success, out = self._exec(cmd)
        if not success:
            self.logger.error("\n".join(out))
            return False
        return True

    @staticmethod
    def get_namespaces():
        ns = Namespace("None")
        namespace_names = ns._exec(f"ip netns list")[1]
        namespaces = [Namespace(n) for n in namespace_names]
        return namespaces
