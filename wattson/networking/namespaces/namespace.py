import json

import pyroute2
import logging
import multiprocessing.pool
import pwd
import shlex
import shutil
import subprocess
import ctypes
import os
import sys
import threading
import traceback
from pathlib import Path
from subprocess import CalledProcessError
from typing import Optional, Tuple, List, Union, Callable, Any, Dict

import wattson.util
from wattson.networking.namespaces.nested_argument import NestedArgument


class Namespace:
    NAMESPACE_PATH_VAR: Path = Path("/var/run/netns/")
    NAMESPACE_PATH_RUN: Path = Path("/run/netns/")
    NAMESPACE_PATH_ETC: Path = Path("/etc/netns/")

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

    @property
    def is_network_namespace(self) -> bool:
        return True

    def create(self, clean: bool = True) -> bool:
        """
        Creates a new networking namespace with the specified name

        Args:
            clean (bool, optional):
                Whether to try to clean any existing namespace with the same name
                (Default value = True)
        """
        if clean:
            self.clean()
        success = self._exec(f"ip netns add {self.name}")[0]
        # Create directories and files
        Namespace.NAMESPACE_PATH_ETC.joinpath(self.name).mkdir(parents=True, exist_ok=True)
        shutil.copy(Path("/etc/resolv.conf"), Namespace.NAMESPACE_PATH_ETC.joinpath(self.name).joinpath("resolv.conf"))
        # Reserve ports
        reserved_ports = ["2404", "51000-51010"]
        try:
            self.logger.debug(f"Reserving ports: {', '.join(reserved_ports)}")
            subprocess.check_call(f"sysctl net.ipv4.ip_local_reserved_ports={','.join(reserved_ports)}",
                                  shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except CalledProcessError:
            self.logger.error(f"Could not reserve ports.")
        return success

    def set_name_servers(self, servers: List[str], search_domain: Optional[str] = None):
        lines = []
        for server in servers:
            lines.append(f"nameserver {server}")
        if search_domain is not None:
            lines.append(f"search {search_domain}")
        return self._write_to_file(Path("/etc/resolv.conf"), "\n".join(lines))

    def _write_to_file(self, file_path: Path, content: str) -> bool:
        content = shlex.quote(content)
        code0, _ = self.exec(["/bin/bash", "-c", f"echo {content} > {str(file_path.absolute())}"])
        return code0

    def _read_from_file(self, file_path: Path) -> Optional[str]:
        code0, lines = self.exec(["cat", str(file_path.absolute())])
        if code0:
            return "\n".join(lines)
        return None

    def file_put_contents(self, file_path: Path, content: str) -> bool:
        try:
            self._write_to_file(file_path, content)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return False
        return True

    def file_get_contents(self, file_path: Path) -> Optional[str]:
        return self._read_from_file(file_path=file_path)

    def clean(self) -> bool:
        """
        Cleans up the networking namespace :return:

        """
        succ = self._exec(f"ip netns delete {self.name}")[0]
        self._exec(f"rm -r /etc/netns/{self.name}")
        return succ

    def thread_attach(self):
        """
        Moves the calling thread to this networking namespace :return:

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

        Args:
            pid (Optional[int], optional):
                The PID of the process to attach. None for current process
                (Default value = None)
        """
        if pid is None:
            pid = os.getpid()
        raise NotImplementedError("Moving processes between namespaces is not (yet) supported")

    def exec(self, command: Union[str, List[str]], **kwargs) -> Tuple[bool, List[str]]:
        """
        Executes a command in the network namespace

        Args:
            command (Union[str, List[str]]):
                
            **kwargs:
                
        """
        if isinstance(command, str):
            command = shlex.split(command)
        cmd = ["ip", "netns", "exec", self.name] + command
        return self._exec(cmd)

    def popen(self, cmd: Union[str, List[str]], **kwargs) -> subprocess.Popen:
        """
        Spawns a new process in the network namespace

        Args:
            cmd (Union[str, List[str]]):
                The cmd to execute
            **kwargs:
                

        Returns:
            subprocess.Popen: Popen
        """
        # print(f"Default namespace POpen {cmd}")
        netns_cmd = kwargs.pop("wrap_command", f"ip netns exec {self.name}")
        stdout = kwargs.pop("stdout", sys.stdout)
        stderr = kwargs.pop("stderr", sys.stderr)

        if isinstance(netns_cmd, str):
            netns_cmd = shlex.split(netns_cmd)
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)

        final_cmd = cmd

        popen_class = kwargs.pop("popen", subprocess.Popen)

        env = kwargs.pop("env", os.environ.copy())

        as_user = kwargs.pop("as_user", None)
        as_user_cmd = " "
        if as_user is not None:
            final_cmd = ["sudo", "-Eu", as_user, NestedArgument(final_cmd)]
            pw_record = pwd.getpwnam(as_user)
            env["HOME"] = pw_record.pw_dir
            env["LOGNAME"] = pw_record.pw_name
            env["USER"] = pw_record.pw_name

        kwargs["env"] = env

        shell = kwargs.pop("shell", False)
        if shell is not False:
            if shell is True:
                shell = os.environ.get('SHELL', "/bin/bash")
            final_cmd = [shell, "-c", str(NestedArgument(final_cmd))]

        final_cmd = netns_cmd + final_cmd

        return popen_class(final_cmd, stdout=stdout, stderr=stderr, **kwargs)

    def exists(self) -> bool:
        """
        Checks whether the namespace exists :return: True if the namespace exists

        """
        # Just execute a dummy operation
        return self.exec("echo")[0]

    def loopback_up(self) -> bool:
        """
        Sets the loopback interface in the namespace up :return:

        """
        ret, _ = self.exec("ip link set dev lo up")
        return ret == 0

    def _exec(self, cmd, stdout=None, stderr=None) -> Tuple[bool, List[str]]:
        if stdout is None:
            stdout = subprocess.PIPE
        if stderr is None:
            stderr = subprocess.STDOUT
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, universal_newlines=True)
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

        Args:
            function (Callable):
                The function (callable) to run
            arguments (Optional[Tuple], optional):
                (Optional) parameters to pass to the function
                (Default value = None)

        Returns:
            Any: The return value of the called function
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

    """
    Interface handling
    """
    def if_add(self, interface_name: str, interface_type: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "add", interface_name, "type", interface_type])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to create {interface_name} of type {interface_type}\n" + "\n".join(lines))
        return code0

    def if_delete(self, interface_name: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "del", interface_name])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to delete {interface_name}\n" + "\n".join(lines))
        return code0

    def if_set_namespace(self, interface_name: str, namespace: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "set", interface_name, "netns", namespace])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to set namespace for {interface_name}\n" + "\n".join(lines))
        return code0

    def if_up(self, interface_name: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "set", "dev", interface_name, "up"])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to set link {interface_name} up\n" + "\n".join(lines))
        return code0

    def if_down(self, interface_name: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "set", "dev", interface_name, "down"])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to set link {interface_name} down\n" + "\n".join(lines))
        return code0

    def if_flush_ip(self, interface_name: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "addr", "flush", "dev", interface_name])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to flush {interface_name}\n" + "\n".join(lines))
        return code0

    def if_set_ip(self, interface_name: str, ip_address: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "addr", "add", ip_address, "dev", interface_name])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed set IP for {interface_name} to {ip_address}\n" + "\n".join(lines))
        return code0

    def if_set_mac(self, interface_name: str, mac_address: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "set", "dev", interface_name, "address", mac_address])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to set MAC for {interface_name} to {mac_address}\n" + "\n".join(lines))
        return code0

    def if_rename(self, interface_name_old: str, interface_name_new: str, enable_logging: bool = False) -> bool:
        code0, lines = self.exec(["ip", "link", "set", interface_name_old, "name", interface_name_new])
        if not code0 and enable_logging:
            self.logger.warning(f"Failed to rename {interface_name_old} to {interface_name_new}\n" + "\n".join(lines))
        return code0

    def _if_parse_info(self, info: dict) -> dict:
        ip_addresses = []
        for address_info in info.get("addr_info", []):
            addr_type = "inet"
            if address_info.get("family") == "inet":
                addr_type = "ipv4"
            elif address_info.get("family") == "inet6":
                addr_type = "ipv6"
            ip_addresses.append({
                "ip-address-type": addr_type,
                "ip-address": address_info.get("local"),
                "prefix": address_info.get("prefixlen")
            })
        return {
            "name": info["ifname"],
            "ip-addresses": ip_addresses,
            "statistics": {},
            "hardware-address": info.get("address"),
            "index": info.get("ifindex"),
        }

    def if_exists(self, interface_name: str) -> bool:
        return self.if_info(interface_name) is not None
        for interface in self.if_list_existing():
            if interface["name"] == interface_name:
                return True
        return False


    def if_info(self, interface_name: str, try_num = 0, max_tries: int = 10, enable_logging: bool = False) -> Optional[dict]:
        if try_num >= max_tries:
            return None
        code0, lines = self.exec(["ip", "--json", "link", "show", interface_name])
        if not code0:
            if "does not exist" in lines[0]:
                return None
            if enable_logging:
                self.logger.error(f"Could not load interface information for {interface_name}\n" + "\n".join(lines))
            return None

        if "Dump was interrupted and may be inconsistent" in "\n".join(lines):
            return self.if_info(interface_name, try_num + 1, max_tries, enable_logging)
        try:
            data = json.loads("\n".join(lines))
            if not isinstance(data, list) or len(data) != 1:
                if enable_logging:
                    self.logger.error(f"Unexpected info result for {interface_name}\n" + "\n".join(lines))
                return None
            data = data[0]
        except json.JSONDecodeError:
            if enable_logging:
                self.logger.error(f"Could not parse interface information for {interface_name}\n" + "\n".join(lines))
            return None
        return self._if_parse_info(data)

    def if_list_existing(self, try_num: int = 0, max_tries: int = 10, retry_error_message: str = "", context: str = "") -> List[Dict]:
        if try_num >= max_tries:
            self.logger.error(f"[{context}] Could not load interface information - maximum number of tries ({try_num} / {max_tries}) exceeded ({retry_error_message})")
            return []

        code0, lines = self.exec(["ip", "--json", "a"], stderr=subprocess.PIPE)
        if not code0:
            self.logger.error(f"[{context}] Could not load interface information\n {'\n'.join(lines)}")
            return []

        # Check for interrupted dump
        if "Dump was interrupted and may be inconsistent" in "\n".join(lines):
            #err = f"[{context}] Dump was interrupted and may be inconsistent: \n" + "\n".join(lines)
            return self.if_list_existing(try_num=try_num + 1, retry_error_message="Inconsistent dump", context=context)
            return self.if_list_existing(try_num=try_num + 1, retry_error_message=err, context=context)

        try:
            data = json.loads("\n".join(lines))
        except json.JSONDecodeError:
            self.logger.error(f"[{context}] Could not load interface information ({self.name})")
            self.logger.error("\n".join(lines))
            return []
        # Sanitize - follow QEMU Agent format
        interfaces = []
        for interface_data in data:
            interfaces.append(self._if_parse_info(interface_data))
        return interfaces
