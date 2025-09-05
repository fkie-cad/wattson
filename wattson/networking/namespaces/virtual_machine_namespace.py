import json
import logging
import shlex
import subprocess
import time
import traceback
from typing import Optional, List, Tuple, Any, Callable, Union, Dict

import docker
from docker.models.containers import Container, ContainerCollection

from wattson.networking.namespaces.docker_popen import DockerPopen
from wattson.networking.namespaces.namespace import Namespace
from wattson.networking.namespaces.nested_argument import NestedArgument
from wattson.networking.namespaces.qemu_popen import QemuPopen


class VirtualMachineNamespace(Namespace):
    def __init__(self, name: str, logger: Optional[logging.Logger] = None, domain: Optional[str] = None):
        import libvirt
        from libvirt import virDomain
        
        super().__init__(name, logger)
        self._connection = libvirt.open('qemu:///system')
        if domain is None:
            domain = name
        self.domain_name = domain
        self._domain: Optional[virDomain] = self._connection.lookupByName(self.domain_name)
        self._os: Optional[str] = None

    @property
    def is_network_namespace(self) -> bool:
        return False

    def exists(self) -> bool:
        return True   # self._domain ?

    def is_running(self):
        return self._domain.isActive()

    def get_os(self) -> str:
        if self._os is None:
            qemu_command = {
                "execute": "guest-get-osinfo"
            }
            output = subprocess.check_output(["virsh", "-c" "qemu:///system", "qemu-agent-command", self.domain_name, json.dumps(qemu_command)])
            data = json.loads(output)
            self._os = data.get("return", {}).get("id", "unknown")
            if self._os == "mswindows":
                self._os = "windows"
            if self._os in ["ubuntu", "arch"]:
                self._os = "linux"
        return self._os

    def loopback_up(self) -> bool:
        """
        Sets the loopback interface in the namespace up :return:

        """
        if self.get_os() == "linux":
            return super().loopback_up()
        elif self.get_os() == "windows":
            self.logger.debug(f"No loopback support for Windows")
            return False
        else:
            self.logger.error(f"Unsupported OS: {self.get_os()}")
            return False

    def get_interfaces(self) -> List[Dict]:
        tries = 4
        timeout = 1
        current_try = 1
        while current_try <= tries:
            try:
                qemu_command = {
                    "execute": "guest-network-get-interfaces"
                }
                output = subprocess.check_output(
                    ["virsh", "-c", "qemu:///system", "qemu-agent-command", self.domain_name, json.dumps(qemu_command)],
                    stderr=subprocess.DEVNULL
                )
                data = json.loads(output)
                return data.get("return", [])
            except:
                current_try += 1
                time.sleep(timeout)
        raise Exception("Could not receive interfaces")

    def create(self, clean: bool = True) -> bool:
        self._domain.create()
        # TODO: Return value?
        return True

    def clean(self) -> bool:
        self.shutdown()
        if self.exists():
            # Destroy?
            return False
            self._domain.destroy()
        return True

    def shutdown(self):
        if self._domain and self._domain.isActive():
            self._domain.shutdown()
        self._connection.close()

    def thread_attach(self):
        raise NotImplementedError("This function is not available for Virtual Machine Namespaces")

    def wait_until_available(self, timeout: Optional[float] = 30, poll_interval: float = 1) -> bool:
        start_time = time.perf_counter()
        self.logger.debug(f"Waiting for {self.domain_name} to boot")
        while True:
            try:
                qemu_config = {
                    "execute": "guest-ping",
                }
                subprocess.check_call(
                    ["virsh", "-c", "qemu:///system", "qemu-agent-command", self.domain_name, json.dumps(qemu_config)],
                    stderr=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL
                )
                return True
            except subprocess.CalledProcessError:
                pass

            if timeout is not None:
                wait_time = time.perf_counter() - start_time
                if wait_time > timeout:
                    self.logger.error(f"Boot process timed out")
                    return False
            time.sleep(poll_interval)

    def exec(self, command, **kwargs) -> Tuple[bool, List[str]]:
        # kwargs.setdefault("docker_wrap", False)
        return self._exec(command, **kwargs)

    def popen(self, cmd: Union[str, List[str]], **kwargs) -> subprocess.Popen:
        if not self.exists():
            raise RuntimeError("No matching container found")

        if not self.wait_until_available():
            self.logger.error(f"VM guest agent is not available")

        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if cmd[0] != "sudo" and self.get_os() == "linux":
            cmd.insert(0, "sudo")
        return QemuPopen(cmd, namespace=self)

    def _exec(self, cmd, stdout=None, stderr=None, **kwargs) -> Tuple[bool, List[str]]:
        if not self.exists():
            return False, []

        if not isinstance(cmd, list):
            shlex.split(cmd)

        try:
            p = self.popen(cmd, stdout=stdout, stderr=stderr, universal_newlines=True, **kwargs)
        except subprocess.CalledProcessError as e:
            return False, [traceback.format_exc()]
        output, error = p.communicate()
        lines = []
        for line in output.splitlines():
            lines.append(str(line))
        return p.returncode == 0, lines

    def call(self, function: Callable, arguments: Optional[Tuple] = None) -> Any:
        raise NotImplementedError("Calling python functions in a virtual machine is not possible")

    def from_pid(self, pid: Optional[int] = None, clean: bool = False) -> bool:
        return False

    def process_attach(self, pid: Optional[int] = None):
        raise NotImplementedError("Process attachment not possible for VirtualMachineNamespace")
