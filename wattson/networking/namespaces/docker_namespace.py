import logging
import shlex
import subprocess
from typing import Optional, List, Tuple, Any, Callable, Union

import docker
from docker.models.containers import Container, ContainerCollection

from wattson.networking.namespaces.docker_popen import DockerPopen
from wattson.networking.namespaces.namespace import Namespace
from wattson.networking.namespaces.nested_argument import NestedArgument


class DockerNamespace(Namespace):
    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        super().__init__(name, logger)
        self._docker_client = docker.DockerClient()
        self._container = None

    def get_container(self) -> Optional[Container]:
        if self._container is None:
            for container in self._docker_client.containers.list(filters={"status": "running"}):
                if container.name == self.name:
                    self._container = container
        return self._container

    def exists(self) -> bool:
        return self.get_container() is not None

    def create(self, clean: bool = True) -> bool:
        # Cannot create a Docker Container via the namespace interface
        return False

    def clean(self) -> bool:
        if self.exists():
            self.get_container().kill()
        return True

    def thread_attach(self):
        raise NotImplementedError("This function is not available for Docker Namespaces")

    def exec(self, command, **kwargs) -> Tuple[bool, List[str]]:
        kwargs.setdefault("docker_wrap", False)
        return self._exec(command, **kwargs)

    def popen(self, cmd: Union[str, List[str]], **kwargs) -> subprocess.Popen:
        if not self.exists():
            raise RuntimeError("No matching container found")
        # print(f"Docker namespace POpen {cmd}")
        custom_command = cmd
        docker_command = f"docker exec -t {self.name}"

        cwd = kwargs.pop("cwd", None)
        if cwd is not None:
            custom_command = ["sh", "-c", NestedArgument(["cd", cwd, ";"] + custom_command)]

        # Handle docker bug that does not forward signals to docker containers
        # https://github.com/moby/moby/issues/9098
        kwargs["popen"] = DockerPopen
        kwargs["shell"] = False
        kwargs["namespace"] = self
        return super().popen(custom_command, wrap_command=docker_command, **kwargs)

    def _exec(self, cmd, stdout=None, stderr=None, **kwargs) -> Tuple[bool, List[str]]:
        if not self.exists():
            return False, []

        if stdout is None:
            stdout = subprocess.PIPE
        if stderr is None:
            stderr = subprocess.STDOUT

        if not isinstance(cmd, list):
            shlex.split(cmd)

        kwargs.pop("stdout", None)
        kwargs.pop("stderr", None)
        kwargs.pop("universal_newlines", None)

        p = self.popen(cmd, stdout=stdout, stderr=stderr, universal_newlines=True, **kwargs)
        output, error = p.communicate()
        lines = []
        for line in output.splitlines():
            lines.append(str(line))
        return p.returncode == 0, lines

    def call(self, function: Callable, arguments: Optional[Tuple] = None) -> Any:
        raise NotImplementedError("Calling python functions in a docker container is not possible")

    def from_pid(self, pid: Optional[int] = None, clean: bool = False) -> bool:
        return False

    def process_attach(self, pid: Optional[int] = None):
        raise NotImplementedError("Process attachment not possible for DockerNamespace")
