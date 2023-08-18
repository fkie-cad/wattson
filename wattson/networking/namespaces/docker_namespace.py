import logging
import shlex
import subprocess
import traceback
from typing import Optional, List, Tuple, Any, Callable, Union

import docker
from docker.models.containers import Container, ContainerCollection

from wattson.networking.namespaces.namespace import Namespace


class DockerNamespace(Namespace):
    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        super().__init__(name, logger)
        self._docker_client = docker.DockerClient()

    def get_container(self) -> Optional[Container]:
        for container in self._docker_client.containers.list(filters={"status": "running"}):
            if container.name == self.name:
                return container
        return None

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

    def exec(self, command) -> Tuple[bool, List[str]]:
        return self._exec(command)

    def popen(self, cmd: Union[str, List[str]], **kwargs) -> subprocess.Popen:
        if not self.exists():
            raise RuntimeError("No matching container found")
        custom_command = cmd
        docker_command = f"docker exec -t {self.name}"
        return super().popen(custom_command, wrap_command=docker_command, **kwargs)

    def _exec(self, cmd, stdout=None, stderr=None) -> Tuple[bool, List[str]]:
        if not self.exists():
            return False, []

        if stdout is None:
            stdout = subprocess.PIPE
        if stderr is None:
            stderr = subprocess.STDOUT
        p = self.popen(shlex.split(cmd), stdout=stdout, stderr=stderr, universal_newlines=True)
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


