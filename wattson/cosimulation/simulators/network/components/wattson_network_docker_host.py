import dataclasses
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Dict, List, Union

import docker
import docker.errors
from docker.models.containers import Container

from wattson.cosimulation.exceptions import NetworkException
from wattson.cosimulation.simulators.network.components.interface.network_docker_host import NetworkDockerHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.networking.namespaces.docker_namespace import DockerNamespace
from wattson.util.terminal import get_console_and_shell


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkDockerHost(WattsonNetworkHost, NetworkDockerHost):
    def __post_init__(self):
        super().__post_init__()
        self._docker_client = docker.from_env()
        self._namespace: Optional[DockerNamespace] = None
        self._container_name = f"mn.{self.system_id}"
        self._python_executable = None
        self.config.setdefault("capabilities", []).extend(["NET_BIND_SERVICE", "NET_RAW", "SYS_ADMIN"])

    @classmethod
    def get_class_id(cls):
        # Share value with default hosts
        return WattsonNetworkHost.get_class_id()

    @property
    def container_name(self) -> str:
        return self._container_name

    @container_name.setter
    def container_name(self, name):
        self._container_name = name

    def get_container(self) -> Optional[Container]:
        try:
            container = self._docker_client.containers.get(self.container_name)
            return container
        except docker.errors.NotFound:
            return None

    def container_exists(self) -> bool:
        return self.get_container() is not None

    def is_container_running(self) -> bool:
        container = self.get_container()
        if container is not None:
            return container.attrs["State"]["Running"]
        return False

    def get_container_pid(self) -> int:
        if not self.is_container_running():
            return -1
        container = self.get_container()
        return container.attrs["State"]["Pid"]

    @classmethod
    def set_class_id(cls, class_id: int):
        # Share value with default hosts
        WattsonNetworkHost.set_class_id(class_id)

    def get_prefix(self):
        return "h"

    def start(self):
        self.start_container()
        super().start()
        # Start rsyslog
        if self.config.get("start_syslog", True):
            self.exec("chmod 0777 /var/log")
            self.exec("rsyslogd")
            
        # Remove unwanted interfaces added by docker
        _, data = self.exec("ip --json a")
        host_interfaces = json.loads("".join(data))
        host_interface_names = [interface["ifname"] for interface in host_interfaces]
        # All registered interfaces along with loopback (lo)
        allowed_interface_names = [interface.interface_name for interface in self.get_interfaces()] + ["lo"]
        for interface in host_interface_names:
            if interface not in allowed_interface_names:
                self.logger.debug(f"Deleting interface {interface}")
                code, lines = self.exec(f"ip link delete {interface}")
                if code != 0:
                    self.logger.error(f"Could not delete {interface}")
                    self.logger.error("\n".join(lines))

    def stop(self):
        super().stop()
        self.stop_container()

    def add_volume(self, name: str, host_path: str, docker_path: str, permission: str = "rw"):
        """
        Add a volume for mounting to the container (before it is started).

        Args:
            name (str):
                The name for the volume mount (i.e., an ID)
            host_path (str):
                The path on the host system
            docker_path (str):
                The mount path within the docker container
            permission (str, optional):
                The permissions for this folder (r, w, rw)
                (Default value = "rw")
        """
        self.config.setdefault("volumes", []).append(
            {
                "name": name,
                "host_path": host_path,
                "docker_path": docker_path,
                "permission": permission
            }
        )

    def remove_volume(self, name: str):
        for i, volume in self.get_volumes(with_default_mount=False):
            if volume.get("name") == name:
                self.config.get("volumes", []).pop(i)
                break

    def get_volume(self, name: str) -> Optional[Dict]:
        for volume in self.get_volumes():
            if volume.get("name") == name:
                return volume
        return None

    def get_volumes(self, with_default_mount: bool = True) -> List[Dict[str, str]]:
        """
        Returns a list of volumes to mount, represented by a dict with "host_path" and "docker_path" keys

        Args:
            with_default_mount (bool, optional):
                
                (Default value = True)
        """
        volumes = self.config.get("volumes", [])
        # Sanity check volumes
        if not isinstance(volumes, list):
            self.logger.error("Invalid volume configuration detected (volumes is not a list)")
            volumes = []
        for volume in volumes:
            if not isinstance(volume, dict):
                self.logger.error("Invalid volume configuration detected (volume is not a dict)")
                volumes = []
                break
            if "host_path" not in volume or "docker_path" not in volume:
                self.logger.error("Invalid volume paths")
                volumes = []
                break

        if with_default_mount:
            volumes.append(
                {
                    "name": "default_mount",
                    "host_path": str(self.get_host_folder().resolve()),
                    "docker_path": "/wattson/mnt",
                    "permission": "rw"
                }
            )
        return volumes

    def get_boot_command(self) -> str:
        """
        


        Returns:
            str: The boot command to start the container with.
        """
        return self.config.get("command", "/bin/bash")

    def get_host_folder(self) -> Path:
        return super().get_host_folder()

    def get_guest_folder(self) -> Path:
        # Attempt to find the mount point with a matching host_path.
        # Then, the respective docker_path is the guest folder
        self.logger.debug(f"Searching for {str(self.get_host_folder().resolve())}")
        for volume in self.get_volumes():
            self.logger.debug(f"  Checking {str(Path(volume.get('host_path')))}")
            if Path(volume.get("host_path")).resolve() == self.get_host_folder().resolve():
                self.logger.debug(f"  Using {str(Path(volume.get('host_path')))}")
                return Path(volume.get("docker_path"))

        self.logger.error("Could not find a mount of the host folder within the container")
        return Path("/wattson")

    def get_python_executable(self) -> str:
        if self._python_executable is None:
            if not self.is_started:
                self.logger.warning("Could not determine python3 executable since node is not yet started - attempting fallback")
                return "python3"

            _, lines = self.exec("which python3")
            if len(lines) != 1:
                self.logger.warning("Could not identify a python3 executable - attempting to use fallback")
                self._python_executable = "python3"
            self._python_executable = lines[0]
        return self._python_executable

    def get_image_name(self) -> Optional[str]:
        return self.config.get("image")

    def get_image_tag(self) -> Optional[str]:
        return self.config.get("tag", "latest")

    def get_full_image(self) -> Optional[str]:
        return f"{self.get_image_name()}:{self.get_image_tag()}"

    def is_valid_image(self) -> str:
        return self.get_image_name() is not None and self.get_image_tag() is not None

    def is_image_installed(self) -> bool:
        try:
            self._docker_client.images.get(self.get_image_name())
            return True
        except docker.errors.ImageNotFound:
            return False
        except docker.errors.APIError:
            self.logger.error("Docker API unavailable")
            return False

    def get_image(self):
        if self.is_image_installed():
            return self._docker_client.images.get(self.get_image_name())
        return None

    def pull_image(self) -> bool:
        try:
            self._docker_client.images.pull(self.get_image_name(), tag=self.get_image_tag())
        except docker.errors.APIError:
            return False
        return True

    def create_container(self) -> bool:
        volumes = self.get_volumes()
        volume_bind = {}
        for volume in volumes:
            volume_bind[volume["host_path"]] = {
                "bind": volume["docker_path"],
                "mode": volume["permission"]
            }

        host_config = {}
        if self.config.get("memory_limit") is not None:
            host_config["mem_limit"] = self.config.get("memory_limit")
        if self.config.get("cpu_core_limit") is not None:
            host_config["cpus"] = self.config.get("cpu_core_limit")

        if self._docker_client.containers.create(
            image=self.get_full_image(),
            name=self.container_name,
            privileged=self.config.get("privileged", False),
            hostname=self.system_id,
            volumes=volume_bind,
            stdin_open=True,
            tty=True,
            detach=True,
            cap_add=["NET_ADMIN"] + self.config.get("capabilities", []),
            **host_config
        ):
            if self.config.get("privileged", False):
                self.logger.warning(f"Running in privileged mode - use with caution")
            return True
        return False

    def start_container(self):
        if self.is_container_running():
            return True
        try:
            self._docker_client.api.start(self.container_name)
            return True
        except Exception as e:
            self.logger.error(f"Could not start container: {e=}")
            return False

    def stop_container(self):
        if not self.is_container_running():
            return True
        try:
            self._docker_client.api.stop(self.container_name)
            return True
        except Exception as e:
            self.logger.error(f"Could not stop container: {e=}")
            return False

    def remove_container(self, force: bool = False):
        if not self.container_exists():
            return True
        try:
            self._docker_client.api.remove_container(resource_id=self.container_name, force=force)
            return True
        except Exception as e:
            self.logger.error(f"Could not remove container: {e=}")
            return False

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update(
            {
                "entity_id": self.entity_id,
                "class": self.__class__.__name__,
                "image_name": self.get_image_name(),
                "image_tag": self.get_image_tag(),
                "boot_command": self.get_boot_command()
            }
        )
        return d

    def get_namespace(self) -> Optional[DockerNamespace]:
        if self._namespace is None:
            self._namespace = DockerNamespace(self.container_name)
        return self._namespace

    def popen(self, cmd: Union[List[str], str], **kwargs) -> subprocess.Popen:
        if not self.is_container_running():
            raise NetworkException("No container running to open a process in")
        return self.get_namespace().popen(cmd, **kwargs)

    def open_terminal(self) -> bool:
        # Check if DISPLAY environment variable is available
        if "DISPLAY" not in os.environ:
            self.logger.error("No DISPLAY available")
            return False
        # Check if Terminal and Shell are available
        terminal, shell = get_console_and_shell()
        if shutil.which(terminal) is None:
            self.logger.error(f"Terminal {terminal} not available")
            return False
        # Open terminal
        docker_shell = "/bin/bash"
        container = self.container_name
        command = f"docker exec -it {container} {docker_shell}"
        divider = "-e"
        use_shell = False
        pre_cmd = ""
        if "gnome-terminal" in terminal:
            divider = "--"
            dbus = shutil.which("dbus-launch")
            pre_cmd = f"{dbus} "
            use_shell = False

        cmd = f"{pre_cmd}{terminal} {divider} {command}"
        cmd = shlex.split(cmd)

        def pre_exec_function():
            # Detach from process group to ignore signals sent to main process
            os.setpgrp()

        self.logger.info(" ".join(cmd))

        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, shell=use_shell, stderr=subprocess.DEVNULL, preexec_fn=pre_exec_function)
        self.manage_process(p)
        return p.poll() is None

    """
    FILE AND MOUNTING METHODS
    """
    def exec_fs_cmd(self, cmd: List[str], **kwargs) -> int:
        code, _ = self.exec(cmd, **kwargs)
        return code
