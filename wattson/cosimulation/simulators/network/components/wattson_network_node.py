import copy
import dataclasses
import json
import shlex
import shutil
import subprocess
import sys
import typing
from pathlib import Path
from typing import List, TYPE_CHECKING, Optional, Dict, Union, Callable, Tuple

from wattson.cosimulation.exceptions import ServiceNotFoundException
from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity
from wattson.cosimulation.simulators.network.constants import DEFAULT_SERVICE_PRIORITY
from wattson.services.configuration import ServiceConfiguration
from wattson.services.wattson_pcap_service import WattsonPcapService
from wattson.services.wattson_python_service import WattsonPythonService
from wattson.services.wattson_service import WattsonService
from wattson.util.misc import dynamic_load_class

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkNode(WattsonNetworkEntity, NetworkNode):
    """
    A WattsonNetworkNode is a WattsonNetworkEntity that serves as node. A node can have interfaces and run services.
    Switches, Routers and ordinary hosts are NetworkNodes.

    """
    interfaces: List['WattsonNetworkInterface'] = dataclasses.field(default_factory=lambda: [])
    config: dict = dataclasses.field(default_factory=lambda: {})
    priority: float = DEFAULT_SERVICE_PRIORITY

    _services: Dict[int, WattsonService] = dataclasses.field(default_factory=lambda: {})
    _processes: List[subprocess.Popen] = dataclasses.field(default_factory=list)
    _min_interface_id: int = 0

    _is_outside_namespace: bool = False

    class_id: typing.ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()
        self.load_services()

    @property
    def os(self) -> str:
        return "linux"

    def start(self):
        super().start()
        for key, value in self.config.get("sysctl", {}).items():
            self.set_sysctl(key, value)
        
    def stop(self):
        self.shutdown_processes()
        super().stop()

    def add_dns_entry(self, entry):
        success = self.mkdir(Path(f"/etc/netns/w_n{self.id}"))
        code = self.exec(["touch", f"/etc/netns/w_n{self.id}/resolv.conf"])
        code = self.exec(f"echo {entry} >> /etc/netns/w_n{self.id}/resolv.conf", shell=True)

    def is_outside_namespace(self):
        return self._is_outside_namespace

    def set_sysctl(self, key, value) -> bool:
        code = self.exec(["sysctl", "-w", f"{key}={value}"])
        return code == 0

    def get_sysctl(self, key) -> typing.Any:
        code, out = self.exec(["sysctl", f"{key}"])
        if code != 0:
            return None
        for line in out:
            if line.startswith(key):
                return line.split(" = ", 1)[1].strip()
        return None

    def get_prefix(self) -> str:
        return "n"

    def add_interface(self, interface: 'WattsonNetworkInterface'):
        interface.node = self
        if interface not in self.get_interfaces():
            self.interfaces.append(interface)

    def remove_interface(self, interface: 'WattsonNetworkInterface'):
        if interface in self.interfaces:
            self.interfaces.remove(interface)

    def on_interface_start(self, interface: 'WattsonNetworkInterface'):
        pass

    def get_free_interface_name(self, prefix="eth"):
        i = self._min_interface_id
        used_names = [interface.system_id for interface in self.interfaces]
        while f"{prefix}{i}" in used_names:
            i += 1
        self._min_interface_id = i + 1
        return f"{prefix}{i}"

    def get_interfaces(self) -> List['WattsonNetworkInterface']:
        return self.interfaces.copy()

    def get_ip_addresses(self) -> List[str]:
        return super().get_ip_addresses()

    def add_service(self, service: WattsonService):
        service.network_node = self
        self._services[service.id] = service

    def has_services(self) -> bool:
        return len(self._services) > 0

    def load_services(self):
        """
        Loads and instantiates service instances from the (optional) 'services' list in the node's configuration.
        For each found service, a respective instance is created.
        :return:

        """
        services = self.config.get("services", [])
        for service_config in services:
            service_configuration = ServiceConfiguration()
            # TODO: Better loading with sanitizing?
            for key, value in service_config.get("config", {}).items():
                service_configuration[key] = copy.deepcopy(value)

            service = None
            if service_config["service-type"] == "python":
                service_module = service_config["module"]
                service_class_name = service_config["class"]
                service_class = dynamic_load_class(service_module + "." + service_class_name)
                service = WattsonPythonService(service_class=service_class,
                                               service_configuration=service_configuration,
                                               network_node=self)
            elif service_config["service-type"] == "custom":
                service_module = service_config["module"]
                service_class_name = service_config["class"]
                service_class = dynamic_load_class(service_module + "." + service_class_name)
                if not issubclass(service_class, WattsonService):
                    raise ServiceNotFoundException(f"{service_class.__name__} is no WattsonService")
                service = service_class(service_configuration=service_configuration, network_node=self)

            if service is None:
                raise ServiceNotFoundException(f"Cannot create service of type {service_config['service-type']}")
            if "name" in service_config:
                service.name = service_config["name"]
            self._services[service.id] = service

    def stop_service(self, service_id: int, wait_seconds: float = 5, auto_kill: bool = False,
                     async_callback: Optional[Callable[['WattsonService'], None]] = None):
        super().stop_service(service_id, wait_seconds, auto_kill, async_callback)

    def get_services(self) -> Dict[int, WattsonService]:
        return self._services.copy()

    def get_service_by_name(self, service_name: str) -> WattsonService:
        return typing.cast(WattsonService, super().get_service_by_name(service_name))

    def get_service(self, service_id: int) -> WattsonService:
        return typing.cast(WattsonService, super().get_service(service_id=service_id))

    def start_pcap(self, interface: Optional['WattsonNetworkInterface'] = None) -> List['WattsonService']:
        if interface is None:
            services = []
            for interface in self.get_interfaces():
                services.extend(self.start_pcap(interface))
            return services
        else:
            for service in self.get_services().values():
                # Check if associated PCAP service already exists
                if isinstance(service, WattsonPcapService):
                    if interface.entity_id == service.interface.entity_id:
                        self.logger.info(f"Using existing PCAP service: {service.id}")
                        service.start()
                        return [service]
            # No service for interface found: Start new service
            service = WattsonPcapService(interface=interface, service_configuration=ServiceConfiguration(), network_node=self)
            self.logger.info(f"Creating new PCAP service: {service.id}")
            self.add_service(service)
            service.start()
            return [service]

    def stop_pcap(self, interface: Optional['WattsonNetworkInterface'] = None):
        for service in self.get_services().values():
            if isinstance(service, WattsonPcapService):
                if interface is None or service.interface.entity_id == interface.entity_id:
                    service.stop(auto_kill=True)

    def manage_process(self, process: subprocess.Popen):
        self._processes.append(process)

    def shutdown_processes(self):
        for process in self._processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Process {process.pid} takes too long to terminate - killing it")
                    process.kill()

    @property
    def entity_id(self) -> str:
        return self.node_id

    def get_host_root(self, ensure_exists: bool = True) -> Path:
        """
        Returns the root folder of this network node.

        Args:
            ensure_exists (bool, optional):
                
                (Default value = True)
        """
        path = self.network_emulator.get_working_directory().joinpath(self.get_hostname())
        if ensure_exists:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def get_host_folder(self, ensure_exists: bool = True) -> Path:
        """
        Return the working directory of this node relative to the host machine running Wattson.

        Args:
            ensure_exists (bool, optional):
                
                (Default value = True)

        Returns:
            Path: The working directory of this node relative to the host machine
        """
        path = self.get_host_root(ensure_exists=ensure_exists).joinpath("working_directory")
        if ensure_exists:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def get_guest_folder(self) -> Path:
        """
        Return the working directory of this node relative to this node.
        This is just the host folder / artifact folder, except for nodes that use a different file system (e.g., Docker or VMs)


        Returns:
            Path: The working directory of this node relative to the node / guest.
        """
        return self.get_host_folder()

    def get_python_executable(self) -> str:
        return sys.executable

    def exec(self, cmd: Union[List[str], str], **kwargs) -> typing.Tuple[int, List[str]]:
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        # namespace = self.network_emulator.get_namespace(self)
        # cmd = shlex.split(" ".join(cmd))
        stderr = kwargs.pop("stderr", subprocess.STDOUT)
        proc = self.popen(cmd, stdout=subprocess.PIPE, stderr=stderr, universal_newlines=True, **kwargs)
        res_lines = []
        output, error = proc.communicate()
        for line in output.splitlines():
            res_lines.append(str(line).strip())
        code = proc.returncode
        return code, res_lines

    def exec_fs_cmd(self, cmd: List[str], **kwargs) -> int:
        """
        Executes a file-system related command (e.g., mounting).
        This does not necessarily belong to the network namespace in mixed-namespace environments

        Args:
            cmd (List[str]):
                The command to execute
            **kwargs:
                

        Returns:
            int: The command's return code
        """
        p = subprocess.run(cmd, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, **kwargs)
        return p.returncode

    def popen(self, cmd: Union[List[str], str], **kwargs) -> subprocess.Popen:
        return self.get_namespace().popen(cmd, **kwargs)

    def get_config(self) -> dict:
        return self.config

    def set_config(self, key, value):
        self.config[key] = value

    def update_config(self, config) -> bool:
        self.config.update(config)
        return True

    def get_roles(self) -> List[str]:
        return self.config.get("roles", [])

    def delete_role(self, role: str):
        if self.has_role(role):
            self.config["roles"].remove(role)

    def add_role(self, role: str):
        if self.has_role(role):
            return
        self.config.setdefault("roles", []).append(role)

    def get_working_directory(self) -> Path:
        directory = self.config.get("working_directory", ".")
        if directory.startswith("/"):
            return Path(directory).absolute()
        return self.network_emulator.get_working_directory().joinpath(directory).absolute()

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__,
            "hostname": self.get_hostname(),
            "roles": self.get_roles(),
            "config": self.get_config(),
            "interfaces": [interface.to_remote_representation() for interface in self.interfaces],
            "services": {service_id: service.to_remote_representation() for service_id, service in self._services.items()}
        })
        return d

    def open_terminal(self) -> bool:
        """
        Attempts to open a terminal / konsole for the network node.

        """
        return False

    """
    FILE SYSTEM OPERATIONS
    """
    def mount(self, mount_point: Path, target: Path, remove_file: bool = False, remove_folder: bool = False, bind: bool = False) -> bool:
        """
        Mounts the target folder at the given mount point.

        Args:
            mount_point (Path):
                The mount point
            target (Path):
                The target folder
            remove_file (bool, optional):
                Whether to remove the mount point if it is a file
                (Default value = False)
            remove_folder (bool, optional):
                Whether to remove the mount point if it is a folder
                (Default value = False)
            bind (bool, optional):
                Whether to mount via --bind (if applicable)
                (Default value = False)

        Returns:
            bool: Whether the mounting was successful
        """
        if remove_file and self.file_exists(file=mount_point):
            self.remove_file(mount_point)
        if remove_folder and self.folder_exists(folder=mount_point):
            self.unmount(mount_point=mount_point)
            self.remove_folder(folder=mount_point, with_contents=True)
        self.mkdir(mount_point, parents=True)
        cmd = ["mount", str(target.absolute()), str(mount_point.absolute())]
        if bind:
            cmd.insert(1, "--bind")
        return self.exec_fs_cmd(cmd) == 0

    def unmount(self, mount_point: Path, lazy: bool = False, force: bool = False) -> bool:
        """
        Unmount the given mount point

        Args:
            mount_point (Path):
                The mount point
            lazy (bool, optional):
                Use lazy unmounting
                (Default value = False)
            force (bool, optional):
                force unmounting
                (Default value = False)

        Returns:
            bool: Whether the unmounting was successful
        """
        cmd = ["umount", str(mount_point.absolute())]
        if lazy:
            cmd.insert(1, "-l")
        if force:
            cmd.insert(1, "-f")
        return self.exec_fs_cmd(cmd) == 0

    def mkdir(self, folder: Path, mode: int = 0o777, parents: bool = False, exists_ok: bool = False) -> bool:
        if self.file_exists(folder):
            return False
        if not exists_ok and not parents:
            if self.folder_exists(folder):
                return False
        mode = str(oct(mode)).replace("o", "")
        cmd = ["mkdir", "-m", mode, str(folder.absolute())]
        if parents:
            cmd.insert(1, "-p")
        return self.exec_fs_cmd(cmd) == 0

    def remove_file(self, file: Path, missing_ok: bool = False) -> bool:
        if self.folder_exists(folder=file):
            return False
        if not self.file_exists(file):
            return missing_ok
        return self.exec_fs_cmd(["rm", str(file.absolute())]) == 0

    def remove_folder(self, folder: Path, missing_ok: bool = False, with_contents: bool = False):
        if self.file_exists(folder):
            return False
        if not self.folder_exists(folder):
            return missing_ok

        if with_contents:
            cmd = ["rm", "-r", str(folder.absolute())]
        else:
            cmd = ["rmdir", str(folder.absolute())]
        return self.exec_fs_cmd(cmd) == 0

    def file_exists(self, file: Path) -> bool:
        """
        Checks whether the given file exists on this node

        Args:
            file (Path):
                The file path to check for

        Returns:
            bool: Whether the file exists (and is actually a file)
        """
        return self.exec_fs_cmd(["test", "-f", str(file.absolute())]) == 0

    def socket_exists(self, socket: Path) -> bool:
        return self.exec_fs_cmd(["test", "-S", str(socket.absolute())]) == 0

    def folder_exists(self, folder: Path) -> bool:
        """
        Checks whether the given folder exists on this node

        Args:
            folder (Path):
                The folder path to check for

        Returns:
            bool: Whether the folder exists (and is actually a folder)
        """
        return self.exec_fs_cmd(["test", "-d", str(folder.absolute())]) == 0

    def transform_path(self, path: Path) -> Path:
        """
        Transforms the given path to a node-specific path, i.e., relative paths are transformed to the in-node file system w.r.t. the working
        directory.

        Args:
            path (Path):
                The path to transform

        Returns:
            Path: The transformed path
        """
        if path.is_absolute():
            return path
        return self.get_guest_folder().joinpath(path)

    def file_put_contents(self, path: Path, contents: str) -> Tuple[bool, Optional[Path]]:
        transformed_path = self.transform_path(path)
        return self.get_namespace().file_put_contents(transformed_path, contents), transformed_path

    def file_get_contents(self, path: Path) -> Optional[str]:
        return self.get_namespace().file_get_contents(path)

    """
    Interface control
    """
    def interface_set_mac(self, interface: 'WattsonNetworkInterface') -> bool:
        return False

    def interface_up(self, interface: 'WattsonNetworkInterface') -> bool:
        code, lines = self.exec(["ip", "link", "set", "dev", interface.get_system_name(), "up"])
        if not code == 0:
            self.logger.error(f"Could not set interface {interface.get_system_name()} up")
            self.logger.debug(lines)
        return code == 0

    def interface_down(self, interface: 'WattsonNetworkInterface') -> bool:
        code, lines = self.exec(["ip", "link", "set", "dev", interface.get_system_name(), "down"])
        if not code == 0:
            self.logger.error(f"Could not set interface {interface.get_system_name()} down")
            self.logger.debug(lines)
        return code == 0

    def interface_flush_ip(self, interface: 'WattsonNetworkInterface') -> bool:
        code, lines = self.exec(["ip", "addr", "flush", "dev", interface.get_system_name()])
        if not code == 0:
            self.logger.error(f"Could not flush IP of {interface.get_system_name()}")
            self.logger.debug(lines)
        return code == 0

    def interface_set_ip(self, interface: 'WattsonNetworkInterface') -> bool:
        if interface.ip_address_string is None:
            return True
        code, lines = self.exec(["ip", "addr", "add", interface.ip_address_string, "dev", interface.get_system_name()])
        if not code == 0:
            self.logger.error(f"Could not set IP of {interface.get_system_name()} to {interface.ip_address_string}")
            self.logger.debug(lines)
        return code == 0

    def interface_rename(self, old_name: str, new_name: str) -> bool:
        """
        Renames a physical interface on the node

        Args:
            old_name (str):
                The original interface name
            new_name (str):
                The new interface name

        Returns:
            bool: Whether the action was successful
        """
        code, lines = self.exec(["ip", "link", "set", "dev", old_name, "name", new_name])
        if not code == 0:
            self.logger.error(f"Could not rename interface {old_name} to {new_name}")
            self.logger.error(lines)
            return False
        return True

    def interfaces_list_existing(self, try_num: int = 0, max_tries: int = 10, retry_error_message: str = "") -> List[Dict]:
        if try_num >= max_tries:
            self.logger.error(f"Could not load interface information - maximum number of tries exceeded ({retry_error_message})")
            return []

        code, lines = self.exec(["ip", "--json", "a"], stderr=subprocess.PIPE)
        if code != 0:
            self.logger.error(f"Could not load interface information: {code} \n {'\n'.join(lines)}")
            return []

        # Check for interrupted dump
        if "Dump was interrupted and may be inconsistent" in "\n".join(lines):
            return self.interfaces_list_existing(try_num=try_num + 1, retry_error_message="Inconsistent dump")

        try:
            data = json.loads("\n".join(lines))
        except json.JSONDecodeError:
            self.logger.error(f"Could not load interface information ({self.entity_id} // {self.display_name})")
            self.logger.error("\n".join(lines))
            return []
        # Sanitize - follow QEMU Agent format
        interfaces = []
        for interface_data in data:
            ip_addresses = []
            for address_info in interface_data.get("addr_info", []):
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
            interface = {
                "name": interface_data["ifname"],
                "ip-addresses": ip_addresses,
                "statistics": {},
                "hardware-address": interface_data.get("address")
            }
            interfaces.append(interface)
        return interfaces
