import json
import subprocess
import time
import typing
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING
from xml.etree import ElementTree

from wattson.cosimulation.simulators.network.components.interface.network_entity import NetworkEntity
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_virtual_machine_host import WattsonNetworkVirtualMachineHost
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace
from wattson.networking.namespaces.virtual_machine_namespace import VirtualMachineNamespace

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator


class VirtualMachineWrapper(NodeWrapper):
    def __init__(self, entity: NetworkEntity, emulator: 'WattsonNetworkEmulator', _type: str = "qemu"):
        super().__init__(entity, emulator)
        if _type not in ["qemu"]:
            raise ValueError(f"Supported virtualization types: qemu\nNot: {_type}")
        self.type = _type
        self.conn = None
        self.domain = None
        self.img_file_path = Path(self.virtual_machine.config.get("vm_path", "tmp")).joinpath(f"{self.name}.img")
        self.config_file_path = self.virtual_machine.config["vm_config_file"]
        self.disk_file_path = self.virtual_machine.config["vm_disk_file"]
        self.do_clone = self.virtual_machine.config.get("vm_clone", True)

        self._virtual_machine_namespace = VirtualMachineNamespace(f"w_{self.entity.entity_id}", domain=self.name)
        # self._linux_namespace = Namespace(f"w_{self.entity.entity_id}")
        self._main_namespace = self.emulator.get_main_namespace()

        # TODO: check if qcow image needs to be resized before creating the vm
        # qemu-img convert -f qcow2 -O qcow2 -o preallocation=off self.qcow_file newdisk.qcow2

    @property
    def name(self) -> str:
        return self.virtual_machine.config["vm_name"]

    @property
    def virtual_machine(self) -> WattsonNetworkVirtualMachineHost:
        return typing.cast(WattsonNetworkVirtualMachineHost, self.entity)

    def get_namespace(self) -> VirtualMachineNamespace:
        return self._virtual_machine_namespace

    def get_additional_namespace(self) -> Namespace:
        return self._main_namespace

    def is_virtual_machine_running(self) -> bool:
        if self.type == "qemu":
            return self.domain.state() == [1, 1]  # running
        return False

    def create(self) -> bool:        
        import libvirt
        #if self.get_additional_namespace().exists():
        #    self.logger.error("Namespace already exists")
        #    return False
        #self.get_additional_namespace().create()

        if self.type == "qemu":
            # Clone a base VM?
            if self.do_clone:
                # Copy and adapt config
                tree = ElementTree.parse(self.config_file_path)
                root = tree.getroot()
                for node in root.findall("devices/disk"):
                    if node.attrib.get("device") == "disk":
                        node.find("source").attrib["file"] = str(self.disk_file_path)
                tree.write(self.config_file_path)

                # Clone
                success, lines = self._main_namespace.exec(
                    [
                        "virt-clone",
                        "--original-xml", self.config_file_path,
                        "--name", self.name,
                        "-f", self.img_file_path
                    ]
                )
                if not success:
                    self.logger.error(f"Could not start VM {self.name}")
                    self.logger.error(f"{success}: {lines}")
                    return False
            self.conn = libvirt.open('qemu:///system')
            self.domain = self.conn.lookupByName(self.name)
            self.add_shared_folder()
            self.domain.create()
            self.logger.info(f"Waiting for {self.name} to boot")
            self._wait_for_boot()
            self.logger.info(f'Started VM "{self.name}".')
            self._mount_shared_folder()
            if self.get_namespace().get_os() == "linux":
                self.logger.info(f'Stopping NetworkManager')
                self.get_namespace().exec("systemctl stop NetworkManager.service")
            self.logger.info(f"Clearing routes")
            self.virtual_machine.clear_routes()
            return True
        return False

    def _wait_for_boot(self, timeout: typing.Optional[float] = 30, poll_interval: float = 1) -> bool:
        return self.get_namespace().wait_until_available(timeout=timeout, poll_interval=poll_interval)

    def _mount_shared_folder(self) -> bool:
        if "shared_folder" in self.virtual_machine.config:
            target_dir = self.virtual_machine.config["shared_folder"]["target_folder"]
            success, lines = self._virtual_machine_namespace.exec(["mkdir", "-p", f"/mnt/{target_dir}"])
            success2, _ = self._virtual_machine_namespace.exec(["/usr/bin/mount", "-v", "-t", "virtiofs", target_dir, f"/mnt/{target_dir}"])
            return success and success2
        return True

    def add_shared_folder(self) -> bool:
        if self.type == "qemu":
            if not self.virtual_machine.config.get("shared_folder", {}):
                self.logger.debug("No shared folder specified.")
                return False
            self.logger.info("Mounting shared folder")
            host_folder = self.virtual_machine.config["shared_folder"]["host_folder"]
            target_dir = self.virtual_machine.config["shared_folder"]["target_folder"]
            self._main_namespace.exec(["virt-xml", self.name, "--edit", "--memorybacking", "access.mode=shared"])
            success, _ = self._main_namespace.exec(
                [
                    "virt-xml", self.name,
                    "--add-device", "--filesystem",
                    f"driver.type=virtiofs,source.dir={host_folder},target.dir={target_dir}"]
            )
            return success

    def clean(self) -> bool:
        # self.get_additional_namespace().clean()
        if self.type == "qemu":
            self.get_namespace().shutdown()
            if self.do_clone:
                success, _ = self._main_namespace.exec(["virsh", "undefine", self.name])
                p = Path(self.img_file_path)
                p.unlink()
                return success
            return True
        return False

    def add_interface(self, interface: WattsonNetworkInterface) -> bool:
        self.logger.debug(f"Adding interface {interface.interface_name} ({interface.entity_id}) to {self.name} ({self.virtual_machine.entity_id})")

        original_mac = interface.get_mac_address()
        interfaces = interface.node.interfaces_list_existing()
        existing_interface_names = [vm_interface["name"] for vm_interface in interfaces]

        target_mac = None
        if original_mac is not None:
            # Adjust MAC address
            target_mac = original_mac
            # __:__:__:__:vw:xy -> __:__:__:__:Fw:xy
            parts = target_mac.split(":")
            parts[4] = f"F{parts[4][1]}"
            target_mac = ":".join(parts)

        # Add interface to VM from existing veth interface
        attach_interface_command = [
            "virsh", "attach-interface",
            "--type", "direct",
            "--source", interface.interface_name,
            "--model", "virtio"
        ]
        if target_mac is not None:
            attach_interface_command.extend([
                "--mac", target_mac
            ])
        attach_interface_command.append(self.name)

        success, lines = self._main_namespace.exec(attach_interface_command)
        if not success:
            self.logger.error(f"Could not attach interface {interface.interface_name} to node")
            self.logger.error(repr(lines))
            return False

        # Todo: Is there a better way?
        time.sleep(1)

        if not self._wait_for_boot():
            self.logger.error(f"Lost connection to VM waiting for interface...")

        new_interfaces = interface.node.interfaces_list_existing()
        if len(new_interfaces) == len(existing_interface_names):
            # Potentially, the interface has been created in an earlier run of the VM and is disabled.
            # Try to enable it and receive a new list
            self.virtual_machine.interface_up(interface)
            new_interfaces = interface.node.interfaces_list_existing()

        if len(new_interfaces) != len(existing_interface_names) + 1:
            self.logger.error("Could not find newly created interface")
            self.logger.debug(f"Previous known: {repr(existing_interface_names)}")
            self.logger.debug(f"Now known: {repr(i['name'] for i in new_interfaces)}")
            return False
        new_interface = None
        for vm_interface in new_interfaces:
            if vm_interface["name"] not in existing_interface_names:
                new_interface = vm_interface
                break
        if new_interface is None:
            self.logger.error("Could not extract newly created interface")

        new_interface_name = new_interface["name"]
        interface.config["virtual_machine_mac"] = new_interface["hardware-address"]
        interface.config["virtual_machine_original_name"] = new_interface_name
        self.logger.debug(f"Created interface {new_interface_name} with MAC {new_interface['hardware-address']}")
        if new_interface_name != interface.interface_name:
            # Only try to rename the interface if the name is different
            if not interface.node.interface_rename(new_interface_name, interface.interface_name):
                return False
            self.logger.debug(f"Interface {new_interface_name} renamed to {interface.interface_name}")
        return True

    def remove_interface(self, interface: WattsonNetworkInterface) -> bool:
        mac = interface.config.get("virtual_machine_mac")
        if mac:
            success1, _ = self._main_namespace.exec(["virsh", "detach-interface", "--mac", mac, self.name])
        else:
            self.logger.warning(f"No MAC found for interface to be detached: {interface.entity_id} ({interface.interface_name})")
            success1 = True
        success2, _ = self.get_additional_namespace().exec(["ip", "link", "delete", interface.interface_name])
        return success1 and success2
