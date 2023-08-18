import dataclasses
import os
import shutil
import subprocess
from typing import ClassVar, Optional, TYPE_CHECKING

from wattson.cosimulation.simulators.network.components.interface.network_host import NetworkHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.util.terminal import get_console_and_shell

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkHost(WattsonNetworkNode, NetworkHost):
    class_id: ClassVar[int] = 0

    def get_prefix(self):
        return "h"

    def start(self):
        super().start()
        self.loopback_up()

    def on_interface_start(self, interface: 'WattsonNetworkInterface'):
        pass

    def update_default_route(self) -> bool:
        routers = self.network_emulator.find_routers(self)
        local_interface = None
        router_interface = None

        if len(routers) == 0:
            # No router found - just use first interface
            for interface in self.get_interfaces():
                if interface.is_management:
                    continue
                local_interface = interface
                break
        else:
            # Use Interface leading to first Router
            router, subnet = routers[0]
            # Find local interface matching the router subnet
            for interface in self.get_interfaces():
                if interface.has_ip() and interface.get_ip_address() in subnet:
                    local_interface = interface
                    break
            # Find router interface matching the subnet
            for interface in router.get_interfaces():
                if interface.has_ip() and interface.get_ip_address() in subnet:
                    router_interface = interface
                    break
        if local_interface is None:
            self.logger.error(f"Cannot add default route for {self.entity_id} as no interface is present")
            return False
        if router_interface is None:
            # Set just via interface
            self.exec("ip route delete default")
            return self.exec(f"ip route add default dev {local_interface.interface_name}")[0] == 0
        else:
            # Set with gateway IP
            self.exec("ip route delete default")
            return self.exec(f"ip route add default dev {local_interface.interface_name} via {router_interface.ip_address_short_string}")[0] == 0

    def generate_display_name(self) -> str:
        # TODO: Allow to register names for certain roles
        if self.has_role("rtu"):
            return f"RTU {self.config.get('coa')}"
        if self.has_role("mtu"):
            return f"MTU {self.entity_id}"
        if self.has_role("vcc"):
            return f"Wattson VCC"
        if self.has_role("firewall"):
            return f"Firewall {self.entity_id}"
        return super().generate_display_name()

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "entity_id": self.entity_id,
            "class": self.__class__.__name__,
        })
        return d

    def open_terminal(self) -> bool:
        """
        Attempts to open a terminal / konsole for the network host.
        @return:
        """
        # Check if DISPLAY environment variable is available
        if "DISPLAY" not in os.environ:
            self.logger.error("No DISPLAY available")
            return False
        # Check if Terminal and Shell are available
        terminal, shell = get_console_and_shell()
        if shutil.which(terminal) is None or shutil.which(shell) is None:
            self.logger.error(f"Terminal {terminal} or shell {shell} not available")
            return False
        # Open terminal
        cwd = str(self.get_working_directory())
        cwd = str(self.get_artifact_folder().absolute())
        divider = "-e"
        use_shell = False
        pre_cmd = ""
        if "gnome-terminal" in terminal:
            divider = "--"
            dbus = shutil.which("dbus-launch")
            pre_cmd = f"{dbus} "
            use_shell = True

        cmd = f"{pre_cmd}{terminal} {divider} {shell}"
        p = self.popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, shell=use_shell, stderr=subprocess.DEVNULL)
        self.manage_process(p)
        return p.poll() is None

    def loopback_up(self) -> bool:
        return self.get_namespace().loopback_up()
