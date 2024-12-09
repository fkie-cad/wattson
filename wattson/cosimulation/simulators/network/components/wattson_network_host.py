import dataclasses
import ipaddress
import os
import shutil
import subprocess
from typing import ClassVar, Optional, TYPE_CHECKING, Tuple, List, Union

from wattson.cosimulation.control.constants import SIM_CONTROL_ID
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
        if not self.is_outside_namespace():
            self._set_name_servers()

    def _set_name_servers(self):
        servers = self.config.get("name-servers", [])
        search_domain = self.config.get("search-domain")
        if len(servers) == 0:
            return
        self.set_name_servers(name_servers=servers, search_domain=search_domain)

    def on_interface_start(self, interface: 'WattsonNetworkInterface'):
        pass

    def get_interfaces_in_subnet(self, subnet: ipaddress.IPv4Network):
        interfaces = []
        for interface in self.interfaces:
            if interface.has_ip() and interface.get_subnet().subnet_of(subnet):
                interfaces.append(interface)
        return interfaces

    def get_first_router_interface(self) -> Tuple[Optional['WattsonNetworkInterface'], Optional['WattsonNetworkInterface']]:
        routers = self.network_emulator.find_routers(self)
        local_interface = None
        router_interface = None

        if len(routers) == 0:
            for interface in self.get_interfaces():
                if interface.is_management:
                    continue
                local_interface = interface
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

        return local_interface, router_interface

    def update_default_route(self) -> bool:
        local_interface, router_interface = self.get_first_router_interface()
        if self.entity_id == SIM_CONTROL_ID:
            return True
        custom_routes = self.config.get("custom-routes", [])
        for route in custom_routes:
            self.set_route(route.get("route"), route.get("interface"), route.get("gateway"))

        if not self.config.get("use-default-routes", True):
            return True
        if local_interface is None:
            self.logger.error(f"Cannot add default route for {self.entity_id} as no interface is present")
            return False
        if router_interface is None:
            # Set just via interface
            return self.set_route("default", local_interface)
        else:
            # Set with gateway IP
            return self.set_route("default", local_interface, router_interface.ip_address_short_string)

    def clear_routes(self) -> bool:
        if self.is_outside_namespace():
            self.logger.warning(f"Refusing to clear routes when not in namespace")
            return False

        code, lines = self.exec(["ip", "route", "flush", "table", "main"])
        if not code == 0:
            self.logger.error(f"Could not clear routes")
            return False
        return True

    def set_route(self, target: str, interface: Optional[Union['WattsonNetworkInterface', str]] = None, gateway_ip: Optional[str] = None) -> bool:
        if interface is None and gateway_ip is None:
            return False
        self.exec(f"ip route delete {target}")
        cmd = f"ip route add {target}"
        if interface is not None:
            if isinstance(interface, str):
                interface_name = interface
            else:
                interface_name = interface.interface_name
            cmd += f" dev {interface_name}"
        if gateway_ip is not None:
            cmd += f" via {gateway_ip}"
        return self.exec(cmd)[0] == 0

    def get_routes_list(self) -> list:
        code, lines = self.exec(f"ip --json route show")
        routes = []
        if code != 0:
            self.logger.error(f"Could not get routes for {self.entity_id}")
            return routes
        import json
        try:
            routes = json.loads("\n".join(lines))
            parsed_routes = []
            translation_dict = {
                "dst": "destination",
                "dev": "device",
                "prefsrc": "preferred_source"
            }
            for route in routes:
                translated_route = {}
                for key, value in route.items():
                    translated_route[translation_dict.get(key, key)] = value
                parsed_routes.append(translated_route)
            routes = parsed_routes
        finally:
            return routes

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

    def generate_dns_host_name(self) -> Optional[str]:
        if self.has_role("rtu"):
            return f"rtu-{self.config.get('coa')}"
        if self.has_role("mtu"):
            return f"mtu-{self.entity_id}"
        if self.has_role("vcc"):
            return f"vcc"
        if self.has_role("sip-server"):
            return f"sip"
        if self.has_role("dns-server"):
            return f"dns"
        if self.has_role("mail-server"):
            return f"mail"
        if self.has_role("file-server"):
            return f"file"
        if self.has_role("firewall"):
            return f"firewall-{self.entity_id}"
        return None

    def set_name_servers(self, name_servers: List[str], search_domain: str) -> bool:
        return self.get_namespace().set_name_servers(name_servers, search_domain)

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
        cwd = str(self.get_guest_folder())
        divider = "-e"
        use_shell = False
        pre_cmd = ""
        if "gnome-terminal" in terminal:
            divider = "--"
            dbus = shutil.which("dbus-launch")
            pre_cmd = f"{dbus} "
            use_shell = True

        cmd = f"{pre_cmd}{terminal} {divider} {shell}"

        def pre_exec_function():
            # Detach from process group to ignore signals sent to main process
            os.setpgrp()
            
        p = self.popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, shell=use_shell, stderr=subprocess.DEVNULL, preexec_fn=pre_exec_function)
        self.manage_process(p)
        return p.poll() is None

    def loopback_up(self) -> bool:
        return self.get_namespace().loopback_up()
