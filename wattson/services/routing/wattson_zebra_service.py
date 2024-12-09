import socket
from pathlib import Path
from typing import List, Optional

from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.configuration import ServiceConfiguration
from wattson.services.routing.wattson_fr_routing_service import WattsonFrRoutingService


class WattsonZebraService(WattsonFrRoutingService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.frr_artifacts = [
            self.get_artifact("zebra.pid"),
            self.get_artifact("zebra.cfg"),
            self.get_artifact("zebra.api"),
            self.get_artifact("zebra_vty", is_folder=True)
        ]

    def write_fr_config_file(self, refresh_config: bool = False):
        if not refresh_config and not self.get_artifact("ospf.cfg").is_empty():
            return
        self.network_node.logger.debug(f"Writing Zebra Config")
        config_lines = [
            f"hostname {self.network_node.system_name}",
            "password zebra",
            ""
            "interface lo",
            "  no shutdown",
            "  description -> n/a",
            "  link-detect",
            "",
            "!"
        ]

        subnets = []
        for interface in self.network_node.get_interfaces():
            if interface.is_management:
                continue
            if interface.has_ip():
                subnets.append(interface.get_subnet())
            config_lines.extend([
                f"interface {interface.interface_name}",
                f"  no shutdown",
                f"  description {interface.interface_name}",
                f"  link-detect",
                "",
                "!"
            ])

        """
        nat_found = False

        for subnet in subnets:
            nodes_in_subnet = self.network_node.network_emulator.find_nodes_in_subnet(subnet)
            for node in nodes_in_subnet:
                if node.has_role("nat"):
                    nat_ip = node.get_primary_ip_address_string(with_subnet_length=False)
                    if nat_ip is not None:
                        # self.network_node.logger.info(f"Found NAT in subnet, setting default route via {nat_ip}")
                        nat_found = True
                        break
            if nat_found:
                break
        """

        config_file = self.get_artifact("zebra.cfg").get_current()
        with config_file.open("w") as f:
            f.write("\n".join(config_lines))
        config_file.chmod(0o777)

    def pre_start(self):
        super().pre_start()
        self.get_artifact("zebra.pid").get_current().unlink(missing_ok=True)

    def get_socket_file(self) -> Optional[Path]:
        return self.get_tmp_path(self.get_artifact("zebra.api"))

    def get_start_command(self) -> List[str]:
        config_file = self.get_artifact("zebra.cfg")
        pid_file = self.get_artifact("zebra.pid")
        socket_file = self.get_artifact(f"zebra.api")
        vty_folder = self.get_artifact("zebra_vty", is_folder=True)
        vty_folder.get_current().mkdir(exist_ok=True)

        cmd = [
            "zebra",
            "-f", str(self.get_tmp_path(config_file)),
            "-i", str(self.get_tmp_path(pid_file)),
            "-z", str(self.get_tmp_path(socket_file)),
            "--vty_socket", str(self.get_tmp_path(vty_folder)),
            "--log", "stdout",
            "--log-level", "debug",
            "-u", "root"
        ]
        # self.network_node.logger.info(" ".join(cmd))
        return cmd

    def get_pid_file(self):
        return self.get_artifact("zebra.pid")
