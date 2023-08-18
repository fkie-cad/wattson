import ipaddress
from typing import List, Optional

from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.configuration import ServiceConfiguration
from wattson.services.routing.wattson_fr_routing_service import WattsonFrRoutingService


class WattsonOSPFService(WattsonFrRoutingService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.frr_artifacts = [
            self.get_artifact("ospf.pid"),
            self.get_artifact("ospf.cfg"),
            self.get_artifact("zebra.api"),
            self.get_artifact("ospf_vty", is_folder=True)
        ]

    def write_fr_config_file(self):
        self.network_node.logger.debug(f"Writing OSPF Config")

        networks = []

        config_lines = [
            f"hostname {self.network_node.system_name}",
            f"password zebra",
            f"",
            f"interface lo",
            f"  ip ospf priority 10",
            f"  ip ospf cost 1",
            f"  ip ospf passive"
            "",
            "!"
        ]
        networks.append("127.0.0.1/8")

        id_ip = None

        for interface in self.network_node.get_interfaces():
            if interface.is_management:
                continue
            if not interface.has_ip():
                continue
            config_lines.extend([
                f"interface {interface.interface_name}",
                f"  ip ospf priority 10",
                f"  ip ospf cost 1",
            ])
            passive = True
            next_node = interface.get_next_node()
            if next_node is not None:
                routers = interface.network_emulator.find_routers(next_node)
                if len(routers) > 1:
                    # At least one other router exists -> active interface
                    passive = False
            if passive:
                config_lines.extend(
                    [
                        f"  no ip ospf passive"
                    ]
                )
            else:
                config_lines.extend(
                    [
                        f"  no ip ospf passive",
                        f"  ip ospf dead-interval minimal hello-multiplier 5",
                        f"  ip ospf hello-interval 1"
                    ]
                )
            config_lines.append("!")

            if id_ip is None or interface.ip_address > id_ip:
                id_ip = interface.ip_address

            networks.append(interface.ip_address_string)

        if id_ip is None:
            id_ip = ipaddress.IPv4Address("127.0.0.1")

        config_lines.extend([
            f"router ospf",
            f"  ospf router-id {id_ip}"
        ])
        for network in networks:
            config_lines.append(f"  network {network} area 0.0.0.0")
        config_lines.append("!")

        config_file = self.get_artifact("ospf.cfg").get_current()
        with config_file.open("w") as f:
            f.write("\n".join(config_lines))
        config_file.chmod(0o777)

    def pre_start(self):
        super().pre_start()
        self.get_artifact("ospf.pid").get_current().unlink(missing_ok=True)

    def get_start_command(self) -> List[str]:
        config_file = self.get_artifact("ospf.cfg")
        pid_file = self.get_artifact("ospf.pid")
        socket_file = self.get_artifact(f"zebra.api")
        vty_folder = self.get_artifact("ospf_vty", is_folder=True)
        return [
            "ospfd",
            "-f", str(self.get_tmp_path(config_file)),
            "-i", str(self.get_tmp_path(pid_file)),
            "-z", str(self.get_tmp_path(socket_file)),
            "--vty_socket", str(self.get_tmp_path(vty_folder)),
            "--log", "stdout",
            "--log-level", "debug",
            "-u", "root"
        ]
