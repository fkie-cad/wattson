import ipaddress
from pathlib import Path

import time
from typing import List, Optional

from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.configuration import ServiceConfiguration
from wattson.services.routing.wattson_fr_routing_service import WattsonFrRoutingService


class WattsonFrMgmtService(WattsonFrRoutingService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.frr_artifacts = [
            self.get_artifact("mgmt.pid"),
            self.get_artifact("frr-mgmt.cfg"),
            #self.get_artifact("mgmt_vty", is_folder=True),
            self.get_artifact("zebra.api"),
        ]

    def write_fr_config_file(self, refresh_config: bool = False):
        if not refresh_config and not self.get_artifact("frr-mgmt.cfg").is_empty():
            return

        self.network_node.logger.debug(f"Writing FRR Config")

        networks = []
        subnets = []

        # Original: OSPF config

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
                from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
                if isinstance(next_node, WattsonNetworkRouter):
                    passive = False
                else:
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
                        #f"  ip ospf dead-interval 5",
                        #f"  ip ospf hello-interval 1"
                    ]
                )
            config_lines.append("!")
            subnets.append(interface.get_subnet())

            if id_ip is None or interface.ip_address > id_ip:
                id_ip = interface.ip_address

            networks.append(interface.ip_address_string)

        # Check for NAT
        t_start = time.perf_counter()
        originate_default_route = False
        nats = self.network_node.network_emulator.find_nodes_by_role("nat")
        for nat in nats:
            for subnet in subnets:
                if nat.has_subnet(subnet):
                    nat_ip = nat.get_primary_ip_address_string(with_subnet_length=False)
                    if nat_ip is not None:
                        self.network_node.logger.info(f"Setting default route via {nat_ip}")
                        self.network_node.exec(["ip", "route", "add", "default", "via", nat_ip])
                        originate_default_route = True
                        break

            if originate_default_route:
                break

        if id_ip is None:
            id_ip = ipaddress.IPv4Address("127.0.0.1")

        config_lines.extend([
            f"router ospf",
            f"  ospf router-id {id_ip}",
            f"  redistribute connected",
        ])
        if originate_default_route:
            config_lines.extend([
                f"  default-information originate always",
                f"  redistribute static",
                f"  redistribute kernel",
            ])
        for network in networks:
            config_lines.append(f"  network {network} area 0.0.0.0")
        config_lines.append("!")

        config_file = self.get_artifact("frr-mgmt.cfg").get_current()
        with config_file.open("w") as f:
            f.write("\n".join(config_lines))
        config_file.chmod(0o777)

    def pre_start(self):
        super().pre_start()
        self.get_artifact("mgmt.pid").get_current().unlink(missing_ok=True)

    def get_start_command(self) -> List[str]:
        pid_file = self.get_artifact("mgmt.pid")
        socket_file = self.get_artifact(f"zebra.api")
        # vty_folder = self.get_artifact("mgmt_vty", is_folder=True)
        return [
            "mgmtd",
            "-N", str(self.network_node.system_name),
            "-i", str(self.get_tmp_path(pid_file)),
            "-z", str(self.get_tmp_path(socket_file)),
            "--log", "stdout",
            "--log-level", "debug"
        ]

    def get_tmp_config_path(self) -> Path:
        return self.get_tmp_path(self.get_artifact("frr-mgmt.cfg"))

    def get_pid_file(self):
        #return self.get_frr_path().joinpath("mgmtd.pid")
        return self.get_artifact("mgmt.pid")

    def get_vty_socket(self) -> Path:
        return self.get_frr_path().joinpath("mgmtd_vty")
        return self.get_tmp_path(self.get_artifact("mgmt_vty", is_folder=True))
