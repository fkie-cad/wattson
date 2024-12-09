import subprocess
from time import sleep
from typing import Optional, Callable

from wattson.services.deployment import PythonDeployment
from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger


class WattsonZeekService(WattsonService):

    def __init__(self, service_configuration, network_node):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("WattsonZeek", "WattsonZeek")
        self.interface = self._service_configuration["interface"]
        self.inter_startup_delay = self._service_configuration["inter_startup_delay_s"]

    def start(self, refresh_config: bool = False):
        super().start(refresh_config=refresh_config)
        self.network_node.popen(["sysctl", "-w", "vm.max_map_count=1677720"], shell=True)
        self.network_node.popen(["/usr/bin/mongod", "--config", "/etc/mongod.conf", "--fork"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["su", "opensearch", "-c", "'/usr/share/opensearch/bin/opensearch'"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["/usr/share/graylog-server/bin/graylog-server"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["filebeat", "setup", "-e"], shell=True)
        sleep(self.inter_startup_delay)
        self.network_node.popen(["filebeat", "-e"], shell=True)
        sleep(self.inter_startup_delay)
        p = self.network_node.popen(["zeek", "-i", self.interface], shell=True)
        self._process = p
        return p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.logger.error("Stop command not (yet) implemented for ZEEK service.")
        return success

    def write_zeek_configuration(self, refresh_config: bool = False):
        if not refresh_config and not self.get_artifact("node.cfg").is_empty():
            return

        self.network_node.logger.debug(f"Writing Zeek Config")
        return

        networks = []
        subnets = []

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

        config_file = self.get_artifact("ospf.cfg").get_current()
        with config_file.open("w") as f:
            f.write("\n".join(config_lines))
        config_file.chmod(0o777)