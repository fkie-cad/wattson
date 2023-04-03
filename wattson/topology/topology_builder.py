import json
from typing import TYPE_CHECKING

from mininet.node import Docker
from pathlib import Path
from .patches.docker_iphost import IPDocker

if TYPE_CHECKING:
    import wattson


class TopologyBuilder:
    def __init__(self,
                 importer: "wattson.topology.network_manager.NetworkManager",
                 topology_utils: "wattson.topology.TopologyUtils.TopologyUtils",
                 namespace: str = "main"):

        self.importer: "wattson.topology.network_manager.NetworkManager" = importer
        self.utils: "wattson.topology.TopologyUtils.TopologyUtils" = topology_utils
        self.namespace = namespace

    def build_topo(self, topo):
        """
        Builds the topology by adding hosts, switches, routers and links to the given topology
        :param topo: The base topology to build on
        :return:
        """
        self._topo_add_hosts(topo)
        self._topo_add_switches(topo)
        self._topo_add_routers(topo)
        self._topo_add_links(topo)

    def _topo_add_hosts(self, topo):
        hosts = self.importer.get_hosts()
        for host in hosts:
            if not self.importer.host_manager.host_in_namespace(host, self.namespace):
                self.importer.logger.debug(f"Skipping {self.importer.host_manager.ghn(host)} as host is not in current namespace")
                continue
            host_name = self.importer.host_manager.ghn(host)
            if "deploy" in host and host["deploy"]["type"] == "docker":
                image = host["deploy"]["image"]
                dcmd = host["deploy"].get("dcmd") # will default to /bin/bash in addHost
                mounting = [f"{self.importer.host_manager.get_host_directory(host)}:/wattson_tmp:rw"]
                if "volumes" in host["deploy"]:
                    for volume in host["deploy"]["volumes"]:
                        source_path, mount_path = volume.split(":", 1)
                        resolved_source_path = self.importer.path.joinpath(Path(source_path)).resolve(strict=True)
                        mounting.append(f"{resolved_source_path}:{mount_path}")
                self.importer.logger.info(f"Using Docker for host {host_name} with image {image} and mounting {mounting}")
                topo.addHost(name=host_name, ip="", cls=IPDocker, dimage=image, dcmd=dcmd, volumes=mounting)
            else:
                topo.addHost(name=host_name, ip="")

    def _topo_add_switches(self, topo):
        switches = self.importer.get_switches()
        stp = self.importer.config.get("switches_use_stp", False)
        if stp:
            self.importer.logger.info("Enabling STP for ALL switches")
        for switch in switches:
            if not self.importer.host_manager.host_in_namespace(switch, self.namespace):
                self.importer.logger.debug(f"Skipping {self.importer.host_manager.ghn(switch)} as switch is not in current namespace")
                continue

            switch_name = self.importer.host_manager.ghn(switch)
            local_stp = stp or switch.get("stp")
            if local_stp:
                topo.addSwitch(switch_name, rstp=True, failMode="standalone")
            else:
                topo.addSwitch(switch_name)

    def _topo_add_routers(self, topo):
        routers = self.importer.get_routers()
        for router in routers:
            if not self.importer.host_manager.host_in_namespace(router, self.namespace):
                self.importer.logger.debug(f"Skipping {self.importer.host_manager.ghn(router)} as router is not in current namespace")
                continue
            router_name = self.importer.host_manager.ghn(router)
            topo.addRouter(router_name, ip="")

    def _topo_add_links(self, topo):
        links = self.importer.get_links()
        for link in links:
            if not self.importer.link_manager.link_in_namespace(link, self.namespace):
                self.importer.logger.debug(f"Skipping {link['id']} as link is not in current namespace")
                continue

            iface_left = link["interfaces"][0]
            iface_right = link["interfaces"][1]
            hid_left, iid_left = self.utils.parse_interface(iface_left)
            hid_right, iid_right = self.utils.parse_interface(iface_right)

            linkopts = self.utils.get_linkopts(link)

            mac_left = self.utils.get_iface_mac(iface_left)
            mac_right = self.utils.get_iface_mac(iface_right)
            linkopts["addr1"] = mac_left
            linkopts["addr2"] = mac_right

            params_left = {}
            ip_left = self.utils.get_iface_ip(iface_left)
            if ip_left is not None:
                params_left["ip"] = ip_left
                self.importer.add_network(ip_left)
            params_right = {}
            ip_right = self.utils.get_iface_ip(iface_right)
            if ip_right is not None:
                params_right["ip"] = ip_right
                self.importer.add_network(ip_right)

            topo.addLink(
                self.importer.host_manager.ghn(hid_left), self.importer.host_manager.ghn(hid_right),
                params1=params_left, params2=params_right, **linkopts
            )
