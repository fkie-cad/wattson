from pathlib import Path
from typing import Union, Optional

from wattson.topology.constants import DEFAULT_NAMESPACE, DEFAULT_HOST_DEPLOY_PRIORITY


class HostManager:
    def __init__(self, importer: 'NetworkManager'):
        self.importer = importer

    def get_host_namespace(self, host: Union[str, dict]) -> str:
        host = self.importer.get_node(host)
        if "namespace" in host:
            return host["namespace"]
        return DEFAULT_NAMESPACE

    def get_hostname(self, host) -> str:
        """
        Returns the hostname for a given host or hostid.
        This handles potential prefixes.
        :param host The host or hostid to derive the hostname for
        :return The Mininet hostname for this host.
        """
        if type(host) == dict:
            if "id" in host:
                host = host["id"]

        if type(host) == str:
            if self.importer._prefix_hostnames:
                return f"n{host}"
            else:
                return host
        else:
            raise RuntimeError("Invalid Host provided to get_hostname")

    def ghn(self, host: Union[str, dict]) -> str:
        """
        Convenience method for get_hostname.
        """
        return self.get_hostname(host)

    def get_host_directory(self, host) -> Path:
        host_dir = self.importer.host_dir_root.joinpath(f"{self.importer.host_manager.ghn(host)}")
        host_dir.mkdir(exist_ok=True)
        return host_dir

    def get_net_host(self, host: Union[str, dict]):
        if self.importer._net is None:
            return None
        hostname = self.get_hostname(host)
        return self.importer._net.get(hostname)

    def _decide_hostname_prefix_requirement(self):
        for node_id in self.importer.graph["nodes"]:
            if node_id[0].isdigit():
                self.importer._prefix_hostnames = True
                return

    def get_host_deploy_priority(self, host: Union[str, dict]) -> int:
        host = self.importer.get_node(host)
        if "priority" in host:
            return int(host["priority"])
        if host.get("type") == "field":
            return 3
        if host.get("deploy", {}).get("class") == "ProfileLoaderDeployment":
            return 2
        return DEFAULT_HOST_DEPLOY_PRIORITY

    def profile_loader_exists(self) -> bool:
        for host in self.importer.get_hosts():
            if host.get("deploy", {}).get("class") == "ProfileLoaderDeployment":
                return True
        return False

    def get_hosts_by_type(self, *hosttypes: str):
        self.importer._check_graph()
        _nodes = [node for i, node in self.importer.graph["nodes"].items() if node["type"] in hosttypes]
        return sorted(_nodes, key=lambda n: self.get_host_deploy_priority(n))

    def host_is_external(self, host: Union[str, dict]) -> bool:
        # TODO: No usage - delete?
        return self.get_host_namespace(host) != DEFAULT_NAMESPACE

    def host_in_namespace(self, host: Union[str, dict], namespace: Optional[str] = None) -> bool:
        if namespace is None:
            namespace = self.importer.namespace
        return self.get_host_namespace(host) == namespace