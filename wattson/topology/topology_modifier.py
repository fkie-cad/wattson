import importlib
from pathlib import Path

import yaml

from typing import TYPE_CHECKING
import ipaddress

from wattson.topology.constants import MANAGEMENT_SWITCH, DEFAULT_NAMESPACE, STAT_SERVER_NODE, STAT_SERVER_ID

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager

from wattson.topology.modificator_interfaces import NetworkModificatorInterface, TopologyModificatorInterface


class TopologyModifier:
    """
    Applies modifications to the topology represented by the
    given NetworkImporter and further initializes programmatic modifications.
    """

    def __init__(self, importer: 'NetworkManager', namespace: str):
        self.importer = importer
        self.namespace = namespace
        self._extension_list = []
        self._programmatic_modifiers = {
            "topology": [],
            "network_prestart": [],
            "network_poststart": []
        }

    def merge_modifications(self):
        """
        Load Modifications, if any exist, and merge them into the existing network
        representation.
        """
        if not self.importer.path.joinpath(self.importer.extensions_file).exists():
            return
        try:
            with self.importer.path.joinpath(self.importer.extensions_file).open("r") as f:
                self._extension_list = yaml.load(f, Loader=yaml.FullLoader)
                assert type(self._extension_list) == list
        except Exception as e:
            self.importer.logger.warning(
                f"{self.importer.extensions_file} could not be read. Skipping ALL extensions...")
            self.importer.logger.error(e)
            return

        for ext in self._extension_list:
            if type(ext) == str:
                p = Path(ext)
                if not p.is_absolute():
                    p = self.importer.path.joinpath(ext)

                if not p.exists():
                    raise RuntimeError(f"Invalid Extension configuration: Invalid path {ext}")
                self._apply_config_modification(p)
            elif type(ext) == dict:
                self._register_modifier(ext)
        # Global override for periodic updates
        if self.importer.disable_periodic_updates:
            self.importer.logger.info("Globally disabling periodic updates")
            self.importer.config["globals"]["do_periodic_updates"] = False
        # Inclusion PCaps from command line parameter
        if self.importer.config["store_pcaps"] is True:
            pass
        else:
            if self.importer.config["store_pcaps"] is False:
                self.importer.config["store_pcaps"] = []
            for host_id in self.importer.pcap_hosts:
                if host_id not in self.importer.config["store_pcaps"]:
                    self.importer.config["store_pcaps"].append(host_id)

    def create_management_network(self, subnet):
        sw_manage_id = MANAGEMENT_SWITCH
        main_manage_id = self.importer.get_main_management_switch()
        sw_manage_id += f"{self.importer.get_namespace_id(self.namespace)}"

        self.importer.logger.info(f"Setting up Management Switch in namespace '{DEFAULT_NAMESPACE}'")
        # Add Main Management Switch
        self.importer.graph["nodes"][main_manage_id] = {
            "type": "switch",
            "id": main_manage_id,
            "name": f"Management Switch {DEFAULT_NAMESPACE}",
            "namespace": DEFAULT_NAMESPACE,
            "interfaces": []
        }

        # Add server ports for all other namespaces
        self.importer.graph["nodes"][main_manage_id]["tap_interfaces"] = []
        self.importer.logger.info(f"Adding Tap Server Ports on {main_manage_id}")
        for i, namespace in enumerate(self.importer.get_namespaces()):
            if namespace == DEFAULT_NAMESPACE:
                continue
            self.importer.graph["nodes"][main_manage_id]["tap_interfaces"].append({
                "id": f"tap{i}",
                "mode": "server"
            })

        if self.importer.namespace != DEFAULT_NAMESPACE:
            self.importer.logger.info(f"Setting up Management Switch in namespace '{self.namespace}'")
            # Add Namespace Management Switch
            self.importer.graph["nodes"][sw_manage_id] = {
                "type": "switch",
                "id": sw_manage_id,
                "name": f"Management Switch {self.namespace}",
                "namespace": self.namespace,
                "interfaces": []
            }
            # Add client port to default namespace
            namespace_id = self.importer.get_namespace_id(self.namespace)
            self.importer.logger.info(f"Adding Tap Client for {main_manage_id}.tap{namespace_id}")
            self.importer.graph["nodes"][sw_manage_id]["tap_interfaces"] = [{
                "id": "tap0",
                "mode": "client",
                "target": f"{main_manage_id}.tap{namespace_id}"
            }]

        # Add Management Network Interface to each Hosts
        hosts = self.importer.get_hosts()
        self.importer.logger.info(f"Creating Management Subnet {subnet}")

        i = 0   # Total Management connections
        j = 0   # Namespace specific connections
        for host in hosts:
            # Management IP counts up globally for all namespaces
            if not host.get("add_to_management", True):
                self.importer.logger.info(f"Host {self.importer.host_manager.ghn(host)} has requested no management network binding")
                continue

            management_ip = self.importer.get_next_management_ip()
            network = self.importer.management_network_obj
            i += 1

            if not self.importer.host_manager.host_in_namespace(host):
                if host.get("standalone_deployment", False):
                    j += 1
                    self.importer.logger.info(
                        f"Adding direct management switch connection for standalone "
                        f"host {self.importer.host_manager.ghn(host)} ({management_ip})"
                    )
                    host.setdefault("tap_interfaces", []).append({
                        "id": "mgm",
                        "ip": f"{management_ip}/{network.prefixlen}",
                        "mode": "client",
                        "target": f"{main_manage_id}.htap{host['id']}"
                    })
                    self.importer.graph["nodes"][main_manage_id]["tap_interfaces"].append({
                        "id": f"htap{host['id']}",
                        "mode": "server"
                    })
                else:
                    self.importer.logger.info(f"Skipping management network creation for host {self.importer.host_manager.ghn(host)}"
                                              f" as host is not in current namespace")
                continue
            j += 1

            if "interfaces" not in host:
                host["interfaces"] = []
            host["interfaces"].append({
                "id": "mgm",
                "ip": f"{management_ip}/{network.prefixlen}",
                "destination_host": sw_manage_id
            })
            self.importer.graph["nodes"][sw_manage_id]["interfaces"].append({
                "id": f"mgm{i}",
                "destination_host": host["id"]
            })

            # Add Management Link
            self.importer.graph["links"].append({
                "id": f"m{i}",
                "namespace": self.importer.host_manager.get_host_namespace(host),
                "interfaces": [
                    f"{sw_manage_id}.mgm{i}",
                    f"{host['id']}.mgm"
                ],
                "delay": "0ms",
                "data-rate": "1Gbps"
            })
        self.importer.logger.debug(f"Management Switch {sw_manage_id} has {j}/{i} connections")

    def _apply_config_modification(self, path: Path):
        """
        Apply a configuration-based modification to the network graph.
        The extension is defined in a file specified by the given path.
        """
        try:
            with path.open("r") as f:
                ext = yaml.load(f, Loader=yaml.FullLoader)
        except Exception:
            raise RuntimeError(f"Cannot Load Extension Script {repr(path)}")

        self.importer.log(f"Applying extensions from {path.name}")

        if "config" in ext:
            for key, value in ext["config"].items():
                self.importer.log(f"Updating config {key} from modifier {path.name}")
                if key in self.importer.config and type(self.importer.config[key]) == dict and type(value) == dict:
                    self.importer.config[key].update(value)
                else:
                    self.importer.config[key] = value

        # Update and Insert Nodes
        # TODO:
        # Make this more intuitive (!) and enable recursive updates (?)
        if "nodes" in ext:
            for nid, node in ext["nodes"].items():
                if nid in self.importer.graph["nodes"]:
                    # Delete ?
                    if "__delete" in node:
                        self.importer.log(f"Deleting Node {nid}")
                        del self.importer.graph["nodes"][nid]
                        continue

                    # Update
                    self.importer.log(f"Updating Node {nid}")
                    for key, val in node.items():
                        if type(val) == dict:
                            if "__replace" in val and val["__replace"]:
                                # Replace whole dict instead of updating
                                del (val["__replace"])
                                self.importer.graph["nodes"][nid][key] = val
                            else:
                                if key in self.importer.graph["nodes"][nid]:
                                    if type(self.importer.graph["nodes"][nid][key]) != dict:
                                        raise RuntimeError(f"Modification requires a dict at key {key}")
                                else:
                                    self.importer.graph["nodes"][nid][key] = {}
                                self.importer.graph["nodes"][nid][key].update(val)
                        elif type(val) == list:
                            if len(val) > 0 and val[0] == "__replace":
                                # Replace existing list, but remove magic string
                                val = val[1:]
                                self.importer.graph["nodes"][nid][key] = val
                            else:
                                # Update List
                                if type(self.importer.graph["nodes"][nid][key]) != list:
                                    raise RuntimeError(f"Modification requires a list at key {key}")

                                for elem in val:
                                    if type(elem) == dict:
                                        if "id" in elem:
                                            # Replace existing Element?
                                            id_matches = [
                                                i for i, oelem in enumerate(self.importer.graph["nodes"][nid][key])
                                                if type(oelem) == dict
                                                and "id" in oelem
                                                and oelem["id"] == elem["id"]
                                            ]
                                            if len(id_matches) == 1:
                                                self.importer.graph["nodes"][nid][key][id_matches[0]] = elem
                                                continue
                                    # Otherwise: Append
                                    self.importer.graph["nodes"][nid][key].append(elem)
                        else:
                            # Update value of existing node (strings, numbers, ...)
                            self.importer.graph["nodes"][nid][key] = val
                else:
                    # Insert
                    if "__delete" not in node:
                        self.importer.log(f"Inserting Node {nid}")
                        self.importer.graph["nodes"][nid] = node

        # Replace and Insert Links
        if "links" in ext:
            for link in ext["links"]:
                link_matches = [
                    i for i, l in enumerate(self.importer.graph["links"])
                    if l["id"] == link["id"]
                ]
                if len(link_matches) == 1:
                    if "__delete" in link:
                        self.importer.log(f"Deleting Link {link['id']}")
                        del self.importer.graph["links"][link_matches[0]]
                    else:
                        self.importer.log(f"Modifying Link {link['id']}")
                        self.importer.graph["links"][link_matches[0]] = link
                else:
                    if "__delete" not in link:
                        self.importer.log(f"Adding further Link {link['id']}")
                        self.importer.graph["links"].append(link)

        # TODO
        # Consider DataPoints and Powernetwork?
        # -> Not really a communication network modification, but a complete
        #    new power network model

        return

    def apply_prestart_extensions(self):
        if "network_prestart" in self._programmatic_modifiers:
            for netmod in self._programmatic_modifiers["network_prestart"]:
                mname = self._get_modifier_name(netmod)
                self.importer.log(f"Applying Programmatic Network Prestart Modificator: {mname}")
                self.importer._net = netmod.modify_network(self.importer._net, self)

    def apply_topology_extensions(self, topo):
        for topomod in self._programmatic_modifiers["topology"]:
            mname = self._get_modifier_name(topomod)
            self.importer.log(f"Applying Programmatic Topology Modificator: {mname}")
            topo = topomod.modify_topology(topo, self)
        return topo

    def apply_poststart_extensions(self):
        if "network_poststart" in self._programmatic_modifiers:
            for netmod in self._programmatic_modifiers["network_poststart"]:
                self.importer.log(
                    f"Applying Programmatic Network Poststart Modificator: {self._get_modifier_name(netmod)}")
                self.importer._net = netmod.modify_network(self.importer._net, self)

    def add_modifier(self, modifier_type: str, modifier, prestart: bool = False):
        """
        Add a custom modifier that is not loaded from the configuration folder.
        :param modifier_type The type (both, network, topology)
        :param modifier The modifier object implementing the desired functions
        :param prestart Whether the network modifier should be executed before mininet is started
        """

        if modifier_type in ["both", "topology"]:
            if not isinstance(modifier, TopologyModificatorInterface):
                raise RuntimeError("Invalid Modificator for Topology modifications")
            self._programmatic_modifiers["topology"].append(modifier)

        if modifier_type in ["both", "network"]:
            if not isinstance(modifier, NetworkModificatorInterface):
                raise RuntimeError("Invalid Modificator for Network modifications")
            k = "network_prestart" if prestart else "network_poststart"
            self._programmatic_modifiers[k].append(modifier)

    def _register_modifier(self, modifier: dict):
        for key in ["type", "module", "class"]:
            if key not in modifier:
                raise RuntimeError(f"Invalid Programmatic Modifier, missing entry {key}")

        try:
            module = importlib.import_module(modifier["module"])
        except Exception:
            raise RuntimeError(f"Cannot Import Modifier Module {modifier['module']}")

        ocls = getattr(module, modifier["class"])
        o = ocls()

        if modifier["type"] in ["both", "topology"]:
            self._programmatic_modifiers["topology"].append(o)
        if modifier["type"] in ["both", "network"]:
            prestart = modifier["prestart"] if "prestart" in modifier else False
            k = "network_prestart" if prestart else "network_poststart"
            self._programmatic_modifiers[k].append(o)

    @staticmethod
    def _get_modifier_name(o):
        module = o.__class__.__module__
        if module is None or module == str.__class__.__module__:
            return o.__class__.__name__
        else:
            return module + '.' + o.__class__.__name__

    def add_optional_hosts(self):
        if self.importer.config["deploy_statistic_server"]:
            self.importer.graph["nodes"][STAT_SERVER_ID] = STAT_SERVER_NODE
            self.importer.config["globals"]["statistics"]["server"] = f"!management_ips.{STAT_SERVER_ID}"
