import multiprocessing
import threading

import zmq
import copy
import datetime
import hashlib
import ipaddress
import json
import logging
import shlex
import shutil
import subprocess
import sys
import time
from io import BytesIO
from os import getcwd
from pathlib import Path
from threading import Event
from typing import Union, Optional, Dict, List, Tuple
from urllib.request import urlopen
from zipfile import ZipFile
from wattson.exceptions.host_already_exists_exception import HostAlreadyExistsException

import mininet.nodelib  # without this import tings break
import ipmininet.ipnet
import ipmininet.iptopo
import pandapower as pp
import psutil
import yaml

import wattson.util
from wattson.powergrid import CoordinationClient
from wattson.topology import network_utils
from wattson.topology.cli.cli import CLI
from wattson.topology.constants import DEFAULT_NAMESPACE, MANAGEMENT_SWITCH
from wattson.topology.data_point_loader import DataPointLoader
from wattson.topology.host_deployment import HostDeployment
from wattson.topology.host_manager import HostManager
from wattson.topology.link_manager import LinkManager
from wattson.topology.mininet_manager import MininetManager
from wattson.topology.topology_builder import TopologyBuilder
from wattson.topology.topology_modifier import TopologyModifier
from wattson.topology.utils import TopologyUtils
from wattson.util.namespace import Namespace


class NetworkManager:
    """
    Imports a Co-Simulation Network from a YAML-based configuration.
    Format has to follow the format created by the PowerNetTranslator from the
    cps-ids project.
    """

    def __init__(self, path: Union[str, Path], **kwargs):
        self.path: Path

        self.loglevel = "info"
        if "loglevel" in kwargs:
            self.loglevel = kwargs["loglevel"]
        logging_level = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.warning,
            "critical": logging.CRITICAL
        }.get(self.loglevel, logging.INFO)

        self.logger = wattson.util.get_logger("Topology Manager", "Topology Manager",
                                              use_context_logger=False, use_async_logger=False)
        self.logger.setLevel(logging_level)
        self.logger.info(f"Using Python binary: {Path(sys.executable).resolve().absolute()}")
        self.logger.info(f"Using Wattson module: {Path(wattson.util.__file__).resolve().parent.parent.absolute()}")

        if type(path) == str:
            self.path = Path(path)
        else:
            self.path = path

        self._force_scenario_update = kwargs.get("force_scenario_update", False)
        self.preparation = kwargs.get("preparation", [])
        self.extensions_file = kwargs.get("extensions", "extensions.yml")
        self._check_install()
        self._check_prepare()

        self.namespace = kwargs.get("namespace", DEFAULT_NAMESPACE)
        self.coord_client = None
        self.random_seed = kwargs.get("random_seed", None)
        self.disable_periodic_updates = kwargs.get("disable_periodic_updates", False)

        scenario_name = self.path.name
        utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        scenario_name_time = f"{scenario_name}_{utc_time}"

        self.config = {
            "tap_port_start": 26900,                 # Starting Port for SSH-Tapped Ports
            "tap_id_start": 10,                      # First device is tapX (e.g., tap10)
            "switch": "wattson.networking.nodes.patched_ovs_switch.PatchedOVSSwitch",
            #"switch": "mininet.nodelib.LinuxBridge",
            "attach_to_coordinator": kwargs.get("attach_to_coordinator", False),  # Whether to create a Coordination Client within Wattson's main process
            "switches_use_stp": False,
            "subnet_prefix_length": 24,
            "ip_base": "0.0.0.0/0",
            "link": "wattson.topology.patches.wattson_ip_link.WattsonIPLink",
            "deterministic_mac": True,     # Whether to force the same MAC for a host across runs
            "mac_prefix": "02",            # An optional global prefix for the mac in hex (not-prefixed) hex notation
            "controller": "wattson.networking.nodes.l2_controller.L2Controller",
            "controller_port": 6653,
            "deploy_statistic_server": False,
            "auto_adjust_controller_port": True,
            "inter_namespace_ssh": None,
            "use_v6": False,
            "host_dir_root": False,
            "persistent_artifacts": True,
            "artifacts_dir": "wattson-artifacts",
            "persistent_logs": True,                # Deprecated
            "persistent_logs_dir": "logs",
            "clear_host_root_on_teardown": True,
            "store_pcaps": False,                    # List of hostnames, False, or True
            "disable_checksum_offloading": True,     # List of hostnames, False, or True
            "sysctl": {             # Set SysCtl configuration. Either use key => value or host_id => {key => value}

            },
            "globals": {
                "coord_config": {},
                "mtu_connect_delay": 0.5,
                "random_seed": time.time_ns(),
                "periodic_update_ms": 10000,
                "periodic_update_start": {},
                "rtu_logic": {},
                "statistics_folder": None,
                "artifacts_path": None,
                "do_general_interrogation": True,
                "do_clock_sync": True,
                "do_periodic_updates": True,
                "statistics": {
                    "server": False,
                    "max_size": 20000,
                    "folder": None
                }
            },
            "management_network": "10.0.0.0/8"  # Valid values are "False" or a Subnet (e.g., "10.0.0.0/8")
        }
        self.switch_cls = None

        self.networks = []

        if "config" in kwargs:
            self.config.update(kwargs["config"])

        self.artifacts_dir = Path(getcwd()).joinpath(self.config.get("artifacts_dir")).joinpath(scenario_name_time)
        self.artifacts_dir.mkdir(parents=True)
        symlink_current = self.artifacts_dir.parent.joinpath("_current")
        if symlink_current.is_symlink():
            symlink_current.unlink()
        if not symlink_current.exists():
            self.logger.info("Creating _current as symlink for current run")
            symlink_current.symlink_to(self.artifacts_dir.relative_to(symlink_current.parent))

        statistics_folder = self.artifacts_dir.joinpath("statistics")
        if self.config["globals"]["statistics_folder"] is None:
            statistics_folder.mkdir()
            self.config["globals"]["statistics_folder"] = str(statistics_folder.absolute())
        if self.config["globals"]["statistics"]["folder"] is None:
            statistics_folder.mkdir(exist_ok=True)
            self.config["globals"]["statistics"]["folder"] = str(statistics_folder.absolute())
        if self.config["globals"]["artifacts_path"] is None:
            self.config["globals"]["artifacts_path"] = str(self.artifacts_dir.absolute())

        if not self.config["host_dir_root"]:
            self.host_dir_root = self.artifacts_dir.joinpath("hosts")
            self.host_dir_root.mkdir(parents=True, exist_ok=True)
        else:
            self.host_dir_root = Path(self.config["host_dir_root"])
            self.host_dir_root.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"NetworkManager namespace {self.namespace}")
        self._namespaces = []
        self.tap_bridges = {}
        self._ssh_proc = None
        self.link_manager = LinkManager(self)
        self.host_manager = HostManager(self)
        self.mininet_manager = MininetManager(self)
        self._override_switch = kwargs.get("switch")
        self._override_link = kwargs.get("link")

        self._link_map = {}

        self._used_macs = {}

        self.pcap_hosts = kwargs.get("pcap", [])

        self._restart_requested = False
        self.devices: dict = {}
        self.graph = None
        self._prefix_hostnames = False
        self._prefix_linknames = False
        self.enable_log = True
        self.raw_powernet = None
        self.powernet = None
        self._net = None
        self.topo = None
        self.gui = kwargs.get('gui', False)
        self.cli = CLI(self)
        self.utils = TopologyUtils(self, namespace=self.namespace)
        self.deployment = HostDeployment(self, self.utils, namespace=self.namespace)
        self.topology_modifier = TopologyModifier(self, namespace=self.namespace)
        self.topology_builder = TopologyBuilder(self, self.utils, namespace=self.namespace)
        self._running = Event()
        self._start_time = datetime.datetime.now()
        self._management_address_generator = None
        self.management_network_obj = None
        self._network_namespaces: Dict[str, Namespace] = {}
    def handle_attack_commands(self, topic, data):
        for attack in data:
            thread = threading.Thread(target=self.handle_attack, args=[attack])
            thread.start()

    def handle_attack(self, attack):
        if self.wait_for_time_to_start(attack):
            old_element = self.execute_attack(attack)
            if old_element:
                self.reverse_attack(attack, old_element)

    def wait_for_time_to_start(self, attack):
        if self.coord_client.get_sim_time() > attack["execution_delay_from_start"]:
            # missed the time the attack is supposed to be executed
            return False
        # sleep until attack is supposed to be executed
        time.sleep(attack["execution_delay_from_start"] - self.coord_client.get_sim_time())
        return True

    def execute_attack(self, attack):
        old_element = None
        start_time = self.coord_client.get_sim_time()
        current_time = start_time

        while current_time - start_time <= attack["duration"]:
            if attack["element_type"] == "link":
                if attack["reverse"]:
                    old_element = self.link_manager.get_link_from_id(attack["id"]).copy()
                    attack["reverse"] = False
                self.handle_attack_link_command(attack)
            elif attack["element_type"] == "host":
                if attack["reverse"]:
                    old_element = self.host_manager.get_net_host(attack["id"]).copy()
                    attack["reverse"] = False
                self.handle_attack_host_command(attack)
            if attack["interval"] == 0:
                break
            time.sleep(attack["interval"])
            current_time = self.coord_client.get_sim_time()
        return old_element

    def reverse_attack(self, attack, old_element):
        reverse_attack = self.get_reverse_attack(attack, old_element)
        if attack["element_type"] == "link":
            self.handle_attack_link_command(reverse_attack)
        elif attack["element_type"] == "host":
            self.handle_attack_host_command(reverse_attack)

    def get_reverse_attack(self, attack, old_element):
        if attack["element_type"] == "host":
            if attack["action"] == "stop":
                attack["action"] = "start"
            elif attack["action"] == "start":
                attack["action"] = "stop"
        elif attack["element_type"] == "link":
            if attack["action"] == "modify":
                if attack["field"] == "data-rate":
                    attack["value"] = f"{old_element[attack['field']]}mbps"
                elif attack["field"] not in old_element:
                    attack["value"] = 0.0
                else:
                    attack["value"] = old_element[attack["field"]]
            elif attack["action"] == "down":
                attack["action"] = "up"
            elif attack["action"] == "up":
                attack["action"] = "down"
        return attack

    def handle_attack_host_command(self, attack):
        if attack["action"] == "stop":
            self.deployment.host_stop(attack["id"])
        elif attack["action"] == "start":
            self.deployment.host_start(attack["id"])
        elif attack["action"] == "restart":
            self.deployment.host_restart(attack["id"])

    def handle_attack_link_command(self, attack):
        if attack["action"] == "modify":
            self.link_manager.set_link_parameter(attack["id"], attack["field"], attack["value"])
        elif attack["action"] == "down" or attack["action"] == "up":
            self.link_manager.set_link_status(attack["id"], attack["value"])
        elif attack["action"] == "remove":
            self.link_manager.remove_link(attack["id"])
        elif attack["action"] == "stop":
            self.link_manager.stop_link(attack["id"])

    def teardown(self):
        for namespace in self._network_namespaces.values():
            namespace.clean()

    def get_management_network(self):
        return self.management_network_obj

    def get_next_management_ip(self):
        if self._management_address_generator is None:
            self.management_network_obj = ipaddress.IPv4Network(self.config.get("management_network"))
            self._management_address_generator = self.management_network_obj.hosts()

        while True:
            a = next(self._management_address_generator)
            if a.packed[3] == 0 or a.packed[3] == 255:
                continue
            return str(a)

    def get_deterministic_mac(self, node_id: str, interface_id: str) -> str:
        """
        Returns a deterministic MAC address for the given interface.
        In case a collision occurs, a warning is issued.
        :param node: The node the interface belongs to (dict)
        :param interface: The interface (dict)
        :return: A valid MAC address
        """
        prefix = self.config.get("mac_prefix", "")
        prefix_bytes = int(len(prefix) / 2)
        if len(prefix) % 2 != 0:
            raise ValueError(f"Invalid MAC prefix {prefix} length {prefix_bytes}: Only full bytes are allowed")
        if prefix_bytes > 2:
            raise ValueError("Invalid MAC prefix length: At most 2 byte long prefixes are allowed")
        postfix_length = 6 - prefix_bytes
        n_id = node_id
        i_id = interface_id

        node_postfix = 0
        iface_postfix = 0

        iface_hash_length = 2
        node_hash_length = postfix_length - iface_hash_length

        collision = True
        mac = None
        while collision:
            # Create unique ID for node and interface
            n_hash_id = n_id if node_postfix == 0 else f"{n_id}-{node_postfix}"
            i_hash_id = i_id if iface_postfix == 0 else f"{i_id}-{iface_postfix}"
            # Hash the unique IDs
            node_hash = hashlib.sha256(n_hash_id.encode("utf-8"))
            iface_hash = hashlib.sha256(i_hash_id.encode("utf-8"))
            # Shorten the hashes to match their respective parts of the MAC
            node_mac = node_hash.digest()[:node_hash_length].hex()
            iface_mac = iface_hash.digest()[:iface_hash_length].hex()
            # Check for collisions
            if node_mac not in self._used_macs:
                self._used_macs[node_mac] = {
                    "node": n_id,
                    "node_hash_id": n_hash_id,
                    "interfaces": {}
                }

            mac_info: dict = self._used_macs[node_mac]
            if mac_info["node"] != n_id:
                # Node part of MAC is already used by another node - add postfix
                collision_node = mac_info["node"]
                self.logger.warning(f"MAC collision for {n_id} with node {collision_node}!")
                node_postfix += 1
                continue
            if iface_mac in mac_info["interfaces"] and mac_info["interfaces"][iface_mac] != i_id:
                collision_iface = mac_info["interfaces"][iface_mac]
                self.logger.warning(f"MAC collision for interface {i_id}@{n_id} with interface {collision_iface}!")
                iface_postfix += 1
                continue
            collision = False
            mac_info["interfaces"][iface_mac] = i_id
            mac = f"{prefix}{node_mac}{iface_mac}"
            mac = ':'.join(mac[i:i + 2] for i in range(0, 12, 2))
        return mac

    def add_modifier(self, modifier_type: str, modifier, prestart: bool = False):
        self.topology_modifier.add_modifier(modifier_type, modifier, prestart)

    def get_cosimnet(self):
        """
        Returns the Mininet and the PandaPower networks.
        """
        return self.mininet_manager.get_mininet(), self.get_powernet()

    def get_mininet(self):
        return self.mininet_manager.get_mininet()

    def get_powernet(self) -> pp.auxiliary.pandapowerNet:
        """
        Returns the PandaPower Network based on the configuration.
        """
        if self.powernet is not None:
            return self.powernet

        dicts = self._load_powernet_dicts_from_file()
        self.powernet = pp.from_json_string(json.dumps(dicts))
        self._sanitize_powernet()
        powernet_str = pp.to_json(self.powernet)
        self.raw_powernet = powernet_str.encode("utf-8").hex()
        return self.powernet

    def _load_powernet_dicts_from_file(self):
        """
        Reads the pandapower network representation from the corresponding yaml-file and restores the
        dictionaries that define the network.
        :return: A dictionary containing the dictionaries defining the pandapower network
        """
        with self.path.joinpath("powernetwork.yml").open("r") as f:
            dicts = yaml.load(f, Loader=yaml.FullLoader)
            #f.seek(0)
            #powernet_str = f.read()
            #powernet_str = json.dumps(dicts)
        return dicts

    def deploy_host(self, host: Union[str, dict]):
        self.deployment.deploy_host(host)

    def deploy_hosts(self, **kwargs):
        self.deployment.deploy_hosts(**kwargs)

    def create_coordinator_client(self):
        if not self.config.get("attach_to_coordinator", False):
            return
        ip = self.deployment.utils.get_host_management_ip("coord").split("/")[0]
        self.coord_client = CoordinationClient(
            ip,
            logger=self.logger.getChild("CoordClient"),
            node_id="WattsonMain",
            namespace=self.get_network_namespace("coord")
        )
        self.coord_client.subscribe(self.handle_attack_commands, "ATTACK_COMMAND")

    def start_coordinator_client(self):
        if self.coord_client is not None:
            self.coord_client.start()

    def disconnect_coordinator(self):
        if self.coord_client is not None:
            self.coord_client: CoordinationClient
            try:
                self.coord_client.stop()
            except Exception as e:
                self.logger.error(e)

    def _build_containernet(self, net, topo):
        print('*** Adding Hosts:')
        for hostName in topo.hosts():
            net.addHost(hostName, **topo.nodeInfo(hostName))
            #self.addDocker(hostName, dimage=dimage, **topo.nodeInfo(hostName))
            print(hostName, end=" ")

        print("")
        print('*** Adding switches:')
        for switchName in topo.switches():
            # A bit ugly: add batch parameter if appropriate
            params = topo.nodeInfo(switchName)
            cls = params.get('cls', self.switch_cls)
            params["cls"] = cls
            net.addSwitch(switchName, **params)
            print(switchName, end=" ")

        print("")
        print("*** Adding links:")
        for srcName, dstName, params in topo.links(sort=True, withInfo=True):
            print(f"{params}")
            net.addLink(**params)
            print(f"({srcName}, {dstName})", end=" ")
        print("")

    def get_link_map(self):
        return self._link_map

    def get_override_link(self):
        return self._override_link

    def get_topology(self, extensions: bool = True) -> ipmininet.iptopo.IPTopo:
        """
        Returns the Topology based on the given Configuration.
        Already applies configuration and programmatic Modifications.
        This can be disabled via the extensions parameter.
        """
        if self.topo is not None:
            return self.topo
        # Load Basic Topology
        self._load_communication_network_from_files()
        # Load (custom) Modifications
        if extensions:
            self.topology_modifier.merge_modifications()
        # Add optional default hosts
        self.topology_modifier.add_optional_hosts()
        if self.random_seed is not None:
            self.config["globals"]["random_seed"] = self.random_seed
        seed = self.config['globals']['random_seed']
        self.logger.info(f"Using Random Seed: {seed}")
        # Validate if any Hostname starts with a digit and has to be prefixed
        self.host_manager._decide_hostname_prefix_requirement()
        # do the same for switches
        self.link_manager.decide_linkname_prefix_requirement()

        # Add Management Network if required

        self._extract_namespaces()

        if self.config["management_network"]:
            self.topology_modifier.create_management_network(self.config["management_network"])
        # Add Nodes and Links
        self.topology_builder.build_topo(self.topo)
        # Programmatic Topology Modifications
        if extensions:
            self.topo = self.topology_modifier.apply_topology_extensions(self.topo)

        return self.topo

    def add_network(self, network: Union[ipaddress.IPv4Network, str]):
        if isinstance(network, str):
            network = ipaddress.IPv4Network(network, strict=False)
        for n in self.networks:
            if n == network:
                return
        self.networks.append(network)

    def get_network(self, ip: Union[ipaddress.IPv4Address, str]):
        if isinstance(ip, str):
            ip = ipaddress.IPv4Address(ip.split("/")[0])
        for n in self.networks:
            if ip in n:
                return n

    def _load_communication_network_from_files(self):
        """
        Loads the communication network topology as well as associated IEC 104 datapoints from the respective
        configuration YAML-files.
        :return:
        """
        self.topo = ipmininet.iptopo.IPTopo()
        self._load_network_graph_from_file()
        self.datapoints = {}
        self._load_datapoints_from_file()

    def _get_datapoint_identifier(self, datapoint):
        # TODO: Adjust this for new datapoint format
        #return f"{datapoint['coa']}.{datapoint['ioa']}"
        return datapoint["identifier"]

    def _load_datapoints_from_file(self):
        loader = DataPointLoader(self.path)
        self.datapoints = loader.get_data_points()

    def _load_network_graph_from_file(self):
        try:
            with self.path.joinpath("graph.yml").open("r") as f:
                self.graph = yaml.load(f, Loader=yaml.FullLoader)
        except Exception:
            raise RuntimeError("Invalid Network configuration: Cannot read graph.yml")

    def addHost(self, nid: str, **kwargs):
        """
        Adds a host with a single interface.
        """
        if nid in self.graph["nodes"]:
            self.logger.warning(f"Node with id {nid} already exists.")
            raise HostAlreadyExistsException(f"Node with id {nid} already exists.")

        ip = kwargs.pop('ip', None)
        switch = None
        switch = kwargs.get('switch', None)
        namespace = kwargs.get("namespace", DEFAULT_NAMESPACE)

        iid = "i1"
        interfaces = []
        if ip is None:
            if switch is not None:
                if switch == self.get_main_management_switch():
                    ip = f"{self.get_next_management_ip()}/{self.management_network_obj.prefixlen}"
                    iid = "mgm"
                else:
                    ip = self.utils.get_ip_for_switch_subnet(switch)
        elif switch is None:
            switch = self.utils.get_switch_for_subnet(ip)

        if ip is not None:
            interfaces = [
                {
                    "id": iid,
                    "ip": ip
                }
            ]

        self.graph["nodes"][nid] = {
            "id": nid,
            "namespace": namespace,
            "name": nid,
            "type": "host",
            "interfaces": interfaces
        }
        self.graph["nodes"][nid].update(kwargs)

        if switch is not None:
            switchiface = self._add_iface(switch)
            link = self.link_manager.addLink(f"{nid}.{iid}", f"{switch}.{switchiface}")

        # Add to Topology and Network
        if self.topo is None:
            raise RuntimeError("Topology does not yet exist")

        for instance in [self.topo, self._net]:
            if instance is None:
                continue
            hname = self.host_manager.ghn(nid)
            instance.addHost(hname, ip="")
            if switch is not None:
                sname = self.host_manager.ghn(switch)
                linkopts = self.utils.get_linkopts(link)
                instance.addLink(hname, sname, params1={"ip": ip}, params2={}, **linkopts)

    def get_container_name(self, host: Union[str, dict]) -> str:
        return f"mn.{self.host_manager.get_hostname(host)}"

    def get_hostname(self, host: Union[str, dict]) -> str:
        return self.host_manager.get_hostname(host)

    def ghn(self, host: Union[str, dict]) -> str:
        return self.get_hostname(host)

    def get_node_by_ip(self, ip: str):
        for _, node in self.graph["nodes"].items():
            if "interfaces" in node:
                for interface in node["interfaces"]:
                    if "ip" in interface and ip in interface["ip"]:
                        return node

    def get_field_devices_by_rtu_id(self) -> Dict:
        out = {}
        for rtu in self.get_rtus():
            fields = rtu.get("fields", {})
            out[rtu["id"]] = fields
        return out

    def get_nodes(self):
        return [node for _, node in self.graph["nodes"].items()]

    def get_switches(self):
        return self.host_manager.get_hosts_by_type("switch")

    def is_switch(self, node: Union[dict, str]):
        node = self.get_node(node)
        return node["type"] == "switch"

    def get_routers(self):
        return self.host_manager.get_hosts_by_type("router")

    def get_rtus(self):
        return self.host_manager.get_hosts_by_type("rtu")

    def get_mtus(self):
        return self.host_manager.get_hosts_by_type("mtu")

    def get_field_devices(self):
        return self.host_manager.get_hosts_by_type("field")

    def get_hosts(self):
        return self.host_manager.get_hosts_by_type("rtu", "mtu", "attacker", "host", "field")

    def get_ip_hosts(self):
        return self.host_manager.get_hosts_by_type("rtu", "mtu", "attacker", "host", "router",
                                       "field")

    def get_node(self, node: Union[str, dict]):
        self._check_graph()
        ret_node = None
        if type(node) == dict:
            ret_node = node
        elif node in self.graph["nodes"]:
            ret_node = self.graph["nodes"][node]
        if ret_node is None:
            return None
        if "interfaces" not in ret_node:
            ret_node["interfaces"] = []
        return ret_node

    def get_typed_node_ids(self):
        """
        Returns a dict of node IDs sorted by node type.
        The special key "all" contains all node IDs
        """
        nodes = self.get_nodes()
        d = {
            "all": [],
            "rtus_with_pandapower": []
        }
        for n in nodes:
            nid = n["id"]
            d["all"].append(nid)
            t = n["type"]
            if t not in d:
                d[t] = []
            if t == "rtu":
                if self._rtu_uses_pandapower(nid):
                    d["rtus_with_pandapower"].append(nid)
            d[t].append(nid)
        return d

    def _rtu_uses_pandapower(self, node_id):
        dps = self.datapoints[node_id]
        for dp in dps:
            for ptype in ["sources", "targets"]:
                if ptype in dp["providers"]:
                    for provider in dp["providers"][ptype]:
                        if provider["provider_type"] == "pandapower":
                            return True
        return False

    def get_typed_node_coas(self):
        """
        Returns a dict of node COAs sorted by node type.
        The special key "all" contains all node IDs
        """
        nodes = self.get_nodes()
        d = {
            "all": []
        }
        for n in nodes:
            coa = n["coa"] if "coa" in n else n["id"]
            d["all"].append(coa)
            t = n["type"]
            if t not in d:
                d[t] = []
            d[t].append(coa)
        return d

    def get_links(self):
        self._check_graph()
        return self.graph["links"]

    def get_prefix_linknames(self):
        return self._prefix_linknames

    def _check_graph(self):
        if self.graph is None:
            raise RuntimeError("Graph not present")

    def _add_iface(self, host: Union[str, dict], iface=None) -> str:
        if iface is None:
            iface = {}
        if type(host) == str:
            host = self.get_node(host)

        if "id" not in iface:
            iface["id"] = self.utils.gen_iface_id(host)
        self.graph["nodes"][host["id"]]["interfaces"].append(iface)
        return iface["id"]

    def get_network_graph(self):
        graph = copy.deepcopy(self.graph)
        self.link_manager.graph_add_tap_links(graph)
        return graph

    def log(self, msg: str, level = logging.INFO):
        if self.enable_log:
            self.logger.log(level, msg)

    def request_restart(self):
        self._restart_requested = True

    def restart_requested(self) -> bool:
        return self._restart_requested

    def is_running(self):
        return self._running.is_set()

    def start_running(self):
        if not self.is_running():
            self._running.set()

    @property
    def management_network(self) -> str:
        return self.config['management_network']

    def _check_prepare(self):
        """
        For scenario folders with a `prepare.py` which contains a class names `Scenario` that implements the
        `ScenarioInterface`.
        :return:
        """
        prepare_path = self.path.joinpath("prepare.py")
        if not prepare_path.is_file():
            self.logger.debug("No preparation class found")
            return
        self.logger.info("Preparing scenario")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("scenario.preparation", str(prepare_path.absolute()))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            scenario = mod.Scenario(logger=self.logger.getChild("Scenario"))
            params = {}
            for p in self.preparation:
                arg, value = p.split(":", 1)
                params[arg] = value
            path = scenario.prepare(params)
            if path is None:
                self.logger.error("Preparation failed. See log.")
                return False
            self.path = path
        except Exception as e:
            self.logger.error(f"Failed to prepare scenario: {e}")
            raise e
            return False
        return True

    def _check_install(self) -> bool:
        """
        For scenario folders with external install dependencies, this method checks its current state and
        installs the required components
        @return:
        """
        install_file = self.path.joinpath("install.yml")
        if install_file.exists():
            self.path = self.path.joinpath("scenario")
            if self.path.joinpath("scenario").is_dir():
                if self._force_scenario_update:
                    self._install_dependencies(install_file, update=self._force_scenario_update)
                else:
                    self.logger.info("Skipping Scenario installation - dependency already satisfied")
                    return True
            else:
                self._install_dependencies(install_file)  # first installation
        return True

    def _remove_source_folder(self):
        try:
            shutil.rmtree(self.path.joinpath("scenario"))
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete old version: {e}")
            return False

    def _install_dependencies(self, install_file, update=False):
        with install_file.open("r") as f:
            install_info = yaml.load(f, Loader=yaml.FullLoader)
            if "source" not in install_info:
                self.logger.warning(f"Invalid install source information")
                return False
            source = install_info["source"]
            if source["type"] == "zip":
                if update and self._remove_source_folder():
                    return True if self._install_zip(source) else False  # fresh install
            elif source["type"] == "git":
                return True if self._update_git(source) else False
            elif source["type"] == "python":
                return True if self._install_python(source, True) else False
            else:
                self.logger.error(f"Unknown scenario installation type: {source['type']}")
                return False

    def _install_python(self, source, do_update: bool = False):
        self.logger.info(f"Installing Python (Update: {do_update})")
        script = source["script"]
        script_path = self.path.joinpath(script)
        cmd = f"python3 -u {script_path} {'update' if do_update else 'install'}"
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        for line in p.stdout:
            print(line.decode("utf-8"), end="")
        p.wait()
        return p.returncode == 0

    def _install_zip(self, source):
        print(f"Installing to {self.path.joinpath('scenario')}")
        http_zip = urlopen(source["url"])
        zip_file = ZipFile(BytesIO(http_zip.read()))
        zip_file.extractall(self.path.joinpath("scenario"))
        return True

    def _install_git(self, source):
        print(f"Installing to {self.path.joinpath('scenario')}")
        cmd = [f"git clone {source['url']} {self.path.joinpath('scenario')}"]
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        for line in p.stdout:
            print(line.decode("utf-8"), end="")
        p.wait()
        install_path = self.path.joinpath("scenario").joinpath("install.py")
        if install_path.exists():
            self._install_python({"script": "scenario/install.py"})
        return p.returncode == 0

    def _update_git(self, source):
        print(f"Updating Git in {self.path.joinpath('scenario')}")
        cmd = ["git pull"]
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, cwd=self.path.joinpath("scenario"))
        for line in p.stdout:
            print(line.decode("utf-8"), end="")
        p.wait()
        return p.returncode == 0

    def _extract_namespaces(self):
        for node in self.get_nodes():
            namespace = self.host_manager.get_host_namespace(node)
            if namespace not in self._namespaces:
                if namespace == DEFAULT_NAMESPACE:
                    self._namespaces.insert(0, namespace)
                else:
                    self._namespaces.append(namespace)

    def get_namespaces(self) -> list:
        """
        Returns the list of namespaces in the scenario
        :return: The list of all namespaces
        """
        return self._namespaces

    def get_tap_devices(self) -> list:
        return list(self.tap_bridges.values())

    def restart_tap_bridge(self, tap_id):
        if tap_id not in self.tap_bridges:
            self.logger.error(f"Invalid tap bridge: {tap_id}")
            return False
        tap = self.tap_bridges[tap_id]
        p: subprocess.Popen = tap["process"]
        p.terminate()
        cmd = tap["cmd"]
        tap_dev_name = tap["dev"]
        log_id = tap["log_iteration"] + 1
        tap["log_iteration"] = log_id
        log_file = self.host_dir_root.joinpath(f"socat_server_{tap_dev_name}_{log_id}.log")
        with log_file.open("w") as f:
            socat_proc = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)
        tap["process"] = socat_proc
        if not network_utils.wait_for_interface(tap_dev_name, 5):
            self.logger.error(f"Tap device {tap_dev_name} not available after 5 seconds")
            return False
        return True

    def get_namespace_id(self, namespace: Optional[str] = None) -> int:
        if namespace is None:
            return self.get_namespace_id(self.namespace)

        try:
            return self._namespaces.index(namespace)
        except ValueError:
            return -1

    def get_main_management_switch(self):
        return f"{MANAGEMENT_SWITCH}{self.get_namespace_id(DEFAULT_NAMESPACE)}"

    def exec_with_output(self, cmd: str, print_on_error: bool = False) -> Tuple[int, List[str]]:
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                universal_newlines=True)
        output = []
        for line in proc.stdout:
            output.append(line.strip())
        proc.wait()
        return_code = proc.returncode
        if return_code != 0 and print_on_error:
            for line in output:
                print(line)
        return return_code, output

    def _exec(self, cmd: str) -> bool:
        """
        Execute the given command locally.
        :param cmd: The command to execute
        :return: True iff the command returns a success code
        """
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in proc.stdout:
            self.logger.info(line)
        proc.wait()
        if proc.returncode != 0:
            self.logger.error(f"Failed to execute command: {cmd}")
        return proc.returncode == 0

    def wait_for_cpu(self, cpu_limit, timeout, log: bool = False):
        if timeout == 0:
            return
        timeout = time.time() + timeout
        while time.time() <= timeout:
            cpu_load = psutil.cpu_percent()
            for proc in psutil.process_iter():
                info = proc.as_dict(attrs=["cpu_percent"])
                cpu_load = max(cpu_load, info["cpu_percent"])
            if cpu_load <= cpu_limit:
                if log:
                    self.logger.info(f"CPU load threshold ok: {cpu_load}%")
                return
            elif log:
                self.logger.info(f"CPU load threshold exceeded: {cpu_load}%")

    def _sanitize_powernet(self):
        # Add "slack_weight" if not existing
        add_defaults = {
            "slack_weight": {
                "ext_grid": 1.0,
                "gen": 0.0,
                "xward": 1.0
            }
        }
        self.logger.debug("Sanitizing grid for backwards compatibility")
        for column, info in add_defaults.items():
            for table, def_value in info.items():
                if table in self.powernet and column not in self.powernet[table]:
                    self.logger.debug(f"Column '{column}' not existing for {table} - adding with value {def_value}")
                    self.powernet[table][column] = def_value

    def get_mininet_node(self, host: Union[str, dict]):
        node = self.get_node(host)
        if node is None:
            return None
        hostname = self.ghn(node)
        if self.host_manager.host_in_namespace(node):
            net_host = self.get_mininet().get(hostname)
            return net_host
        return None

    def get_network_namespace(self, host: Union[str, dict]):
        host = self.get_node(host)
        hostname = self.ghn(host)
        if hostname in self._network_namespaces:
            namespace = self._network_namespaces[hostname]
        else:
            net_host = self.get_mininet().get(hostname)
            namespace = Namespace(f"w_{hostname}")
            namespace.from_pid(net_host.pid)
            self._network_namespaces[hostname] = namespace
        return namespace
