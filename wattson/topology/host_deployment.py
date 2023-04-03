import copy
import ipaddress
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from subprocess import Popen, TimeoutExpired, STDOUT
from typing import Union, TYPE_CHECKING, List, Optional

import psutil

from . import network_utils
from .constants import LOG_FOLDER, CONFIG_FOLDER, DEFAULT_MIRROR_PORT
from .network_utils import get_node_interfaces
from .process.pcap_process import PcapProcess
from .process.screen_process import ScreenProcess
from .process.wattson_process import WattsonProcess
from ..util.namespace import Namespace

if TYPE_CHECKING:
    import wattson


from wattson.topology.constants import DEFAULT_MTU_DEPLOY, DEFAULT_RTU_DEPLOY, DEFAULT_FIELD_DEPLOY


class HostDeployment:
    """
    Deploys Hosts based on their text-based configuration and the Mininet Instance.
    """

    def __init__(
            self,
            importer: 'wattson.topology.network_manager.NetworkManager',
            utils: 'wattson.topology.utils.TopologyUtils',
            namespace: str):

        self.importer = importer
        self.utils = utils
        self.namespace = namespace
        self._processes = {}
        self._managed_processes = {}
        self._initialized_hosts = []
        self._tmp_files: List[Path] = []
        self._expansion_context = None

        self.MAX_PROCESS_WAIT_TIMEOUT = 5
        self.MAX_PROCESS_KILL_TIMEOUT = 1

    def deploy_hosts(self, **kwargs):
        """
        Deploys all registered hosts according to their deploy specification, i.e.,
        the respective logic is started.
        """
        delay = kwargs.get("deploywait", 0)
        _switches = self.importer.get_switches()
        for switch in _switches:
            sid = switch["id"]
            if self._pcap_requested(sid):
                self.start_pcap(switch)

        _hosts = self.importer.get_ip_hosts()
        for host in _hosts:
            print(host["id"], end="  ", flush=True)
            self.deploy_host(host)
            time.sleep(delay)
        print("")

    def teardown(self):
        self.importer.logger.info("Stopping hosts...")
        for hid in self._processes.keys():
            print(f"{hid}", end="  ", flush=True)
            self.host_stop(hid)
        print("")
        if len(self._managed_processes) > 0:
            self.importer.logger.info("Stopping managed processes...")
            proc: Popen
            for pname, proc in self._managed_processes.items():
                print(f"{pname}", end="  ", flush=True)
                proc.terminate()
                try:
                    proc.wait(5)
                except TimeoutExpired:
                    print(f"(killed)  ", end="  ", flush=True)
                    proc.kill()
        print("")
        for file in self._tmp_files:
            file.unlink(missing_ok=True)

    def popen(self, host: Union[str, dict], cmd, **kwargs) -> subprocess.Popen:
        namespace: Namespace = self.importer.get_network_namespace(host)
        return namespace.popen(cmd, **kwargs)

    def exec(self, host: Union[str, dict], cmd):
        namespace: Namespace = self.importer.get_network_namespace(host)
        return namespace.exec(cmd)

    """
    Deploys a single Host
    """
    def deploy_host(self, host: Union[str, dict], tmp_path: Union[Path, str] = "/tmp",
                    inplace_config_file: bool = False):
        host = self.importer.get_node(host)
        hid = host["id"]

        if type(tmp_path) == str:
            tmp_path = Path(tmp_path)
        if not self.importer.host_manager.host_in_namespace(host, self.namespace):
            if host.get("standalone_deployment", False):
                self.importer.logger.info(f"Preparing standalone deployment for host {self.importer.host_manager.ghn(host)}")
                host = self._add_default_deploy_if_missing(host)
                tmp_config = {
                    "hostid": hid,
                    "host": host,
                    "deploy_config": False
                }
                if "deploy" in host:
                    if host["deploy"]["type"] != "python":
                        self.importer.logger.warning(f"Dedicated host deployment only supported for Python-based hosts")
                        return
                    host_config = self._expand_deploy_config(host)
                    proc = WattsonProcess.get_process(host, host_config, self.importer, prepare_only=True)
                    config_file: Path = proc.start()
                    if inplace_config_file:
                        tmp_config_file = tmp_path.joinpath(config_file.name)
                        shutil.copy(config_file, tmp_config_file)
                        config_file = tmp_config_file

                    tmp_config["deploy_config"] = str(config_file.absolute())

                tmp_file = tmp_path.joinpath(f"wattson_standalone_{hid}")
                self._tmp_files.append(tmp_file)
                self.importer.logger.info(f"Writing config to {str(tmp_file.absolute())}")
                with tmp_file.open("w") as f:
                    json.dump(tmp_config, f)
            else:
                self.importer.logger.debug(f"Skipping deployment of {self.importer.host_manager.ghn(host)} as "
                                           f"host is not in current namespace")
            return

        if hid in self._processes:
            for p in self._processes[hid]:
                p.start()
        else:
            host = self._add_default_deploy_if_missing(host)
            self._processes[hid] = []

            if self._pcap_requested(hid):
                pcaps = self._deploy_pcap(hid, inline=True)
                if len(pcaps) == 0:
                    self.importer.logger.error(f"Failed to start packet capture on host {self.importer.host_manager.ghn(hid)}")

            if "deploy" in host:
                host_config = self._expand_deploy_config(host)
                p = WattsonProcess.get_process(host, host_config, self.importer)
                self._processes[hid].append(p)

            for p in self._processes[hid]:
                if p.do_autostart():
                    p.start()
        if hid not in self._initialized_hosts:
            self._initialized_hosts.append(hid)
            # Kernel Offloading?
            offloading = self.importer.config["disable_checksum_offloading"]
            if offloading is True or (isinstance(offloading, list) and self.importer.host_manager.get_hostname(host) in offloading):
                self.importer.logger.debug(f"Disabling Checksum Offloading for Host {self.importer.host_manager.get_hostname(host)}")
                net_host = self.importer.host_manager.get_net_host(host)
                net_host.cmd("python3 -c 'from wattson.util.misc import disable_checksum_offloading; "
                             "disable_checksum_offloading()'")
            # SysCtl
            self.apply_sysctl_config(host)

    def apply_sysctl_config(self, host):
        hid = host["id"]
        net_host = self.importer.host_manager.get_net_host(host)
        sysctl_config = self.importer.config["sysctl"]
        assignments = {}
        for key, value in sysctl_config.items():
            if type(value) == dict:
                # Host specific config
                if key == hid:
                    # value is mapping of sysctl options
                    assignments.update(value)
            elif key not in assignments:
                # Value is sysctl value for option 'key'
                assignments[key] = value
        for key, value in assignments.items():
            net_host.cmd(f"sysctl -w {key}={value}")

    def register_process(self, process: Popen, key: Optional[str] = None):
        if key is None:
            key = f"pid-{process.pid}"
        self._managed_processes[key] = process

    def start_screen(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        hid = host["id"]
        hostname = self.importer.host_manager.ghn(host)
        p_info = WattsonProcess.get_process_info(len(self._processes[hid]), host, {}, self.importer)
        p_screen = ScreenProcess(process_info=p_info, manager=self.importer)
        print(f"Starting new screen for Host {hostname}")
        print(f"Attach as user root using 'screen -R {p_screen.get_screen_id()}'")
        self._processes[hid].append(p_screen)
        p_screen.start()

    def start_pcap(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        hid = host["id"]
        pcaps = self._deploy_pcap(hid)
        if pcaps is None:
            return False
        for pcap in pcaps:
            if pcap.is_running():
                print(f"Re-Starting Packet capture {hid}")
                pcap.stop()
                pcap.start()
            else:
                print(f"Starting new Packet capture for {hid}")
                pcap.start()

    def host_stop(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        hid = host["id"]
        if hid not in self._processes:
            print("Host has no known process")
            return True
        for p in self._processes[hid]:
            p.stop()
        return True

    def host_start(self, host: Union[str, dict]):
        self.deploy_host(host)
        return True

    def host_restart(self, host: Union[str, dict]):
        if self.host_is_running(host):
            self.host_stop(host)
        return self.host_start(host)

    def host_is_running(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        hid = host["id"]
        if hid in self._processes:
            for p in self._processes[hid]:
                if p.is_running():
                    return True
        return False

    def host_num_processes(self, host: Union[str, dict]):
        num = 0
        host = self.importer.get_node(host)
        hid = host["id"]
        if hid in self._processes:
            for p in self._processes[hid]:
                if p.is_running():
                    num += 1
        return num

    def host_get_processes(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        hid = host["id"]
        if hid in self._processes:
            return self._processes[hid]
        return []

    def host_get_pids(self, host: Union[str, dict]):
        procs = self.host_get_processes(host)
        pids = []
        for p in procs:
            pids.append(p.get_pid())
        return pids

    def host_get_pid(self, host: Union[str, dict]):
        h = self.importer.host_manager.get_net_host(host)
        return h.pid

    def host_get_active_logs(self, host):
        processes = self.host_get_processes(host)
        logs = []
        for p in processes:
            if p.is_running() and not isinstance(p, PcapProcess):
                logs.append(p.get_log_file())
        return logs

    def host_get_all_logs(self, host):
        processes = self.host_get_processes(host)
        logs = []
        for p in processes:
            logs.append(p.get_log_file())
        return logs

    def _add_default_deploy_if_missing(self, host):
        if "deploy" not in host:
            if host["type"] == "mtu":
                host["deploy"] = self._get_mtu_default_deploy(host)
            elif host["type"] == "rtu":
                host["deploy"] = self._get_rtu_default_deploy(host)
            elif host["type"] == "field":
                host["deploy"] = self._get_field_default_deploy(host)

            else:
                return host

        if "passconfig" not in host["deploy"]:
            host["deploy"]["passconfig"] = "tmpfile"
        return host

    def expand_deploy_config(self, host: Union[str, dict]):
        return self._expand_deploy_config(host)

    def _expand_deploy_config(self, host: Union[str, dict]):
        host = self.importer.get_node(host)
        config = {
            "hostid": host["id"],
            "hostname": self.importer.host_manager.ghn(host["id"]),
            "random_seed": self.importer.config["globals"]["random_seed"],
            "artifacts_dir": str(self.importer.artifacts_dir.absolute())
        }

        deploy = host["deploy"]
        if "config" in deploy:
            config.update(copy.deepcopy(deploy["config"]))
            for key, val in config.items():
                config[key] = self._expand_deploy_config_rec(host, val)
                if type(config[key]) == dict:
                    # Recursively expand the key
                    for sub_key, sub_val in config[key].items():
                        if type(sub_val) == str:
                            config[key][sub_key] = self._expand_deploy_config_rec(host, sub_val)

        return config

    def _expand_deploy_config_rec(self, host: Union[str, dict], val):
        if type(val) == str:
            if "!" in val:
                expanded = []
                # Treat every component individually
                elements = val.split(".")
                for e in elements:
                    if len(e) > 0 and e[0] == "!":
                        expanded.append(self._expand_deploy_config_keyword(host, e))
                    else:
                        expanded.append(e)

                self._expansion_context = {
                    "host": host,
                    "root": val
                }
                val = self._handle_deploy_expansion(expanded)
                self._expansion_context = None
                return val
            return val

        if type(val) == dict:
            for k, v in val.items():
                val[k] = self._expand_deploy_config_rec(host, v)
            return val

        if type(val) == list:
            for i, v in enumerate(val):
                val[i] = self._expand_deploy_config_rec(host, v)
            return val

        return val

    def _expand_deploy_config_keyword(self, host: dict, keyword: str):
        if keyword == "!datapoints":
            return self.importer.datapoints
        if keyword == "!nodeid" or keyword == "!hostid":
            return host["id"]
        if keyword == "!coa":
            return host["coa"] if "coa" in host else host["id"]
        if keyword == "!hostname":
            return self.importer.host_manager.ghn(host)
        if keyword == "!rtu_ips":
            return self._get_rtu_ips_by_hosts()
        if keyword == "!rtu_map":
            return self._get_rtu_map_by_hosts()
        if keyword == "!mtus":
            return self._get_mtu_id_list()
        if keyword == "!ip":
            return self.utils.get_host_ip(host).split("/")[0]
        if keyword == "!ips":
            return self.utils.get_host_ips(host)
        if keyword == "!coord_ip":
            return self.utils.get_host_management_ip("coord").split("/")[0]
        if keyword == "!mgmip":
            return self.utils.get_host_management_ip(host).split("/")[0]
        if keyword == "!fields":
            return self.importer.get_field_devices_by_rtu_id()
        if keyword == "!primary_ips":
            return self.utils.get_primary_host_ips()
        if keyword == "!management_ips":
            return self.utils.get_management_ips()
        if keyword == "!management_network":
            return self.utils.get_management_network()
        if keyword == "!network_graph":
            return self.importer.get_network_graph()
        if keyword == "!node_ids":
            return self.importer.get_typed_node_ids()
        if keyword == "!coas":
            return self.importer.get_typed_node_coas()
        if keyword == "!raw_powernet":
            self.importer.get_powernet()
            return self.importer.raw_powernet
        if keyword == "!profile_loader_exists":
            return self.importer.host_manager.profile_loader_exists()
        if keyword == "!scenario_path":
            return self.importer.path.absolute().__str__()
        if keyword == "!globals":
            if "globals" not in self.importer.config:
                return {}
            return self.importer.config["globals"]
        if keyword == "!host_folder":
            return str(self.importer.host_manager.get_host_directory(host))
        return keyword

    def _handle_deploy_expansion(self, expansions):
        if len(expansions) > 0:
            root = expansions[0]
            tail = self._handle_deploy_expansion(expansions[1:])
            if tail is None:
                return root
            if type(root) == dict:
                if type(tail) == str and tail in root:
                    return root[tail]
                #raise ValueError(f"Invalid Deploy Expansion: {tail} does not exist")
                print(f"ERROR: Invalid Deploy Expansion: {tail} does not exist - Using None.")
                context = self._expansion_context["root"]
                host = self._expansion_context["host"]["id"]
                print(f"   Tried to expand {context} for host {host}")
            elif type(root) == list:
                if str(tail).isdigit() and 0 <= int(tail) < len(root):
                    return root[int(tail)]
                #raise ValueError(f"Invalid Deploy Expansion: {tail} is no valid list index")
                print(f"ERROR: Invalid Deploy Expansion: {tail} is no valid list index")
                context = self._expansion_context["root"]
                host = self._expansion_context["host"]["id"]
                print(f"   Tried to expand {context} for host {host}")
            elif type(root) == str and len(root) > 0 and root[0] == "!":
                if root == "!len":
                    return len(tail)
                raise ValueError(f"Unknown expansion function: {root[1:]}")
            else:
                return f"{root}.{tail}"
        return None

    def _get_mtu_default_deploy(self, host: dict):
        return DEFAULT_MTU_DEPLOY

    def _get_rtu_default_deploy(self, host: dict):
        return DEFAULT_RTU_DEPLOY

    def _get_field_default_deploy(self, host: dict):
        return DEFAULT_FIELD_DEPLOY

    def stop_processes(self):
        for _, processes in self._processes.items():
            for p in processes:
                p.stop()

    @staticmethod
    def _write_deploy_config_tmpfile(configobject, host, launchconfig):
        (fd, fname) = tempfile.mkstemp(host["id"])
        with open(fname, "w") as file:
            file.write(json.dumps(configobject))
            launchconfig["config_file"] = file.name
        os.close(fd)
        return launchconfig

    def _get_rtu_ips_by_hosts(self):
        rtu_ips = {}
        for host in self.importer.get_mtus():
            if "rtu_ips" in host:
                rtu_ips[str(host["id"])] = host["rtu_ips"]
        return rtu_ips

    def _get_rtu_map_by_hosts(self):
        rtu_map = {}
        for host in self.importer.get_mtus():
            if "rtu_ips" in host:
                rtu_map[str(host["id"])] = {}
                for rtu_id, ip in host["rtu_ips"].items():
                    port = None
                    if ":" in ip:
                        ip, port = str(ip).split(":", 1)
                    n = self.importer.get_node(str(rtu_id))
                    info = {
                        "ip": ip,
                        "coa": n["coa"] if "coa" in n else n["id"]
                    }
                    if port is not None:
                        info["port"] = port
                    rtu_map[str(host["id"])][rtu_id] = info
        return rtu_map

    def _deploy_pcap(self, hid, inline=False, management_interfaces: bool = False) -> Optional[List[PcapProcess]]:
        host = self.importer.get_node(hid)

        pcaps = []

        if not self.importer.host_manager.host_in_namespace(host, self.namespace):
            self.importer.logger.debug(f"Skipping PCAP on host {self.importer.host_manager.ghn(host)} as host is not"
                                       f"in current namespace")
            return None

        if hid not in self._processes:
            self._processes[hid] = []

        for p in self._processes[hid]:
            if isinstance(p, PcapProcess):
                pcaps.append(p)
        if len(pcaps) > 0:
            return pcaps

        if host["type"] == "switch":
            bridge = self.importer.ghn(host)
            tap = f"{bridge}-{DEFAULT_MIRROR_PORT}"
            p_info = WattsonProcess.get_process_info(len(self._processes[hid]), host, {}, self.importer)
            p_pcap = PcapProcess(process_info=p_info, manager=self.importer, interface=tap)
            self._processes[hid].append(p_pcap)
            pcaps.append(p_pcap)
            return pcaps

        interfaces = get_node_interfaces(self.importer.host_manager.get_net_host(host))

        for interface_info in interfaces:
            interface = interface_info["name"]
            a = interface_info["ip"]
            if interface == "lo":
                continue

            if not management_interfaces:
                net: ipaddress.IPv4Network = self.importer.get_management_network()
                if a in net:
                    continue

            if inline:
                print(f"{interface}-pcap", end="  ", flush=True)
            else:
                print(f"Initializing PCAP for Host {self.importer.host_manager.get_hostname(host)} at interface {interface}")
            p_info = WattsonProcess.get_process_info(len(self._processes[hid]), host, {}, self.importer)
            p_pcap = PcapProcess(process_info=p_info, manager=self.importer, interface=interface)
            self._processes[hid].append(p_pcap)
            pcaps.append(p_pcap)
        return pcaps

    def _pcap_requested(self, hid):
        host = self.importer.get_node(hid)
        pcaps = self.importer.config["store_pcaps"]
        if pcaps is True or (isinstance(pcaps, list) and (self.importer.host_manager.ghn(host) in pcaps or hid in pcaps)):
            return True
        return False

    def _get_mtu_id_list(self):
        mtus = self.importer.get_mtus()
        mtu_ids = []
        for mtu in mtus:
            mtu_ids.append(mtu["id"])
        return mtu_ids
