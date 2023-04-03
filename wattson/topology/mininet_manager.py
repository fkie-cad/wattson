import datetime
import json
import re
import shlex
import shutil
import subprocess
import time
from os.path import relpath
from pathlib import Path
from typing import Dict, Optional

import ipmininet.ipnet
import mininet
import mininet.node
import psutil

import wattson.util
from wattson.topology import network_utils
from wattson.topology.constants import DEFAULT_LINK_DATARATE

class MininetManager:
    def __init__(self, importer: 'NetworkManager'):
        self.importer = importer
        self._tap_ports = {}

    def _add_ovs_controller(self, net):
        """
        Adds a L2Controller to the Mininet network if the selected switch class extends the OVSSwitch.
        :param net: The Mininet network
        """
        if self.importer.config["controller"] and issubclass(self.importer.switch_cls, mininet.node.OVSSwitch):
            self.importer.logger.info("Adding Controller")
            controller = wattson.util.dynamic_load_class(self.importer.config["controller"])
            port = self.importer.config["controller_port"]
            if self.importer.config["auto_adjust_controller_port"]:
                port += self.importer.get_namespace_id()
            c1 = controller("WattsonController", port=port)
            net.addController(c1)

    def _instantiate_mininet(self, topo) -> ipmininet.ipnet.IPNet:
        """
        Instantiates the Mininet network based on the given topology
        :param topo: The network topology
        :return: An IP-based Mininet network
        """
        self._instantiate_link_type()
        return ipmininet.ipnet.IPNet(
            topo=topo,
            ipBase=self.importer.config["ip_base"],
            switch=self.importer.switch_cls,
            link=wattson.util.dynamic_load_class(self.importer.config["link"]),
            use_v6=self.importer.config["use_v6"],
            build=False
        )

    def _instantiate_link_type(self):
        link_type = self.importer.get_override_link()
        if link_type is not None:
            if link_type.lower() == "default":
                self.importer.config["link"] = "mininet.link.Link"
            elif link_type.lower() == "tclink":
                self.importer.config["link"] = "mininet.link.TCLink"

    def start_mininet(self, net: ipmininet.ipnet.IPNet = None, extensions: bool = True):
        """
        Starts the internal or given Mininet, fixes Router loopback interfaces,
        waits for switches to connect and returns the network.
        """
        if net is None:
            net = self.importer.mininet_manager.get_mininet()
        self.importer.start_running()
        self.importer._start_time = datetime.datetime.now()
        net.start()

        while not net.waitConnected(timeout=10):
            self.importer.logger.info("Waiting for Switches to connect")

        if len(self.importer.get_namespaces()) > 1:
            self.importer.logger.info("Setting up inter-namespace interfaces")
            self._setup_inter_namespace_interfaces()

        self.importer.logger.info("Setting up Switch Mirror Ports")
        self._setup_switch_mirror_ports()

        self.importer.logger.info("Setting up physical interfaces")
        self._setup_physical_interfaces()

        self.importer.logger.info("Fixing Router IPs...")
        for router in self.importer.get_routers():
            if not self.importer.host_manager.host_in_namespace(router):
                self.importer.logger.debug(
                    f"Skipping router {self.importer.host_manager.ghn(router)} as not in current namespace")
                continue
            self._fix_router_loopback(net, router)
        self.importer.logger.info("Fixing Docker Interfaces...")
        for host in self.importer.get_hosts():
            if not self.importer.host_manager.host_in_namespace(host):
                self.importer.logger.debug(
                    f"Skipping docker host {self.importer.host_manager.ghn(host)} as not in current namespace")
                continue
            if "deploy" in host and host["deploy"]["type"] == "docker":
                self._fix_docker_routing(net, host)

        if extensions:
            self.importer.topology_modifier.apply_poststart_extensions()

        return net

    def shutdown_mininet(self, net: ipmininet.ipnet.IPNet = None):
        """
        Terminates all hosts of the given Mininet and finally shuts down the Mininet
        """
        if net is None:
            net = self.importer.mininet_manager.get_mininet()
        self.importer._running.clear()
        self.importer.deployment.stop_processes()
        for host in net.hosts:
            try:
                host.sendInt()
                print(host.waitOutput())
            finally:
                pass

        try:
            if len(self.importer.get_namespaces()) > 0:
                for tap_interface in self.importer.tap_bridges.values():
                    self.importer.logger.info(f"Shutting down Tap Bridge {tap_interface['dev']}")
                    p: subprocess.Popen = tap_interface["process"]
                    process = psutil.Process(p.pid)
                    for proc in process.children(recursive=True):
                        proc.kill()
                    p.terminate()
                    p.wait()
            if self.importer._ssh_proc is not None:
                self.importer.logger.info(f"Shutting down SSH Port Forwarding")
                self.importer._ssh_proc.terminate()
                self.importer._ssh_proc.wait()
        except Exception as e:
            self.importer.logger.error(e)
        finally:
            pass

        net.stop()

        #self.importer.logger.info("Cleaning coordinator namespace attachment")
        #self.importer.coord_client_namespace.clean()

        if self.importer.config["persistent_logs"]:
            # Copy Logs to unique location
            log_dir = self.importer.artifacts_dir.joinpath("logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            self.importer.logger.info(f"Copying logs to {log_dir}")
            for log_file in self.importer.host_dir_root.glob("**/*.log"):
                target_file = log_dir.joinpath(log_file.name)
                shutil.move(log_file, target_file)
                log_file.symlink_to(Path(relpath(target_file, log_file.parent)))
            for pcap_file in self.importer.host_dir_root.glob("**/**/*.pcap"):
                target_file = log_dir.joinpath(pcap_file.name)
                shutil.move(pcap_file, target_file)
                pcap_file.symlink_to(Path(relpath(target_file, pcap_file.parent)))

            # Manage Sym Links
            a_dir = self.importer.artifacts_dir
            symlink_latest = a_dir.parent.joinpath("_latest")
            symlink_current = a_dir.parent.joinpath("_current")
            if symlink_current.is_symlink():
                symlink_current.unlink()
            if symlink_latest.is_symlink():
                symlink_latest.unlink()
            if not symlink_latest.exists():
                self.importer.logger.info("Creating _latest as symlink for this directory")
                symlink_latest.symlink_to(a_dir.relative_to(symlink_latest.parent))
            a_dir.chmod(0o755)
            for file in a_dir.glob("*"):
                file.chmod(0o755)

        """
        if self.importer.config["clear_host_root_on_teardown"]:
            self.importer.logger.info("Clearing Host root...")
            try:
                shutil.rmtree(str(self.importer.host_dir_root.absolute()))
            except OSError as e:
                print(f"Error while deleting temporary host directories: {e}")
        """
        self.importer._net = None

    def get_mininet(self, topo: object = None, extensions: object = True) -> ipmininet.ipnet.IPNet:
        """
        Returns a not started Mininet based on given topology. If no topology is provided,
        the topology is built based on the configuration.
        :return The Mininet network
        """
        mininet.log.setLogLevel(self.importer.loglevel)
        if self.importer._net is not None:
            return self.importer._net
        if topo is None:
            topo = self.importer.get_topology()

        if self.importer._override_switch is not None:
            if self.importer._override_switch.lower() == "linuxbridge":
                self.importer.config["switch"] = "mininet.nodelib.LinuxBridge"
            elif self.importer._override_switch.lower() == "ovs":
                self.importer.config["switch"] = "wattson.networking.nodes.patched_ovs_switch.PatchedOVSSwitch"

        self.importer.switch_cls = wattson.util.dynamic_load_class(self.importer.config["switch"])
        self.importer.logger.info(f"Using Switch Class: {self.importer.switch_cls.__name__}")
        if "LinuxBridge" in self.importer.config["switch"]:
            subprocess.run('sysctl net.bridge.bridge-nf-call-arptables=0'.split())
            subprocess.run('sysctl net.bridge.bridge-nf-call-ip6tables=0'.split())
            subprocess.run('sysctl net.bridge.bridge-nf-call-iptables=0'.split())
        self.importer._net = self._instantiate_mininet(topo)
        self.importer.mininet_manager._add_ovs_controller(self.importer._net)
        # self.importer._build_containernet(self.importer.net, topo)
        if extensions:
            self.importer.topology_modifier.apply_prestart_extensions()
        return self.importer._net

    def _fix_router_loopback(self, net: ipmininet.ipnet.IPNet, router: dict):
        """
        Removes (default) global IP address from Loopback device on the given Router
        """
        rid = self.importer.host_manager.ghn(router)
        cmd = f"ip a s dev lo | grep \"scope global lo\""
        host = net.get(rid)
        ipline = host.cmd(cmd)
        ip = None
        for entry in ipline.split(" "):
            if "/" in entry:
                ip = entry
                break
        if ip is not None:
            self.importer.logger.debug(f"Removing IP: {ip} from router {rid}")
            host.cmd(f"ip addr del {ip} dev lo")

    def _fix_docker_routing(self, net: ipmininet.ipnet.IPNet, host: dict):
        hn = self.importer.host_manager.get_hostname(host)
        cmd = "ip -json a"
        docker = net.get(hn)
        # Bracketed paste mode in bash 5.1 fix - needed for Ubuntu 22.04 Docker images
        # JSONDecodeError if not replaced
        docker_return = docker.cmd(cmd).replace(u"\x1b[?2004l\r", u"").replace(u"\x1b[?2004h", u"")
        try:
            interfaces = json.loads(docker_return)
        except json.JSONDecodeError as jde:
            self.importer.logger.warning("JSONDecodeError:", repr(docker_return))
            interfaces = json.loads(docker_return[1:-1])

        def_iface_addr = self._get_desired_default_interface_address(host)
        default_iface = None

        for iface in interfaces:
            ifname = iface["ifname"]
            if ifname == "lo":
                continue
            if hn in ifname:
                # Valid interface, fix it if necessary
                net_v4 = None
                net_v6 = None
                a_info = iface["addr_info"]
                for info in a_info:
                    if info["family"] == "inet":
                        net_v4 = info
                    elif info["family"] == "inet6" and self.importer.config["use_v6"]:
                        net_v6 = info

                if iface["operstate"] == "DOWN":
                    print(f"Bringing up interface {ifname} on host {hn}")
                    docker.cmd(f"ip link set {ifname} up")

                if default_iface is None:
                    # Use any valid interface as long as no other default interface candidate exists
                    default_iface = ifname
                else:
                    # Default v4 Iface randomly chosen, but don't use management network if other interface exists
                    if net_v4 is not None:
                        net_v4_addr = f"{net_v4['local']}/{net_v4['prefixlen']}"
                        if def_iface_addr == net_v4_addr:
                            default_iface = ifname

            else:
                # Random Interface, delete it
                print(f"Removing Interface {ifname} from host {hn}")
                cmd = f"ip link delete {ifname}"
                docker.cmd(cmd)

        # if default_iface is not None:
        #    # Add default route
        #    print(f"Adding default route for host {hn} via interface {default_iface}")
        #    cmd = f"ip route add default dev {default_iface}"
        #    docker.cmd(cmd)
        print("Setting default route")
        docker.create_default_routes()

    def parse_bandwidth(self, bandwidth):
        """
        Converts a bandwidth given in Bps, Kbps, Mbps or Gpbs (with unit) to
        Mbps without unit for compatibility with Mininet Links
        """
        if bandwidth is None:
            return self.importer.parse_bandwidth(DEFAULT_LINK_DATARATE)

        # Regex for multiple digits followed by multiple letters (e.g., 123Mbps)
        match = re.match(r"([0-9]+)([a-z]+)", bandwidth, re.I)
        if not match:
            raise RuntimeError(f"Invalid Bandwidth: {bandwidth}")

        items = match.groups()
        assert len(items) == 2
        val = int(items[0])
        unit = items[1].lower()
        if unit not in ["bps", "kbps", "mbps", "gbps"]:
            raise RuntimeError(f"Invalid bandwidth unit: {unit}")
        scale = {
            "bps": 1000 ** 2,
            "kbps": 1000,
            "mbps": 1,
            "gbps": 1 / 1000
        }
        return val / scale[unit]

    def _setup_inter_namespace_interfaces(self):
        self.importer.tap_bridges = {}

        self.importer.logger.info("Handling Server Ports...")
        self._handling_server_ports()

        self.importer.logger.info("Deriving Client Ports...")
        self._derieving_client_ports()

        self.importer.logger.info("Inserting Tap Ports for Host Tap Interfaces (Remote Config / standalone mode)")
        self._insert_tap_ports()

        self.importer.logger.info("Adding interfaces to switches")
        self._add_interfaces_to_switch()

    def _handling_server_ports(self):
        port = self.importer.config["tap_port_start"]
        for switch in self.importer.get_switches():
            if "tap_interfaces" not in switch:
                continue
            tap_id = self.importer.config["tap_id_start"]
            for interface in switch["tap_interfaces"]:
                if interface["mode"] == "server":
                    port += 1
                    interface["port"] = port
                    self._tap_ports[f"{switch['id']}.{interface['id']}"] = port
                else:
                    continue
                if self.importer.host_manager.host_in_namespace(switch):
                    tap_dev_name = f"{self.importer.host_manager.ghn(switch)}-tap{tap_id}"
                    self.importer.logger.info(
                        f"Setting up Inter-Namespace Interface "
                        f"{self.importer.host_manager.ghn(switch)}.{interface['id']} "
                        f"as {tap_dev_name}:{port}"
                    )

                    try:
                        # Add Tap device and start TCP bridging with socat
                        cmd = f"socat -dd TUN,tun-type=tap,iff-up,tun-name={tap_dev_name} TCP-LISTEN:{port},bind=127.0.0.1,reuseaddr,keepalive,forever,ignoreeof,fork"
                        socat_proc = self._start_tcp_bridging(tap_dev_name, cmd)
                        tap_dev = {
                            "dev": tap_dev_name,
                            "mode": "server",
                            "id": tap_id,
                            "switch": switch["id"],
                            "port": port,
                            "process": socat_proc,
                            "cmd": cmd,
                            "log_iteration": 0
                        }
                        self._add_tap_device_server(tap_dev, switch)
                        tap_id += 1
                    except Exception:
                        import traceback
                        print(traceback.format_exc())
                        raise

    def _add_tap_device_server(self, tap_dev, switch):
        self.importer.tap_bridges[f"{tap_dev['dev']}"] = tap_dev
        if not network_utils.wait_for_interface(tap_dev['dev'], 5):
            self.importer.logger.error(f"Tap device {tap_dev['dev']} is not available after 5 seconds")
        # Add Interface to switch process
        self._add_interface_to_switch_process(switch, tap_dev['dev'])

    def _start_tcp_bridging(self, tap_dev_name, cmd):
        log_file = self.importer.host_dir_root.joinpath(f"socat_server_{tap_dev_name}_0.log")
        with log_file.open("w") as f:
            return subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)

    def _add_interface_to_switch_process(self, switch, tap_dev_name):
        if self.importer.switch_cls.__name__ == "LinuxBridge":
            self.importer._exec(f"brctl addif {self.importer.host_manager.ghn(switch)} {tap_dev_name}")
        else:  # Assuming OVS
            self.importer._exec(f"ovs-vsctl add-port {self.importer.host_manager.ghn(switch)} {tap_dev_name}")

    def _derieving_client_ports(self):
        if self.importer.config["inter_namespace_ssh"] is not None:
            ssh_host = self.importer.config["inter_namespace_ssh"]
            self.importer.logger.info(f"Inter namespace SSH to {ssh_host}")
            # Derive Ports preemptively and setup SSH Port forwarding
            forward_ports = self._derive_ports_preemptively()
            if len(forward_ports) > 0:
                forward_ports_str = [str(p) for p in forward_ports]
                self.importer.logger.info(f"Setting up SSH forwarding for Ports {', '.join(forward_ports_str)}")
                self._setup_ssh_port_forwarding(forward_ports, ssh_host)

    def _derive_ports_preemptively(self):
        forward_ports = []
        for switch in self.importer.get_switches():
            if "tap_interfaces" not in switch or not self.importer.host_manager.host_in_namespace(switch):
                continue
            for interface in switch["tap_interfaces"]:
                target = interface["target"]
                if target not in self._tap_ports:
                    self.importer.logger.error(f"Target interface {target} not found")
                forward_ports.append(self._tap_ports[target])
        return forward_ports

    def _setup_ssh_port_forwarding(self, forward_ports, ssh_host):
        port_cmds = [f"-L {port}:127.0.0.1:{port}" for port in forward_ports]
        cmd = f"ssh -CqN {' '.join(port_cmds)} {ssh_host}"
        # self.importer.logger.info(cmd)
        log_file = self.importer.host_dir_root.joinpath(f"inter-namespace-ssh.log")
        with log_file.open("w") as f:
            self.importer._ssh_proc = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)
        delay = 2 + len(forward_ports)
        self.importer.logger.info(f"Waiting {delay} seconds for port forwarding to be established")
        time.sleep(delay)

    def _insert_tap_ports(self):
        for host in self.importer.get_hosts():
            if "tap_interfaces" not in host:
                continue
            for interface in host["tap_interfaces"]:
                if interface["mode"] == "client":
                    target = interface["target"]
                    if target not in self._tap_ports:
                        self.importer.logger.error(f"Target interface {target} not found")
                    interface["port"] = self._tap_ports[target]

    def _add_interfaces_to_switch(self):
        for switch in self.importer.get_switches():
            if "tap_interfaces" not in switch or not self.importer.host_manager.host_in_namespace(switch):
                continue
            tap_id = self.importer.config["tap_id_start"]
            for interface in switch["tap_interfaces"]:
                if interface["mode"] == "client":
                    if interface["target"] not in self._tap_ports:
                        self.importer.logger.error(f"Target interface {interface['target']} not found")
                    interface["port"] = self._tap_ports[interface["target"]]
                    tap_dev_name = f"{self.importer.host_manager.ghn(switch)}-tap{tap_id}"

                    self.importer.logger.info(
                        f"Setting up Inter-Namespace Interface "
                        f"{self.importer.host_manager.ghn(switch)}.{interface['id']} "
                        f"as {tap_dev_name}:{interface['port']}"
                    )

                    # Add Tap device and start TCP bridging with socat
                    cmd = f"socat -dd TUN,tun-type=tap,iff-up,tun-name={tap_dev_name} TCP:127.0.0.1:{interface['port']}"
                    socat_proc = self._start_tcp_bridging(tap_dev_name, cmd)
                    tap_dev = {
                        "dev": tap_dev_name,
                        "mode": "client",
                        "id": tap_id,
                        "switch": switch["id"],
                        "port": interface["port"],
                        "process": socat_proc,
                        "cmd": cmd,
                        "log_iteration": 0
                    }
                    self._add_tap_device_client(tap_dev, switch)
                    tap_id += 1

    def _add_tap_device_client(self, tap_dev, switch):
        self.importer.tap_bridges[f"{tap_dev['dev']}"] = tap_dev
        if not network_utils.wait_for_interface(tap_dev['dev'], 5):
            self.importer.logger.error(f"Tap client device {tap_dev['dev']} is not available after 5 seconds")
        # Add Interface to switch process
        self.importer._exec(f"ovs-vsctl add-port {self.importer.host_manager.ghn(switch)} {tap_dev['dev']}")

    def _setup_physical_interfaces(self):
        nodes = self.importer.get_nodes()
        for node in nodes:
            interfaces = node.get("interfaces", [])
            for interface in interfaces:
                if interface.get("type") != "physical":
                    continue
                physical_interface = interface.get("physical")
                node_type = node.get("type")
                self.importer.logger.info(f"Setting up physical interface {physical_interface} for {node_type} "
                                          f"{self.importer.ghn(node)}")
                if node_type == "switch":
                    mn_switch = self.importer.get_mininet_node(node)
                    if not isinstance(mn_switch, mininet.node.OVSSwitch):
                        self.importer.logger.warning(
                            f"Cannot setup Interface {physical_interface} as Switch Port - OVS Switch required"
                        )
                        continue
                    bridge = mn_switch.deployed_name
                    cmd = " ".join([
                        f"ovs-vsctl add-port {bridge} {physical_interface}"
                    ])
                    code, out = self.importer.exec_with_output(cmd, True)
                    if interface.get("mirror", False):
                        self.importer.logger.info(f"Setting up mirror port on physical interface {physical_interface}")
                        cmd = " ".join([
                            "ovs-vsctl",
                            f"-- --id=@p get port {physical_interface}",
                            f"-- --id=@m create mirror name=phys-{physical_interface} select-all=true output-port=@p",
                            f"-- set bridge {bridge} mirrors=@m",
                        ])
                        code, out = self.importer.exec_with_output(cmd, True)
                else:
                    self.importer.logger.warning("Physical Interfaces for Hosts not implemented yet")
                    pass


    def _setup_switch_mirror_ports(self):
        links = self.importer.get_links()
        for link in links:
            l_id = link["id"]
            info_l = link["interfaces"][0]
            info_r = link["interfaces"][1]
            iface_l: str
            iface_r: str
            host_l, iface_l = self.importer.utils.parse_interface(info_l)
            host_r, iface_r = self.importer.utils.parse_interface(info_r)
            if iface_l.startswith("mirror") and self.importer.is_switch(host_l):
                self._setup_as_mirror_port(l_id, host_l, iface_l, host_r, iface_r)
            elif iface_r.startswith("mirror") and self.importer.is_switch(host_r):
                self._setup_as_mirror_port(l_id, host_r, iface_r, host_l, iface_l)

    def _setup_as_mirror_port(self, link_id, switch_id, interface_id, other_node, other_interface):
        if not self.importer.host_manager.host_in_namespace(switch_id):
            self.importer.logger.info(f"Skipping {switch_id}.{interface_id} since the switch is not in this namespace")
            return False
        switch = self.importer.get_node(switch_id)
        mn_switch = self.importer.get_mininet_node(switch_id)
        if not isinstance(mn_switch, mininet.node.OVSSwitch):
            self.importer.logger.warning(
                f"Cannot setup Interface {switch_id}.{interface_id} as Mirror Port - OVS Switch required"
            )
            return False
        link = self.importer.link_manager.get_net_link(link_id=link_id)
        iface1 = link.intf1
        iface2 = link.intf2
        switch_name = self.importer.ghn(switch_id)
        iface = iface1 if iface1.node.name == switch_name else iface2
        if iface.IP():
            self.importer.logger.warning(
                f"Interface {switch_id}.{interface_id} has configured IP - cannot convert to mirror"
            )
            return False
        iface_name = iface.name
        bridge = mn_switch.deployed_name
        self.importer.logger.info(f"Setting up {switch_id}.{interface_id} ({iface_name}@{bridge}) as Mirror Port...")
        cmd = " ".join([
            f"ovs-vsctl",
            f"-- --id=@p get port {iface_name}",
            f"-- --id=@m create mirror name={interface_id} select-all=true output-port=@p",
            f"-- set bridge {bridge} mirrors=@m",
        ])
        code, out = self.importer.exec_with_output(cmd, True)
        if code != 0:
            return False
        return True

    def _get_desired_default_interface_address(self, host: Dict) -> Optional[Dict]:
        if "interfaces" not in host:
            return None
        def_iface = None
        interfaces = host["interfaces"]
        for iface in interfaces:
            if "ip" in iface:
                if def_iface is None:
                    def_iface = iface["ip"]
                elif "default" in iface and iface["default"]:
                    def_iface = iface
        return def_iface
