import json
import os
import argparse
import shlex
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import psutil

from wattson.topology import network_utils
from wattson.util.misc import get_console_and_shell
from .constants import SYSTEM_NAME, DEFAULT_NAMESPACE
from wattson.util.readable_dir import ReadableDir
from ..util import get_logger


def wattson_exec(cmd, logger, stdout=None, stderr=None) -> bool:
    if stdout is None:
        stdout = subprocess.PIPE
    if stderr is None:
        stderr = subprocess.STDOUT
    p = subprocess.Popen(shlex.split(cmd), stdout=stdout, stderr=stderr)
    output, error = p.communicate()
    for line in output.splitlines():
        logger.info(line)
    return p.returncode == 0


def namespace_exec(namespace, cmd, logger, stdout=None, stderr=None) -> bool:
    cmd = f"ip netns exec {namespace} {cmd}"
    return wattson_exec(cmd, logger, stdout, stderr)


def clear_cache(workdir: Path, logger):
    logger.info("Cleaning working directory")
    shutil.rmtree(str(workdir.absolute()))


def main():
    """
    This is the entry point for an external host deployment of a single host for namespace separation
    """
    parser = argparse.ArgumentParser(f"{SYSTEM_NAME} standalone host deployment (Requires root privileges)")
    parser.add_argument("host", type=str, help="The host's ID to deploy")
    parser.add_argument("ssh", type=str, nargs="?",
                        help="[Optional] SSH Server and Login information that is used to setup port forwarding  "
                             "for inter-namespace-links. Make sure that SSH access works for (local) root / sudo."
                             "Also required for obtaining deployment information if configured."
                             "If not present, a local setup is assumed")
    parser.add_argument("--staticconf", "--static-conf", type=str, default=None,
                        help="[Optional] A path to the host deployment information. If not present, the config is"
                             "loaded from the SSH remote host or from /tmp (for non-SSH deployments)")
    parser.add_argument("--work-dir", "-d", type=str, default="wattson_tmp/@host")
    parser.add_argument("--wait", "-w", type=int, default=5,
                        help="How long (in seconds) to wait for interface operations")
    parser.add_argument("--cache", "-c", action="store_true",
                        help="[Optional] Set to load and keep local config for faster loading")
    parser.add_argument("--clear-cache", "-cc", action="store_true",
                        help="[Optional] Set to clear local config")
    parser.add_argument("--port", "-p", type=int, default=None, help="[Optional] SSH Port override")
    parser.add_argument("--password", "-pw", type=str, default=None,
                        help="[Optional] SSH Password override - requries sshpass")
    parser.add_argument("--scenario", "-s", type=str, action=ReadableDir, default=None,
                        help="[Optional] The path to the scenario to use for locally generating the config")
    parser.add_argument("--prep", action="append", default=[],
                        help="List of parameters for the scenario preparation script as 'name:value'")
    parser.add_argument("--interface-stats", "-ifs", action="store_true",
                        help="Set to show network throughput in a dedicated terminal")
    parser.add_argument("--pcap", action="store_true",
                        help="Set to record PCAPs for the tap interfaces")
    parser.add_argument("--terminal", "-t", action="store_true",
                        help="Set to open a host terminal")

    if os.geteuid() != 0:
        print("Root privileges required for namespace creation")
        return

    args = parser.parse_args()
    host_id = args.host
    ssh = args.ssh
    local_conf = args.staticconf
    ssh_port = f"-P {args.port}" if args.port is not None else ""

    logger = get_logger("Wattson Standalone", "Wattson Standalone", use_context_logger=False)
    logger.info(f"Starting Standalone Deployment for Host {host_id}")
    config = {}
    deploy_config = {}

    work_dir_name = args.work_dir.replace("@host", host_id)

    workdir = Path(work_dir_name)
    workdir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Working directory is {str(workdir)}")

    helper_processes = {}
    existing_host_deploy = False

    if args.clear_cache:
        clear_cache(workdir, logger)
        workdir.mkdir(parents=True, exist_ok=True)

    sshpass = ""
    if args.password is not None:
        sshpass = f"sshpass -p {args.password}"

    if args.scenario:
        from wattson.util.compat import fix_iptc
        fix_iptc()
        print("Locally generating host config and writing to cache")
        from wattson.topology.network_manager import NetworkManager
        manager = NetworkManager(Path(args.scenario), preparation=args.prep, loglevel="warning",
                                 config={"persistent_logs": False})
        manager.get_powernet()
        net = manager.mininet_manager.get_mininet()
        manager.deployment.deploy_host(host_id, tmp_path=workdir, inplace_config_file=True)
        manager.mininet_manager.shutdown_mininet(net)
        args.cache = True

    if ssh:
        target_file = workdir.joinpath(f'wattson_standalone_{host_id}')
        if not args.cache or not target_file.exists():
            logger.info(f"Copying config from SSH host {ssh} to {target_file.absolute()}")
            path = Path(f"/tmp/wattson_standalone_{host_id}")
            cmd = f"{sshpass} scp {ssh_port} {ssh}:{str(path.absolute())} {target_file}"
            wattson_exec(cmd, logger)
        else:
            logger.info(f"Searching for cached config")

        if not target_file.exists():
            logger.error("Could not load config from remote host")
            if not args.cache:
                shutil.rmtree(workdir)
            sys.exit(1)

        with target_file.open("r") as f:
            config = json.load(f)

        deploy_config_path = config["deploy_config"]
        if not deploy_config_path:
            logger.warning("No deploy config provided!")
        else:
            existing_host_deploy = True
            deploy_config = Path(deploy_config_path)
            deploy_target_file = workdir.joinpath(deploy_config.name)

            if not args.cache or not deploy_target_file.exists():
                logger.info(f"Copying deploy_config from SSH host to {deploy_target_file.absolute()}")
                cmd = f"{sshpass} scp {ssh_port} {ssh}:{str(deploy_config.absolute())} {str(deploy_target_file.absolute())}"
                wattson_exec(cmd, logger)
            else:
                logger.info(f"Using local deploy_config from cache in {deploy_target_file.absolute()}")

            if not deploy_target_file.exists():
                logger.error("Could not copy deploy configuration")
                if not args.cache:
                    shutil.rmtree(workdir)
                sys.exit(1)

            deploy_config = deploy_target_file

        ports = []
        host = config["host"]
        if "tap_interfaces" in host:
            for interface in host["tap_interfaces"]:
                ports.append(interface["port"])

        if len(ports) > 0:
            ports_str = [str(port) for port in ports]
            logger.info(f"Setting up SSH Port Forwarding for ports {', '.join(ports_str)}")
            port_cmds = [f"-L {port}:127.0.0.1:{port}" for port in ports]
            cmd = f"{sshpass} ssh -CqN {' '.join(port_cmds)} {ssh}  {ssh_port.lower()}"
            log_file = workdir.joinpath("ssh_port_forwards.log")
            with log_file.open("w") as f:
                p = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)
                helper_processes["ssh_forwarding"] = p
                time.sleep(1+len(ports))
    else:
        if local_conf:
            path = Path(local_conf)
        else:
            path = Path(f"/tmp/wattson_standalone_{host_id}")
        logger.info(f"Looking for local configuration in {path.absolute()}")
        if not path.exists():
            logger.error("Local config not found")
            sys.exit(1)
        with path.open("r") as f:
            config = json.load(f)
            deploy_config_path = config["deploy_config"]
            if not deploy_config_path:
                logger.warning("No deploy config provided!")
            else:
                existing_host_deploy = True
                deploy_config = Path(deploy_config_path)
                if deploy_config.exists():
                    filename = f"deploy_config_{host_id}"
                    logger.info(f"Copying deploy config to {filename}")
                    shutil.copy(deploy_config.absolute(), workdir.joinpath(filename))
                    deploy_config = workdir.joinpath(filename)
                else:
                    logger.error("Deploy configuration file not found")
                    sys.exit(1)

    host = config["host"]
    interfaces = []
    if "tap_interfaces" not in host or len(host["tap_interfaces"]) == 0:
        logger.warning("No inter-namespace interfaces defined - no networking possible!")
    else:
        interfaces = host["tap_interfaces"]
        logger.debug(interfaces)

    namespace = f"wattson_{host_id}"
    nscmd = f"ip netns exec {namespace}"

    host_proc: Optional[subprocess.Popen] = None

    try:
        if existing_host_deploy:
            # Copy deploy config to working directory
            shutil.copy(deploy_config, workdir.joinpath("deploy_config"))

        # Create new networking namespace
        logger.info(f"Creating networking namespace {namespace}")
        if not wattson_exec(f"ip netns add {namespace}", logger):
            raise RuntimeError("Could not create networking namespace")
        logger.info("Setting loopback interface up")
        if not wattson_exec(f"ip netns exec {namespace} ip link set dev lo up", logger):
            logger.error("Could not bring up loopback interface")

        for tap_id, interface in enumerate(interfaces):
            # Create socat forwarding
            name = f"w{host_id}-tap{tap_id}"
            logger.info(f"Creating tap interface {name}")
            if "port" not in interface:
                logger.error(f"interface {name} has no TCP port specified")
                continue
            port = interface["port"]
            logger.info(f"Connecting Interface: 127.0.0.1:{port}")
            cmd = f"socat -dddd TUN,tun-type=tap,iff-up,tun-name={name} TCP:127.0.0.1:{port}"
            log_file = workdir.joinpath(f"socat_client_{name}.log")
            with log_file.open("w") as f:
                p = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)
                helper_processes[f"socat {name}"] = p
                # Wait for socat interface to be ready
                if not network_utils.wait_for_interface(name, args.wait):
                    logger.error(f"Interface {name} not ready after {args.wait} seconds")

            # Add Interface to namespace
            logger.info(f"Moving {name} to networking namespace")
            wattson_exec(f"ip link set {name} netns {namespace}", logger)
            if not network_utils.wait_for_interface(name, args.wait, namespace=namespace):
                logger.error(f"Interface {name} not found in namespace {namespace} after {args.wait} seconds")
                raise RuntimeError(f"Interface {name} not found in namespace after {args.wait} seconds")

            if "ip" in interface:
                # Add IP to interface
                ip = interface["ip"]
                logger.info(f"Adding IP {ip} to {name} and bringing interface up")
                namespace_exec(namespace, f"ip addr add {ip} dev {name}", logger)
                namespace_exec(namespace, f"ip link set dev {name} up", logger)

            if args.pcap:
                # Start TShark for interface
                pcap_file = workdir.joinpath(f"{name}.pcap")
                pcap_log_file = workdir.joinpath(f"{name}-tshark.log")
                with pcap_log_file.open("w") as f:
                    logger.info(f"Starting PCAP at interface {name} and writing to {str(pcap_file)}")
                    cmd = f"tshark -ni {name} -w {pcap_file.absolute().__str__()}"
                    cmd = f"{nscmd} {cmd}"
                    pcap_proc = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)
                    helper_processes[f"pcap-{name}"] = pcap_proc

        if args.interface_stats:
            # Open network statistic terminal
            console, shell = get_console_and_shell(os.getpid())
            cmd = f"ip netns exec {namespace} {console} -e iftop"
            stat_proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            helper_processes["ip-stat"] = stat_proc

        # Open a terminal?
        if args.terminal or not existing_host_deploy:
            console, shell = get_console_and_shell(os.getpid())
            cmd = f"ip netns exec {namespace} {console} -e {shell}"
            terminal_process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not existing_host_deploy:
                logger.info("Opening terminal as main host process")
                host_proc = terminal_process
            else:
                helper_processes["terminal"] = terminal_process

        if existing_host_deploy:
            # Run process in new networking namespace
            logger.info(f"Starting Host {host_id}")
            log_file = workdir.joinpath(f"wattson_host_{host_id}.log")
            with log_file.open("w") as f:
                cmd = f"{nscmd} {sys.executable} -m wattson.deployment {host_id} {deploy_config.absolute()}"
                host_proc = subprocess.Popen(shlex.split(cmd), stdout=f, stderr=f)

        # Wait for termination of host process
        host_proc.wait()
    except KeyboardInterrupt:
        print("\r", end="")
        logger.info("Keyboard Interrupt - Shutting Down")
        pass
    except RuntimeError as e:
        logger.error(f"{e=}")
    finally:
        if host_proc is not None and host_proc.poll() is None:
            logger.info("Terminating host processes")
            process = psutil.Process(host_proc.pid)
            for proc in process.children(recursive=True):
                proc.kill()
            host_proc.terminate()
            try:
                logger.info("Waiting for up to 5 seconds")
                host_proc.wait(5)
            except subprocess.TimeoutExpired:
                logger.info("Process did not terminate, killing it...")
                host_proc.kill()

        # Shutdown socat forwarding processes
        for name, p in helper_processes.items():
            logger.info(f"Stopping networking sub process {name}")
            p.terminate()
            p.wait()

        # Delete namespace
        logger.info(f"Cleaning up networking namespace {namespace}")
        wattson_exec(f"ip netns delete wattson_{host_id}", logger)

        if not args.cache:
            clear_cache(workdir, logger)


if __name__ == '__main__':
    main()
