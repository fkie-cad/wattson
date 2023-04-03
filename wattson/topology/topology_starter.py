import datetime
import os
import argparse
import signal
import sys
import time

from pathlib import Path

from .network_manager import NetworkManager
from .constants import SYSTEM_NAME, CLI_HOST, DEFAULT_NAMESPACE
from wattson.util.readable_dir import ReadableDir


def parse_args():
    parser = argparse.ArgumentParser(f"{SYSTEM_NAME} topology deployment")
    parser.add_argument("scenario", type=str, action=ReadableDir,
                        help="The directory holding the topology and power grid information")
    parser.add_argument("namespace", type=str, default=DEFAULT_NAMESPACE, nargs='?',
                        help="[Optional] Namespace to start on this host")
    parser.add_argument("--extensions", "-e", type=str, default="extensions.yml",
                        help="The file to look into for extensions. Defaults to extensions.yml")
    parser.add_argument("--random", "--seed", "-rs", type=str, default=None,
                        help="The random seed to use. Overwrites the setting from extensions!")
    parser.add_argument("--seedfile", "-sf", type=str, default=None,
                        help="The file to reed the random seed from. Use file.txt@LINE to specify a line")
    parser.add_argument("--gui", action="store_true", help="Set to enable logging to XTerms for each deployed host")
    parser.add_argument("--pcap", action="append", default=[], help="Set one or multiple hosts to start pcap recording")
    parser.add_argument("--sshne", "--ssh-namespace-endpoint", default=None, type=str,
                        help="[Optional] SSH Server and Login information that is used to setup port forwarding "
                             "for inter-namespace-links. Make sure that SSH access works for (local) root / sudo")
    parser.add_argument("--nc", "--no-controller", action="store_true", help="Set to disable the controller")
    parser.add_argument("--switch", default=None, type=str, choices=["LinuxBridge", "OVS"],
                        help="Override the switch implementation")
    parser.add_argument("--link", default=None, type=str, choices=["default", "tclink"],
                        help="Override the link implementation")
    parser.add_argument("--disable-periodic-updates", "-dpu", action="store_true",
                        help="Set to disable periodic updates")
    parser.add_argument("--startdelay", "-d", type=float,
                        help="Delay (seconds, float) between network start and host deployment", default=0)
    parser.add_argument("--deploywait", "-w", type=float, help="Delay (seconds, float) between each host deployment",
                        default=0.0)
    parser.add_argument("--cpu-wait-timeout", "-cw", type=float, default=10,
                        help="Delay (seconds, float) to wait for the CPU threshold")
    parser.add_argument("--cpu-limit", "-cpu", type=float, default=70,
                        help="CPU load percentage to be deceeded before host deployment")
    parser.add_argument("--loglevel", action="store", default="info", help="Set the Mininet Log Level (default: info)")
    parser.add_argument("--updatescenario", "-u", action="store_true",
                        help="Set to force a scenario update for external dependencies")
    parser.add_argument("--prep", "-p", action="append", default=[],
                        help="List of parameters for the scenario preparation script as 'name:value'")

    return parser.parse_args()


def get_further_args(args):
    further_args = {}
    if args.nc:
        further_args["controller"] = None
    if args.sshne is not None:
        further_args["inter_namespace_ssh"] = args.sshne
    return further_args


def get_pcaps(args):
    pcaps = []
    for pcap in args.pcap:
        pcaps.extend(pcap.split(","))
    return [pcap.strip() for pcap in pcaps]


def get_random_seed(args):
    random_seed = None
    if args.random is not None:
        random_seed = args.random
    elif args.seedfile is not None:
        sf: str = args.seedfile
        if "@" in sf:
            filename, line = sf.split("@")
            line = int(line)
        else:
            filename = sf
            line = 0
        with Path(filename).open("r") as f:
            lines = f.readlines()
            random_seed = lines[line]
    return random_seed


def get_network_manager(args, folder):
    if hasattr(args, "importer"):
        return args.importer
    return NetworkManager(
        folder,
        gui=args.gui,
        loglevel=args.loglevel,
        force_scenario_update=args.updatescenario,
        namespace=args.namespace,
        config=get_further_args(args),
        preparation=args.prep,
        extensions=args.extensions,
        random_seed=get_random_seed(args),
        switch=args.switch,
        link=args.link,
        pcap=get_pcaps(args),
        disable_periodic_updates=args.disable_periodic_updates,
        attach_to_coordinator=True
    )


def main(args=None):
    """
    This is the entry point for a full deployment of a predefined topology
    """
    if not args:
        args = parse_args()
    folder = Path(args.scenario)

    print(f"Running Simulation main process with PID {os.getpid()} in namespace {args.namespace}")

    original_handlers = {
        "sigint": signal.getsignal(signal.SIGINT),
        "sigterm": signal.getsignal(signal.SIGTERM)
    }
    
    restart = True
    while restart:
        def teardown(num=None, frame=None, force=False):
            if importer.is_running() and (not importer.cli.is_blocked() or force):
                importer.cli.kill()
                importer.disconnect_coordinator()
                importer.deployment.teardown()
                importer.mininet_manager.shutdown_mininet()
                importer.teardown()
                if not restart:
                    signal.signal(signal.SIGINT, original_handlers["sigint"])
                    signal.signal(signal.SIGTERM, original_handlers["sigterm"])
                    sys.exit(0)
            elif importer.cli.is_blocked():
                importer.cli.clear_input_on_interrupt()
            else:
                print("Teardown Skipped")

        def shutdown_requested(num=None, frame=None):
            print(f"Shutdown requested via SIGUSR1")
            teardown(force=True)

        start_time = time.time()

        importer = get_network_manager(args, folder)
        signal.signal(signal.SIGINT, teardown)
        signal.signal(signal.SIGTERM, teardown)
        signal.signal(signal.SIGUSR1, shutdown_requested)

        try:
            importer.get_topology()

            if importer.namespace == DEFAULT_NAMESPACE:
                # Add Coordinator and CLI Server
                importer.addHost("coord", switch=importer.get_main_management_switch(),  deploy={
                    "type": "python",
                    "module": "wattson.powergrid.server.deployment",
                    "class": "DefaultCoordinator",
                    "config": {
                        "data-folder": str(folder.absolute()),
                        "hostid": "!nodeid",
                        "hostname": "!hostname",
                        "ip": "!mgmip",
                        "rtu_ids": "!node_ids.rtus_with_pandapower",
                        "mtu_ids": "!node_ids.mtu",
                        "mtu_coas": "!coas.mtu",
                        "rtu_coas": "!coas.rtu",
                        "datapoints": "!datapoints",
                        "powernet": "!raw_powernet",
                        "artifacts_dir": "!globals.artifacts_path",
                        "scenario_path": "!scenario_path",
                        "config": "!globals.coord_config",
                        "main_pid": os.getpid(),
                        "statistics": "!globals.statistics",
                        "profile_loader_exists": "!profile_loader_exists"
                    }
                }, priority=1)
                importer.addHost(CLI_HOST, switch=importer.get_main_management_switch())

            importer.mininet_manager.start_mininet()
            importer.create_coordinator_client()
            importer.logger.info(f"Waiting for CPU limit ({args.cpu_limit}% to be deceeded "
                                 f"(up to {args.cpu_wait_timeout}s)")
            importer.wait_for_cpu(cpu_limit=args.cpu_limit, timeout=args.cpu_wait_timeout, log=True)
            importer.logger.info(f"Waiting for {args.startdelay} seconds before starting hosts...")
            time.sleep(args.startdelay)
            importer.start_coordinator_client()
            importer.logger.info(f"Starting hosts with delay {args.deploywait}")
            importer.deploy_hosts(wait=args.deploywait)
            end_time = time.time()
            startup_time = datetime.timedelta(seconds=end_time-start_time)
            importer.logger.info(f"Startup Time: {str(startup_time)}")
            coord_pids = importer.deployment.host_get_pids("coord")
            if len(coord_pids) == 1:
                coord_pid = coord_pids[0]
                if coord_pid is None:
                    importer.logger.error(f"Coordinator not running - potentially crashed")
                    break
                importer.logger.info(f"Notifying coordinator (PID {coord_pid}) of successful deployment")
                os.kill(coord_pid, signal.SIGUSR1)
            else:
                importer.logger.warning("Could not get PID of coordinator")
            mtus = [mtu["id"] for mtu in importer.get_mtus()]
            if len(mtus) > 0:
                importer.logger.info(f"The following MTUs exist: {', '.join(mtus)}")
            else:
                importer.logger.warning("No MTUs exist in the simulation")
            if not hasattr(args, "importer"):
                importer.cli.run()
        except Exception as e:
            print(f"{e=}")
            raise e
        finally:
            restart = importer.restart_requested()
            teardown()


def external_teardown(importer, restart=False, num=None, frame=None, force=False):
    original_handlers = {
        "sigint": signal.getsignal(signal.SIGINT),
        "sigterm": signal.getsignal(signal.SIGTERM)
    }
    if importer.is_running() and (not importer.cli.is_blocked() or force):
        importer.cli.kill()
        importer.disconnect_coordinator()
        importer.deployment.teardown()
        importer.mininet_manager.shutdown_mininet()
        importer.teardown()
        if not restart:
            signal.signal(signal.SIGINT, original_handlers["sigint"])
            signal.signal(signal.SIGTERM, original_handlers["sigterm"])
            return
    elif importer.cli.is_blocked():
        importer.cli.clear_input_on_interrupt()
