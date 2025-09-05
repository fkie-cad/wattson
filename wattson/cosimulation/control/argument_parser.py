import argparse
from pathlib import Path

from wattson.cosimulation.simulators.network.constants import DEFAULT_SEGMENT


def get_argument_parser() -> argparse.ArgumentParser:
    """
    Builds an argparse.ArgumentParser with arguments to be used by the co-simulation controller :return: The configured ArgumentParser

    """
    parser = argparse.ArgumentParser("Wattson Co-Simulation Control")
    parser.add_argument("scenario", type=Path,
                        help="The directory with the scenario configuration to run")
    parser.add_argument("segment", type=str, default=DEFAULT_SEGMENT, nargs='?',
                        help="[Optional] Co-Simulation segment to start on this host")
    parser.add_argument("--extensions", "-e", type=str, default="extensions.yml",
                        help="The file to look into for extensions. Defaults to extensions.yml")

    # Artifact Directory
    parser.add_argument("--artifact-directory", "--working-directory", "-d", type=str, default=None,
                        help="The folder to use for artifacts for the simulation run. If not given, an automatic hierarchy is created")
    parser.add_argument("--physical-export", action="store_true", help="Set to enable exports for the physical simulator")
    parser.add_argument("--ccx-export", type=str, default=None, help="Provide a file name (jsonl) to enable notification export in the CCX")

    # Time
    parser.add_argument("--wall-clock-reference", type=float, default=None,
                        help="Timestamp to use as wall clock reference. Defaults to current timestamp")
    parser.add_argument("--sim-clock-reference", type=float, default=None,
                        help="Timestamp to synchronize to the wall clock reference. Defaults to current timestamp")
    parser.add_argument("--clock-speed", type=float, default=1,
                        help="Speed of the simulated clock. Defaults to 1.")

    # Dynamic Scenarios
    parser.add_argument("--update-scenario", "-u", action="store_true",
                        help="Set to force a scenario update for external dependencies")
    parser.add_argument("--prep", "-p", action="append", default=[],
                        help="List of parameters for the scenario preparation script as 'name:value'")

    # Log Level
    parser.add_argument("--loglevel", action="store", default="info", help="Set the Mininet Log Level (default: info)")

    # Reproducibility / random seeds
    parser.add_argument("--random", "--seed", "-rs", type=str, default=None,
                        help="The random seed to use. Overwrites the setting from extensions!")
    parser.add_argument("--seed-file", "-sf", type=str, default=None,
                        help="The file to reed the random seed from. Use file.txt@LINE to specify a line")

    # Network Deployment
    parser.add_argument("--start-delay", type=float,
                        help="Delay (seconds, float) between network start and host deployment", default=0)
    parser.add_argument("--deploy-wait", type=float, help="Delay (seconds, float) between each host deployment",
                        default=0.0)
    parser.add_argument("--cpu-wait-timeout", "-cw", type=float, default=10,
                        help="Delay (seconds, float) to wait for the CPU threshold")
    parser.add_argument("--cpu-limit", "-cpu", type=float, default=70,
                        help="CPU load percentage to be deceeded before host deployment")
    parser.add_argument("--synchronous-start", "--sync", action="store_true",
                        help="Set to disable the asynchronous start (increases stability, reduces performance)")

    # Network Options
    parser.add_argument("--pcap", action="append", default=[], help="Set one or multiple hosts to start pcap recording")
    parser.add_argument("--empty-net", action="store_true", help="Set to skip loading the communication network (Will still create the management network)")
    parser.add_argument("--no-net", action="store_true", help="Set to skip network creation")
    parser.add_argument("--no-link-properties", action="store_true", help="Set to disable network link properties (bandwidth, delay, etc.)")

    # Utils
    parser.add_argument("--clean", "--clean", "-c", action="store_true", help="Set to only clean up any potential running instances")
    parser.add_argument("--no-cli", action="store_true", help="Disable Wattson's default CLI")
    parser.add_argument("--export-notification-topic", "-en", action="append", default=[], help="Notification topics to mark for export")
    parser.add_argument("--notification-topic-history", "-nh", action="append", default=[], help="Notification topics to preserve history for")
    parser.add_argument("--vcc-proxy", "--vcc", "--ccx", action="store_true", help="Set to enable VCC proxy mode if applicable")
    parser.add_argument("--vcc-export", nargs="*", type=str, choices=["measurement", "estimation", "e", "m"],
                        help="Instruct the VCC (not the CCX!) to export measurements and/or estimation values")

    # Configuration Options
    parser.add_argument("--option", "-o", action="append", default=[], nargs=2,
                        help="Configuration options to set. These options can be overwritten by extensions. "
                             "Keys are separated by dots. Values are passed in JSON notation. E.g.: -o power-grid.enable true")
    return parser
