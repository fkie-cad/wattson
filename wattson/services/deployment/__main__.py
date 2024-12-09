import argparse
import codecs
import json
import importlib
import os
import pickle
import sys
from pathlib import Path

from wattson.util.json.pickle_decoder import PickleDecoder

"""
This Module is intended to deploy the logic of already started Mininet hosts.
In particular, it is executed as a process on the Mininet Host and instantiates
the desired Python Object, which implements the logic.
This module also forwards the configuration to the implementing Object.
"""


def _create_restart_script(service_name, service_id, service_config_file):
    pid = os.getpid()
    workdir = Path(".")
    restart_script = workdir.joinpath(f"restart_{service_name}.sh")
    log_file = workdir.joinpath(f"{service_name}.log")
    with restart_script.open("w") as f:
        f.write(
            f"{sys.executable} -m wattson.services.deployment.restart {pid} {service_id} {service_config_file} {str(log_file.absolute())}"
        )
    restart_script.chmod(0o777)


def main():
    parser = argparse.ArgumentParser("Cosimulation Python Deployment Helper")
    parser.add_argument("id", type=str, help="An arbitrary ID to allow easier identification")
    parser.add_argument("config", type=str, help="The JSON-config file location")
    args = parser.parse_args()

    with Path(args.config).open("r") as f:
        deploy_config = json.load(f, cls=PickleDecoder)

    process_config = {}
    if "config" in deploy_config:
        process_config = deploy_config["config"]

    if deploy_config.get("create_restart_script", True):
        service_name = process_config.get("name", deploy_config["class"]).replace(" ", "-")
        _create_restart_script(service_name, args.id, args.config)

    try:
        module = importlib.import_module(deploy_config["module"])
    except Exception as e:
        raise RuntimeError(f"Cannot Import Deployment Module {deploy_config['module']}: {e}")

    ocls = getattr(module, deploy_config["class"])
    print(f"Instantiating {deploy_config['class']} (PID {os.getpid()})")
    o = ocls(process_config)
    print(f"Starting {deploy_config['class']}")
    o.start()
    print(f"Stopped {deploy_config['class']}")


if __name__ == '__main__':
    main()
