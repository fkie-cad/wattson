import argparse
import json
import importlib
import os
from pathlib import Path

"""
This Module is intended to deploy the logic of already started Mininet hosts.
In particular, it is executed as a process on the Mininet Host and instantiates
the desired Python Object, which implements the logic.
This module also forwards the configuration to the implementing Object.
"""


def main():
    parser = argparse.ArgumentParser("Cosimulation Python Deployment Helper")
    parser.add_argument("id", type=str, help="An arbitrary ID to allow easier identification")
    parser.add_argument("config", type=str, help="The JSON-config file location")
    args = parser.parse_args()

    with Path(args.config).open("r") as f:
        deploy_config = json.load(f)

    process_config = {}
    if "config" in deploy_config:
        process_config = deploy_config["config"]
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
