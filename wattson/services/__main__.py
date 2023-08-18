import argparse
import json
import sys
import time
from pathlib import Path
from typing import Type

from wattson.services.deployment import PythonDeployment
from wattson.util.misc import dynamic_get_classes_from_file


def main():
    parser = argparse.ArgumentParser("WattsonService standalone deployment")
    parser.add_argument("service", help="The file implementing the service, i.e., the PythonDeployment")
    parser.add_argument("--config-file", "-f", type=str, help="A JSON config file to define the service's arguments", default=None)
    parser.add_argument("--config-argument", "-c", type=str, nargs="*",
                        help="Individual configuration options. Pass as -c 'key:value'", default=[])

    args = parser.parse_args()

    service_file = Path(args.service)
    if not service_file.exists() or not service_file.is_file():
        print("Invalid service file")
        sys.exit(1)

    service_classes = dynamic_get_classes_from_file(service_file)
    if len(service_classes) == 1:
        service_class: Type[PythonDeployment] = service_classes[0]
    else:
        print(f"Cannot extract service class. Found {len(service_classes)} candidates")
        sys.exit(1)

    config = {}

    if args.config_file is not None:
        config_file = Path(args.config_file)
        if not config_file.exists() or not config_file.is_file():
            print("Invalid config file")
            sys.exit(1)
        with config_file.open("r") as f:
            config = json.load(f)

    for option in args.config_argument:
        key, value = option.split(":", 1)
        config[key] = value

    service = service_class(configuration=config)
    service.start()
    sys.exit(0)


if __name__ == '__main__':
    main()
