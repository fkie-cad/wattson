import argparse
import codecs
import json
import pickle
import sys
from pathlib import Path

from wattson.util.np_encoder import NpEncoder


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, help="The config file to parse")
    parser.add_argument("--datapoints", "-d", action="store_true", help="Whether to show data points")
    parser.add_argument("--power-grid", "-g", action="store_true", help="Whether to show the power grid")
    args = parser.parse_args()
    config_file = Path(args.config)
    if not config_file.exists() or not config_file.is_file():
        print("Not a valid file")
        sys.exit(1)

    with config_file.open("r") as f:
        config = json.load(f)
    deployment_config = config.get("config", None)
    config["config"] = {}
    if isinstance(deployment_config, str):
        deployment_config = pickle.loads(codecs.decode(deployment_config.encode(), "base64"))
    if isinstance(deployment_config, dict):
        for key, value in deployment_config.items():
            if key in ["power_grid", "network"]:
                config["config"][key] = "@hidden"
            elif key == "datapoints":
                if args.datapoints:
                    config["config"][key] = value
                else:
                    config["config"][key] = f"@{len(value)} data points"
            else:
                config["config"][key] = value
    else:
        config["config"] = "FAILED TO LOAD"
    print(json.dumps(config, indent=4, cls=NpEncoder))


if __name__ == '__main__':
    main()
