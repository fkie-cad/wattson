import argparse
import sys

from wattson.cosimulation.control.constants import SIM_CONTROL_ID, SIM_CONTROL_PUBLISH_PORT, SIM_CONTROL_PORT
from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.networking.namespaces.namespace import Namespace


def main():
    parser = argparse.ArgumentParser("Wattson standalone CLI")
    args = parser.parse_args()

    namespace = Namespace(f"w_{SIM_CONTROL_ID}")
    if not namespace.exists():
        print(f"No running Wattson instance found: Namespace {namespace.name} does not exist")
        sys.exit(1)

    server_ip = "127.0.0.1"
    query_server_socket = f"tcp://{server_ip}:{SIM_CONTROL_PORT}"
    publish_server_socket = f"tcp://{server_ip}:{SIM_CONTROL_PUBLISH_PORT}"
    simulation_control_client = WattsonClient(query_server_socket_string=query_server_socket,
                                              publish_server_socket_string=publish_server_socket,
                                              namespace=namespace)
    simulation_control_client.start()


if __name__ == '__main__':
    main()
