import argparse
import sys
from .host_cli import HostCLI

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Wattson Host CLI")
    parser.add_argument("host", type=str, help="Host IP to connect to")
    parser.add_argument("--name", type=str, help="Host name to use in the prompt", default=None)
    parser.add_argument("--port", type=int, help="The server's IP address", default=61195)
    args = parser.parse_args()
    cli = HostCLI(ip=args.host, port=args.port, name=args.name)
    sys.exit(0)
