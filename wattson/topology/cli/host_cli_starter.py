import logging
import sys

from ipmininet.cli import IPCLI
import mininet.log
from wattson.topology.constants import CLI_HOST


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager


class HostCLIStarter:
    def __init__(self, manager: 'NetworkManager', ip: str, port: int = 61195, name: str = None):
        host = manager.host_manager.get_net_host(CLI_HOST)
        self.manager = manager
        if host is None:
            print("CLI not configured")
            return
        self.ip = ip
        self.port = port
        self.name = name
        self.cli_host = host
        self.net = manager._net
        self.run()

    def run(self):
        cmd = f"python3 -m wattson.topology.cli {self.ip} --port {self.port} --name \"{self.name}\""
        self.manager.cli.subprocess = self.cli_host.popen(cmd, shell=True, stdout=sys.stdout, stdin=sys.stdin,
                                                          stderr=sys.stderr)
        self.manager.cli.subprocess.communicate()
        self.manager.cli.subprocess.wait(5)
        self.manager.cli.subprocess.kill()
        self.manager.cli.subprocess = None

