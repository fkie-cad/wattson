#import argparse
import readline
#import sys
from threading import Event

from wattson.deployment.cli.client.client import CLIClient
from typing import TYPE_CHECKING

from wattson.deployment.cli.messages import PrintMessage, SystemMessage, SystemMessageType

if TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager


class HostCLI:
    def __init__(self, ip: str, port: int = 61195, name: str = None):
        self.ip = ip
        self.port = port
        self.hostname = name if name is not None else ip
        self.cli = None
        self.next_input = Event()
        self.stop_event = Event()
        self.start()

    def start(self):
        self.cli = CLIClient(server_ip=self.ip, server_port=self.port,
                             print_callback=self._print_callback, system_callback=self._sys_callback)
        self.cli.start()
        try:
            self.cli.wait_connect()
            self.next_input.set()
            readline.parse_and_bind('tab: complete')
            cont = True
            while cont:
                self.next_input.wait()
                self.next_input.clear()
                if self.stop_event.is_set():
                    self.cli.stop()
                    self.cli.join()
                    return

                cmd = input(f"{self.hostname}> ")
                try:
                    self.cli.cmd(cmd)
                except Exception as e:
                    print("Could not handle your Command:")
                    print(repr(e))
        except KeyboardInterrupt:
            self.cli.stop()
            self.cli.join()
            return
        #self.prompt()

    def prompt(self):
        self._show_prompt()

    def _show_prompt(self):
        print("")

    def _print_callback(self, message: PrintMessage):
        print(message.to_str(80), flush=True)
        if message.follow_prompt:
            self.next_input.set()

    def _sys_callback(self, message: SystemMessage):
        if message.sys_message_type == SystemMessageType.OK \
                or message.sys_message_type == SystemMessageType.ENABLE_PROMPT \
                or message.sys_message_type == SystemMessageType.CONNECT:
            self.next_input.set()
        elif message.sys_message_type == SystemMessageType.DISCONNECT:
            self.stop_event.set()
            self.next_input.set()
