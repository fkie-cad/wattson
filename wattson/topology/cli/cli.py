import sys
import typing
from subprocess import Popen
from threading import Event, Lock

import readline

from wattson.topology.cli.cli_command_handler import CliCommandHandler
from wattson.topology.cli.cli_completer import CLICompleter
from wattson.topology.cli.commands.link_cli_command import LinkCliCommand
from wattson.topology.cli.commands.switch_cli_command import SwitchCliCommand
from wattson.topology.cli.commands.exit_cli_command import ExitCliCommand
from wattson.topology.cli.commands.help_cli_command import HelpCliCommand
from wattson.topology.cli.commands.host.cli_cli_command import CliCliCommand
from wattson.topology.cli.commands.host.deployment_cli_command import DeploymentCliCommand
from wattson.topology.cli.commands.host.log_cli_command import LogCliCommand
from wattson.topology.cli.commands.host.term_cli_command import TermCliCommand
from wattson.topology.cli.commands.host_cli_command import HostCliCommand
from wattson.topology.cli.commands.hosts_cli_command import HostsCliCommand
from wattson.topology.cli.commands.mininet_cli_command import MininetCliCommand
from wattson.topology.cli.commands.plot_cli_command import PlotCliCommand
from wattson.topology.cli.commands.reboot_cli_command import RebootCliCommand
from wattson.topology.cli.commands.tap_cli_command import TapCliCommand
from wattson.topology.constants import SYSTEM_NAME

if typing.TYPE_CHECKING:
    from wattson.topology.network_manager import NetworkManager


class CLI:
    def __init__(self, importer: 'NetworkManager'):
        self.importer = importer
        self._busy_lock = Lock()
        self._shutdown = Event()
        self.subprocess: typing.Optional[Popen] = None
        self.completer = None
        self.hosts = []
        self.handlers = {}

    def is_blocked(self):
        if self.subprocess is None and readline.get_line_buffer() == "":
            return False
        return True

    def clear_input_on_interrupt(self):
        if self.subprocess is None and readline.get_line_buffer() != "":
            raise KeyboardInterrupt()

    def kill(self):
        self._shutdown.set()
        if self._busy_lock.locked():
            # Currently awaiting input
            raise KeyboardInterrupt()

    def register_command(self, namespace: typing.Union[str, typing.List[str]], handler: CliCommandHandler):
        if type(namespace) == str:
            namespace = [namespace]
        self.handlers[" ".join(namespace)] = handler

    def get_handler(self, command: typing.Union[str, typing.List[str]]) \
            -> typing.Tuple[typing.List[str], typing.Optional[CliCommandHandler]]:

        if type(command) == str:
            command = command.split(" ")
        # Get handler with longest prefix match if any
        handler = None
        while handler is None and len(command) > 0:
            handler = self.handlers.get(" ".join(command), None)
            if handler is None:
                command = command[:-1]
        return command, handler

    def unknown_command(self, command: typing.Union[str, typing.List[str]]):
        print(f"Unknown command: {self.get_command_str(command)}")

    def invalid_command(self, command: typing.Union[str, typing.List[str]]):
        print(f"Invalid command: {self.get_command_str(command)}")

    def get_command_str(self, command: typing.Union[str, typing.List[str]]) -> str:
        if type(command) == list:
            return ' '.join(command)
        return command

    def get_host_completion_dict(self, value=None):
        return {hid: value for hid in self.hosts}

    def get_link_completion_dict(self, value=None):
        completion_dict = {}
        only_links = {link["id"]: None for link in self.importer.get_links()}
        for cmd in value:
            if cmd == "modify":
                completion_dict[cmd] = {link: {"bw": None, "jitter": None, "delay": None, "loss": None} for link in only_links.keys()}
            elif cmd == "list":
                completion_dict[cmd] = None
            elif cmd == "find":
                nodes = self.importer.get_nodes()
                completion_dict[cmd] = {node["id"]: {node["id"]: None for node in nodes} for node in nodes}
            else:
                completion_dict[cmd] = only_links
        return completion_dict

    def _init_providers(self):
        # Meta commands
        ExitCliCommand(self)
        HelpCliCommand(self)
        RebootCliCommand(self)
        PlotCliCommand(self)
        MininetCliCommand(self)

        # Topology control
        TapCliCommand(self)

        # Link and Switch control
        SwitchCliCommand(self)
        LinkCliCommand(self)

        # Multiple hosts
        HostsCliCommand(self)

        # Host-related commands
        HostCliCommand(self)
        DeploymentCliCommand(self)
        TermCliCommand(self)
        LogCliCommand(self)
        CliCliCommand(self)

    def run(self):
        print("")
        print(f"{SYSTEM_NAME} CLI")
        readline.parse_and_bind('tab: complete')
        readline.set_completer_delims(" ")
        self.hosts = [h["id"] for h in self.importer.get_hosts()]
        self._init_providers()
        self.completer = CLICompleter(self.importer, self)
        self.completer.setup()
        readline.set_completer(self.completer.complete)

        while not self._shutdown.is_set():
            try:
                with self._busy_lock:
                    cmd = input(f"{SYSTEM_NAME}> ")
                if self._shutdown.is_set():
                    return
                if not self._parse_cmd(cmd):
                    self.kill()
                    return
            except KeyboardInterrupt:
                print("")
                if readline.get_line_buffer() != "" and not self._shutdown.is_set():
                    print("[Ctrl+C]")
                    continue
                return
            except Exception as e:
                print("Error while handling the command:")
                print(f"{e=}")

    def handle_command(self, command: typing.List[str]) -> bool:
        prefix, handler = self.get_handler(command)
        if handler is None:
            self.unknown_command(command)
            return True

        prefix_len = len(prefix)
        if prefix_len > len(command):
            self.unknown_command(command)
            return True

        command = command[prefix_len:]

        return handler.handle_command(command, prefix)

    def _parse_cmd(self, cmd):
        if cmd == "":
            return True
        parts = cmd.split(" ")
        # Remove empty entries
        parts = [p for p in parts if p]
        if len(parts) == 0:
            return True

        return self.handle_command(parts)
