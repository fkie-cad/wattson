import readline
import shlex
import signal
import subprocess
import sys
import threading
import traceback
from typing import Optional, Union, List, Tuple, Type, Dict

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler
from wattson.cosimulation.cli.cli_completer import CLICompleter
from wattson.cosimulation.cli.commands.exit_cli_command import ExitCliCommand
from wattson.cosimulation.cli.commands.grid_value_cli_command import GridValueCliCommand
from wattson.cosimulation.cli.commands.help_cli_command import HelpCliCommand
from wattson.cosimulation.cli.commands.service_cli_command import ServiceCliCommand
from wattson.cosimulation.cli.commands.link_cli_command import LinkCliCommand
from wattson.cosimulation.cli.commands.node_cli_command import NodeCliCommand
from wattson.cosimulation.cli.commands.shutdown_cli_command import ShutdownCliCommand
from wattson.cosimulation.cli.commands.firewall_cli_command import FirewallCliCommand
from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic


class CLI:
    instance: Optional['CLI'] = None
    handler_classes: List[Type[CliCommandHandler]] = []

    def __init__(self, wattson_client: WattsonClient, default_sig_int_handler: Optional = None):
        if CLI.instance is None:
            CLI._add_default_handlers()

        CLI.instance = self
        self._client = wattson_client
        self._default_sig_int_handler = default_sig_int_handler

        self._ignore_keyboard_interrupt = True
        self._busy_lock = threading.Lock()
        self._shutdown_requested = threading.Event()
        self._completer = None
        self._subprocess: Optional[subprocess.Popen] = None
        self._handlers = {}

    @property
    def wattson_client(self) -> 'WattsonClient':
        return self._client

    def is_blocked(self):
        return self._subprocess is None and readline.get_line_buffer() == ""

    def clear_input_on_interrupt(self):
        if self._subprocess is None and readline.get_line_buffer() != "":
            raise KeyboardInterrupt()

    def kill(self):
        self._shutdown_requested.set()
        if self._busy_lock.locked():
            # Clear current input block
            raise KeyboardInterrupt()

    def get_completer(self) -> CLICompleter:
        return self._completer

    """
    RUN / MAIN LOOP
    """
    def run(self):
        print("")
        readline.parse_and_bind("tab: complete")
        readline.set_completer_delims(" ")
        self._init_handlers()
        self._completer = CLICompleter(self)
        print("Loading CLI...")
        self._completer.setup()
        readline.set_completer(self._completer.complete)

        # Update logics on topology changes
        self.wattson_client.subscribe(WattsonNetworkNotificationTopic.TOPOLOGY_CHANGED, lambda _: self._completer.setup())

        if self._default_sig_int_handler is not None:
            signal.signal(signal.SIGINT, self._default_sig_int_handler)
        while not self._shutdown_requested.is_set():
            try:
                with self._busy_lock:
                    cmd = input("Wattson> ")
                if self._shutdown_requested.is_set():
                    return
                if not self.handle_command(cmd):
                    self.kill()
                    return
            except KeyboardInterrupt:
                print("")
                if (readline.get_line_buffer() != "" or self._ignore_keyboard_interrupt) and not self._shutdown_requested.is_set():
                    print("[Ctrl+C]")
                    continue
                return
            except Exception as e:
                print(f"Error while handling command {cmd}")
                print(f"{e=}")
                traceback.print_exception(*sys.exc_info())

    """
    HANDLERS
    """
    def register_command(self, command_hierarchy: Union[str, List[str]], handler: CliCommandHandler):
        """
        Alias for register_handler.

        Args:
            command_hierarchy (Union[str, List[str]]):
                
            handler (CliCommandHandler):
                
        """
        return self.register_handler(command_hierarchy=command_hierarchy, handler=handler)

    def register_handler(self, command_hierarchy: Union[str, List[str]], handler: CliCommandHandler):
        """
        Registers a new handler for the given command (hierarchy).

        Args:
            command_hierarchy (Union[str, List[str]]):
                The command or combination of command and subcommand(s)
            handler (CliCommandHandler):
                The handler to assign for the given hierarchy.
        """
        if type(command_hierarchy) == str:
            command_hierarchy = [command_hierarchy]
        self._handlers[" ".join(command_hierarchy)] = handler

    def get_handler(self, command: Union[str, List[str]]) -> Tuple[List[str], Optional[CliCommandHandler]]:
        """
        Returns the command and a CliCommandHandler for the given command if registered

        Args:
            command (Union[str, List[str]]):
                The command to get the handler for

        Returns:
            Tuple[List[str],Optional[CliCommandHandler]]: The prefix matched for the handler and the assigned Handler, if any
        """
        if type(command) == str:
            command = command.split(" ")
        handler = None
        # Search the responsible handler, start with the most specific one
        while handler is None and len(command) > 0:
            cmd = " ".join(command)
            handler = self._handlers.get(cmd, None)
            if handler is None:
                command = command[:-1]
        return command, handler

    def get_handlers(self) -> Dict[str, CliCommandHandler]:
        return self._handlers

    def _init_handlers(self):
        for handler_class in self.handler_classes:
            handler_class(self)

    @staticmethod
    def add_handler_class(handler_class: Type[CliCommandHandler]):
        CLI.handler_classes.append(handler_class)

    @staticmethod
    def _add_default_handlers():
        CLI.add_handler_class(ExitCliCommand)
        CLI.add_handler_class(HelpCliCommand)
        CLI.add_handler_class(ShutdownCliCommand)
        # Nodes / Hosts
        CLI.add_handler_class(NodeCliCommand)
        CLI.add_handler_class(ServiceCliCommand)
        CLI.add_handler_class(FirewallCliCommand)
        # Links
        CLI.add_handler_class(LinkCliCommand)
        # Power Grid
        CLI.add_handler_class(GridValueCliCommand)

    """
    Command handling
    """
    def handle_command(self, command: Union[List[str], str]) -> bool:
        """
        Handles the given command

        Args:
            command (Union[List[str], str]):
                The command to handle

        Returns:
            bool: Whether the next command should be handled. False if the CLI should terminate.
        """
        command: List[str] = self._sanitize_command(command)
        if len(command) == 0:
            return True
        prefix, handler = self.get_handler(command=command)
        if handler is None:
            self.unknown_command(command)
            return True
        prefix_len = len(prefix)
        if prefix_len > len(command):
            self.unknown_command(command)
            return True
        command = command[prefix_len:]
        return handler.handle_command(command, prefix)

    @staticmethod
    def _sanitize_command(command: Union[List[str], str]) -> List[str]:
        if isinstance(command, str):
            parts = shlex.split(command)
        else:
            parts = command
        return [p for p in parts if p]

    def invalid_command(self, command: Union[str, List[str]]):
        """
        Handles invalid commands

        Args:
            command (Union[str, List[str]]):
                The invalid command
        """
        print(f"Invalid command: {self.get_command_str(command)}")

    def unknown_command(self, command: Union[str, List[str]]):
        """
        Handles unknown commands

        Args:
            command (Union[str, List[str]]):
                The unknown command
        """
        print(f"Unknown command: {self.get_command_str(command)}")

    @staticmethod
    def get_command_str(command: Union[str, List[str]]) -> str:
        if type(command) == list:
            return ' '.join(command)
        return command

    @staticmethod
    def get_instance(wattson_client: Optional[WattsonClient]):
        if CLI.instance is None:
            if wattson_client is not None:
                return CLI(wattson_client)
            raise RuntimeError("No CLI instance created")
        return CLI.instance
