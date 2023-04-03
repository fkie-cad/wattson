import os
import shutil
import subprocess
from pathlib import Path

import psutil
from typing import Optional, List, TYPE_CHECKING

from wattson.util.misc import get_console_and_shell

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class TermCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("term", self)
        self.cli.register_command("kfish", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) != 1:
            self.cli.invalid_command(command)
            return True
        host_id = command[0]
        host = self.cli.importer.get_node(host_id)
        if host is None:
            print(f"Unknown host: {host_id}")
            return True

        cmd = prefix[0]
        terminal = None
        shell = None
        if cmd == "kfish":
            terminal = "konsole"
            shell = "fish"
        self._start_console(host, terminal, shell)
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            prefix[0]: self.cli.get_host_completion_dict()
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return f"""
        {self.description(prefix)}
        Usage: '{prefix[0]} <hostid>'
        """

    def description(self, prefix: List[str]) -> str:
        if prefix[0] == "kfish":
            return "Open a Konsole window with the fish shell for a host"
        return "Open a terminal for a host"

    def _start_console(self, host, terminal=None, shell=None):
        opened = False
        if "deploy" in host:
            if host["deploy"]["type"] in ["python", "script"]:
                print(f"Opening terminal for host {self.importer.host_manager.ghn(host)}")
                self._start_local_console(host, terminal, shell)
                opened = True
            elif host["deploy"]["type"] == "docker":
                print(f"Opening terminal for docker host {self.importer.host_manager.ghn(host)}")
                self._start_docker_console(host)
                opened = True
        if not opened:
            print(f"Opening terminal for host {self.importer.host_manager.ghn(host)}")
            try:
                self._start_local_console(host, terminal, shell)
            except Exception as e:
                print(f"Failed to open terminal: {e}")

    def _start_docker_console(self, host):
        terminal, shell = self._get_console_command()
        # node = self.importer.get_net_host(host)
        container = self.importer.get_container_name(host)
        shell = "/bin/bash"
        cmd = f"docker exec -it {container} {shell}"
        import subprocess
        subprocess.Popen([terminal, "-e", f"{cmd}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _start_local_console(self, host, terminal=None, shell=None):
        if terminal is None or shell is None:
            terminal, shell = self._get_console_command()
        # host = self.importer.get_node(host)
        # namespace = self.importer.get_network_namespace(host)
        # node = self.importer.host_manager.get_net_host(host)
        cwd: str = host.get("cwd", ".")
        if cwd.startswith("/"):
            cwd = str(Path(cwd).absolute())
        else:
            cwd = str(self.importer.path.joinpath(cwd).absolute())
        print(f"Opening {shell} via {terminal} at {cwd}")
        divider = "-e"
        use_shell = False
        pre_cmd = ""
        if "gnome-terminal" in terminal:
            divider = "--"
            dbus = shutil.which("dbus-launch")
            pre_cmd = f"{dbus} "
            use_shell = True

        cmd = f"{pre_cmd}{terminal} {divider} {shell}"
        self.importer.deployment.popen(
            host, cmd, cwd=cwd, stdout=subprocess.DEVNULL, shell=use_shell
        )

    def _get_console_command(self):
        pid = os.getpid()
        return get_console_and_shell(pid)
