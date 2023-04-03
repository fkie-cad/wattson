from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class DeploymentCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("start", self)
        self.cli.register_command("stop", self)
        self.cli.register_command("restart", self)
        self.cli.register_command("pcap", self)
        self.cli.register_command("screen", self)
        self.cli.register_command("info", self)

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

        if cmd == "stop":
            if self.importer.deployment.host_stop(host):
                print("Host stopped")
            else:
                print("Host stop failed")
        elif cmd == "start":
            if self.importer.deployment.host_start(host):
                print("Host started")
            else:
                print("Host start failed")
        elif cmd == "restart":
            if self.importer.deployment.host_restart(host):
                print("Host restarted")
            else:
                print("Host restart failed")
        elif cmd == "pcap":
            self.importer.deployment.start_pcap(host)
        elif cmd == "screen":
            self.importer.deployment.start_screen(host)
        elif cmd == "info":
            print(self.importer.host_manager.ghn(host_id))
            print(f"Primary PID: {self.importer.deployment.host_get_pid(host_id)}")
            print(f"Running Processes: {self.importer.deployment.host_num_processes(host_id)}")
            print(f"Deployment PIDs: {self.importer.deployment.host_get_pids(host_id)}")
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        return {
            prefix[0]: self.cli.get_host_completion_dict()
        }

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return f"""
        {self.description(prefix)}
        Usage: {prefix[0]} <hostid>
        """

    def description(self, prefix: List[str]) -> str:
        if prefix[0] == "pcap":
            return f"Start a PCAP for a host"
        elif prefix[0] == "screen":
            return f"Start a new screen session for a host"
        elif prefix[0] == "info":
            return f"Show process information for a host"
        else:
            command = prefix[0].title()
            return f"{command} a host process"
