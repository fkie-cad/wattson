from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI

from wattson.topology.cli.cli_command_handler import CliCommandHandler


class LogCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.importer = self.cli.importer
        self.cli.register_command("log", self)
        self.cli.register_command("logf", self)

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
        follow = cmd == "logf"

        logfiles = self.importer.deployment.host_get_active_logs(host_id)
        count = len(logfiles)
        show = None
        if count == 0:
            print(f"Host has no active log files")
            logfiles = self.importer.deployment.host_get_all_logs(host_id)
            if len(logfiles) > 0:
                print("Showing log of last process")
                show = logfiles[-1]
        else:
            print(f"Host has {count} active log files")
            show = logfiles[-1]

        if show is None:
            return True

        from subprocess import Popen
        try:
            if follow:
                self.cli.subprocess = Popen(["less", "+F", show.absolute().__str__()])
            else:
                self.cli.subprocess = Popen(["less", show.absolute().__str__()])
            self.cli.subprocess.wait()
        except KeyboardInterrupt:
            pass
        self.cli.subprocess.kill()
        self.cli.subprocess = None
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
        d_str = "Open the most recent log file for this host"
        if prefix[0] == "logf":
            d_str += " in follow mode"
        return d_str
