from typing import Optional, List, TYPE_CHECKING

import tabulate

if TYPE_CHECKING:
    from wattson.cosimulation.cli.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class HelpCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("help", self)

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if len(command) == 0:
            rows = []
            for cmd, handler in sorted(self.cli.get_handlers().items(), key=lambda x: x[0]):
                rows.append([cmd, handler.description(cmd.split(" "))])
            print(tabulate.tabulate(rows))
            return True
        help_prefix, handler = self.cli.get_handler(command)
        if handler is None:
            self.cli.unknown_command(command)
            return True
        subcommand = None
        if len(command) > len(help_prefix):
            subcommand = command[len(help_prefix):]
        help_str = handler.help(help_prefix, subcommand)
        if help_str is None:
            print(f"No help available for {self.cli.get_command_str(command)}")
        else:
            lines = help_str.split("\n")
            ref_line = lines[0]
            if len(lines) > 1:
                ref_line = lines[1]
            adjusted = ref_line.lstrip()
            spacing = len(ref_line) - len(adjusted)
            for line in lines:
                print(line[spacing:])
        return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        options = {
            "help": {
                "children": self.cli.get_completer().get_completions(["help"]),
                "description": "Print help"
            }
        }
        options["help"]["help"] = None
        return options

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return """
        Wattson CLI help.
        Use 'help' to get a list of known top-level commands.
        
        Use 'help <cmd>' to get details on a certain command
        """

    def description(self, prefix: List[str]) -> str:
        return "Print help information"
