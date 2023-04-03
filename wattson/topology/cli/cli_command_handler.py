import abc
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from wattson.topology.cli.cli import CLI


class CliCommandHandler(abc.ABC):
    def __init__(self, cli: 'CLI'):
        self.cli = cli

    @abc.abstractmethod
    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        """
        Gets a command divided into a list to apply the respective actions
        :param command: The command split into a list at spaces
        :param prefix: The command's prefix. Original input is 'prefix command'
        :return: True iff the CLI should continue to prompt for the next command
        """
        ...

    @abc.abstractmethod
    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        """
        Generates a nested dictionary that provides all possible auto complete options.
        In case a level is given, the dictionary's nesting level can be restricted to this level.
        :param prefix The command prefix that is relevant for this auto completion
        :param level Optional restriction of the maximum nesting level of the dictionary that allows to skip
                     non-static command completions on lower nesting levels
        :return: A nested dictionary with auto complete options. Each dictionary key is a string that contains no spaces
        """
        ...

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        """
        Returns the help string to be printed in result of a `help XY` command.
        In case the help-page is requested for a subcommand, the respective command is provided as a list
        as the first argument
        :param prefix: The command's prefix
        :param subcommand: The optionally requested subcommand
        :return: The help text for the requested command
        """
        return None

    @abc.abstractmethod
    def description(self, prefix: List[str]) -> str:
        ...
