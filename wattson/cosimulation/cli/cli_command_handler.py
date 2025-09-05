import abc
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from wattson.cosimulation.cli.cli import CLI


class CliCommandHandler(abc.ABC):
    def __init__(self, cli: 'CLI'):
        self.cli = cli

    @abc.abstractmethod
    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        """
        Gets a command divided into a list to apply the respective actions

        Args:
            command (List[str]):
                The command split into a list at spaces
            prefix (List[str]):
                The command's prefix. Original input is 'prefix command'

        Returns:
            bool: True iff the CLI should continue to prompt for the next command
        """
        ...

    @abc.abstractmethod
    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        """
        Generates a nested dictionary that provides all possible auto complete options.
        In case a level is given, the dictionary's nesting level can be restricted to this level.

        Args:
            prefix (List[str]):
                The command prefix that is relevant for this auto completion
            level (Optional[int], optional):
                Optional restriction of the maximum nesting level of the dictionary that allows to skip non-static command completions on lower
                nesting levels
                (Default value = None)

        Returns:
            dict: A nested dictionary with auto complete options. Each dictionary key is a string that contains no spaces
        """
        ...

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        """
        Returns the help string to be printed in result of a `help XY` command.
        In case the help-page is requested for a subcommand, the respective command is provided as a list as the first argument

        Args:
            prefix (List[str]):
                The command's prefix
            subcommand (Optional[List[str]], optional):
                The optionally requested subcommand
                (Default value = None)

        Returns:
            Optional[str]: The help text for the requested command
        """
        return None

    @abc.abstractmethod
    def description(self, prefix: List[str]) -> str:
        ...
