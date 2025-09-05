from typing import Optional
import shlex
import argparse

from wattson.services.deployment.cli.messages import CLIMessage


class CommandMessage(CLIMessage):
    """
    A message that contains a printing command, mostly in server to client communication.
    Messages can be formatted in different ways, e.g. as table or plain text.

    """
    def __init__(self, msg_dict: Optional[dict] = None):
        super().__init__(msg_dict)
        self.string_cmd = ""
        if msg_dict is not None:
            self._from_dict(msg_dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": "command",
            "cmd": self.string_cmd
        }

    def parse(self) -> dict:
        # Empty object
        if self.string_cmd.strip() == "":
            return {"cmd": [""]}
        try:
            split = shlex.split(self.string_cmd)
            parser = argparse.ArgumentParser()
            parser.add_argument('cmd', nargs="+")
            parsed, unknown = parser.parse_known_args(split)
            for arg in unknown:
                if arg.startswith(("-", "--")):
                    a = arg.split('=')[0]
                    parser.add_argument(a, type=str)
            cmd = vars(parser.parse_args(split))
            return cmd
        except Exception:
            return {"cmd": [""]}

    def _from_dict(self, msg_dict):
        if msg_dict["type"] != "command":
            raise ValueError("Invalid message type")
        try:
            self.string_cmd = msg_dict["cmd"]
        except KeyError as e:
            raise ValueError(f"Message Dict is not valid: {e}")
