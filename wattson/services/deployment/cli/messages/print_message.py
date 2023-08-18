from typing import Optional

from wattson.services.deployment.cli.messages import CLIMessage


class PrintMessage(CLIMessage):
    """
    A message that contains a printing command, mostly in server to client communication.
    Messages can be formatted in different ways, e.g. as table or plain text.
    """
    def __init__(self, msg_dict: Optional[dict] = None):
        super().__init__(msg_dict)
        self.data = ""
        self.format = "plain"
        self.follow_prompt = True
        if msg_dict is not None:
            self._from_dict(msg_dict)

    def to_str(self, width: int = 0):
        """
        returns the message representation as string.
        If a width is given, certain formatting options will respect this as a limit for object widths.
        """
        if self.format == "plain":
            return self.data
        if self.format == "table":
            return f"NIY: Table display\n{self.data}"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": "print",
            "data": self.data,
            "format": self.format,
            "follow_prompt": self.follow_prompt
        }

    def _from_dict(self, msg_dict):
        if msg_dict["type"] != "print":
            raise ValueError("Invalid message type")
        try:
            self.data = msg_dict["data"]
            self.format = msg_dict["format"]
            self.follow_prompt = msg_dict["follow_prompt"]
        except KeyError as e:
            raise ValueError(f"Message Dict is not valid: {e}")
