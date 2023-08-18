from typing import Optional

from wattson.services.deployment.cli.messages import CLIMessage


class AutoCompleteMessage(CLIMessage):
    """
    A message that contains a printing command, mostly in server to client communication.
    Messages can be formatted in different ways, e.g. as table or plain text.
    """
    def __init__(self, msg_dict: Optional[dict] = None):
        super().__init__(msg_dict)
        self.request = None
        self.response = None
        if msg_dict is not None:
            self._from_dict(msg_dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": "auto_complete",
            "request": self.request,
            "response": self.response
        }

    def reply(self, response):
        if self.interface is None:
            raise ValueError("Cannot reply without a communication interface being set")
        self.response = response
        self.interface.send(self)

    def _from_dict(self, msg_dict):
        if msg_dict["type"] != "print":
            raise ValueError("Invalid message type")
        try:
            self.data = msg_dict["data"]
            self.format = msg_dict["format"]
        except KeyError as e:
            raise ValueError(f"Message Dict is not valid: {e}")
