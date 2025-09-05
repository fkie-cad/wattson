from typing import Optional
from typing import TYPE_CHECKING

from wattson.services.deployment.cli.messages import CLIMessage

if TYPE_CHECKING:
    from wattson.services.deployment.cli.messages import SystemMessageType


class SystemMessage(CLIMessage):
    """
    A message that contains a printing command, mostly in server to client communication.
    Messages can be formatted in different ways, e.g. as table or plain text.

    """
    def __init__(self, msg_dict: Optional[dict] = None):
        super().__init__(msg_dict)
        from wattson.services.deployment.cli.messages import SystemMessageType
        self.sys_message_type = SystemMessageType.PING
        self.data = {}
        if msg_dict is not None:
            self._from_dict(msg_dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": "system",
            "sys_message_type": self.sys_message_type.value,
            "data": self.data
        }

    def _from_dict(self, msg_dict):
        from wattson.services.deployment.cli.messages import SystemMessageType
        if msg_dict["type"] != "system":
            raise ValueError("Invalid message type")
        try:
            self.data = msg_dict["data"]
            self.sys_message_type = SystemMessageType(msg_dict["sys_message_type"])
        except KeyError as e:
            raise ValueError(f"Message Dict is not valid: {e}")

    @staticmethod
    def factory(sys_type: 'SystemMessageType'):
        msg = SystemMessage()
        msg.sys_message_type = sys_type
        return msg