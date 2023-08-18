import json
from abc import ABC, abstractmethod
from typing import Optional


class CLIMessage(ABC):
    last_id = 0

    def __init__(self, msg_dict: Optional[dict] = None):
        self.id = CLIMessage._next_id()
        self.interface = None
        if msg_dict is not None and "id" in msg_dict:
            self.id = msg_dict["id"]

    def to_json(self) -> str:
        d = self.to_dict()
        if "id" not in d:
            d["id"] = self.id
        return json.dumps(d)

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @abstractmethod
    def _from_dict(self, msg_dict):
        ...

    @staticmethod
    def _next_id():
        CLIMessage.last_id += 1
        return CLIMessage.last_id

    @staticmethod
    def from_str(message_str):
        from wattson.services.deployment.cli.messages import PrintMessage, CommandMessage, AutoCompleteMessage, SystemMessage
        cli_dict = json.loads(message_str)
        if "type" not in cli_dict:
            raise ValueError("CLIMessage requires a type")
        t = cli_dict["type"]
        if t == "print":
            return PrintMessage(cli_dict)
        elif t == "auto_complete":
            return AutoCompleteMessage(cli_dict)
        elif t == "command":
            return CommandMessage(cli_dict)
        elif t == "system":
            return SystemMessage(cli_dict)
        raise ValueError(f"Unknown CLIMessage type {t}")

