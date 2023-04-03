from typing import Union, Any

from wattson.powergrid.messages.coordination_message import CoordinationMessage


class RequestResponseMessage(CoordinationMessage):
    def __init__(self, request: dict = None, response: dict = None, type: str = None):
        self.request = {
            "type": "INVALID"
        }
        if request is not None:
            self.request.update(request)
        if type is not None:
            self.request["type"] = type
        self.response = response

    def answer(self, answer: Union[dict, Any]):
        if isinstance(answer, dict):
            self.response = answer
            if "success" not in self.response:
                self.response["success"] = True
        else:
            self.response = {
                "value": answer,
                "success": True
            }

    def ok(self):
        self.response = {
            "value": None,
            "success": True
        }

    def fail(self):
        self.response = {
            "value": None,
            "success": False
        }

    def is_successful(self):
        return isinstance(self.response, dict) and "success" in self.response and self.response["success"]