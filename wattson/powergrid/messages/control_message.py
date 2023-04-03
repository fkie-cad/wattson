from wattson.powergrid.messages.coordination_message import CoordinationMessage
from wattson.powergrid.messages.control_message_type import ControlMessageType


class ControlMessage(CoordinationMessage):
    def __init__(self, type: ControlMessageType):
        self.type: ControlMessageType = type
