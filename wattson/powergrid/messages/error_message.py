from wattson.powergrid.messages.coordination_message import CoordinationMessage


class ErrorMessage(CoordinationMessage):
    """
    Sent from coordinator to client when an error occurred.
    """
    def __init__(self, excp: Exception = None, msg: str = ""):
        self.excp = excp
        self.msg = msg
