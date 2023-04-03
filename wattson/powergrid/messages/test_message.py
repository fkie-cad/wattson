from wattson.powergrid.messages.coordination_message import CoordinationMessage


class TestMessage(CoordinationMessage):
    """
    Send to test connection. Must send an identifier, such that the coordinator
    can track which nodes are available.
    """

    def __init__(self, node_id: str, register=True):
        self.node_id = node_id
        self.register = register

    def __eq__(self, other):
        """
        Consider two test messages equal if their single attribute is equal.
        :param other:
        :return:
        """
        return self.node_id == other.node_id and self.register == other.register