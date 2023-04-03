from wattson.powergrid.messages import CoordinationMessage


class PublishMessage(CoordinationMessage):
    def __init__(self, data: dict | list, topic: str):
        self.data = data
        self.topic = topic
