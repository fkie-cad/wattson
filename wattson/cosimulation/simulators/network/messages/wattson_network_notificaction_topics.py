import enum


class WattsonNetworkNotificationTopic(str, enum.Enum):
    SERVICE_EVENT = "service-event"
    LINK_PROPERTY_CHANGED = "link-property-changed"
    TOPOLOGY_CHANGED = "topology-changed"
    NODE_EVENT = "node-event"
    NODE_CUSTOM_EVENT = "node-custom-event"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
