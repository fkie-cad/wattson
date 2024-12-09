import enum


class WattsonNetworkQueryType(str, enum.Enum):
    GET_NODES = "get-nodes"
    ADD_NODE = "add-node"
    REMOVE_NODE = "remove-node"
    CONNECT_NODES = "connect-nodes"

    SET_INTERFACE_IP = "set-interface-ip"
    SET_INTERFACE_UP = "set-interface-up"
    SET_INTERFACE_DOWN = "set-interface-down"
    REMOVE_INTERFACE = "remove-interface"
    CREATE_INTERFACE = "create-interface"

    GET_LINKS = "get-links"
    GET_LINK_MODEL = "get-link-model"
    SET_LINK_PROPERTY = "set-link-property"
    SET_LINK_UP = "set-link-up"
    SET_LINK_DOWN = "set-link-down"
    GET_LINK_STATE = "get-link-state"
    REMOVE_LINK = "remove-link"

    GET_SERVICES = "get-services"
    GET_SERVICE = "get-service"
    SERVICE_ACTION = "service-action"
    ADD_SERVICE = "add-service"

    NODE_ACTION = "node-action"
    GET_ENTITY = "get-entity"
    PROCESS_ACTION = "process-action"

    UPDATE_NODE_CONFIGURATION = "update-node-configuration"
    GET_UNUSED_IP = "get-unused-ip"

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.value
        if isinstance(other, self.__class__):
            return other.name == self.name
        return False

    def __hash__(self):
        return hash(self.value)
