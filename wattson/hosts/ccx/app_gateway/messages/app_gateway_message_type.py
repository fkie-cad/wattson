import enum


class AppGatewayMessageType(str, enum.Enum):
    # MANAGEMENT
    REGISTRATION = "registration"
    RESPONSE = "response"
    RESOLVE_ASYNC_RESPONSE = "resolve_async_response"

    # PUBLISHING MESSAGES
    DATA_POINT_RECEIVED = "data_point_received"
    DATA_POINT_COMMAND_SENT = "data_point_command_sent"
    DATA_POINT_COMMAND_REPLY = "data_point_command_reply"
    RAW_PACKET_RECEIVED = "raw_packet_received"
    RAW_PACKET_SENT = "raw_packet_sent"

    CONNECTION_CHANGE = "connection_change"

    CLIENT_EVENT = "client_event"

    # DATA POINTS
    REQUEST_DATA_POINTS = "request_data_points"
    REQUEST_GRID_VALUE_MAPPING = "request_grid_value_mapping"

    # COMMAND MESSAGES
    SET_DATA_POINT_COMMAND = "set_data_point_command"
    READ_DATA_POINT_COMMAND = "read_data_point_command"
    TRIGGER_INTERROGATION = "trigger_interrogation"
    DISCONNECT = "disconnect"
    CONNECT = "connect"
    PROTOCOL_COMMAND = "protocol_command"
    GET_NODE_STATUS = "get_node_status"
