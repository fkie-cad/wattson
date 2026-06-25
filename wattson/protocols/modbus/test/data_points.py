from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName

MODBUS_TEST_DATA_POINTS = [
    {
        "identifier": "ir",
        "protocol_server_id": "123",
        "protocol": ProtocolName.MODBUS_TCP.value,
        "protocol_data": {
            "address": 20000,
            "table_offset": 20000,
            "zero_based_pdu_address": 0,
            "table": "input register",
            "direction": "monitoring",
            "field_id": "RTU 101",
            "type_id": "float32",
            "unit_id": 0,
            "width": 2,
            "endian": "big-endian",
            "polling_interval": 10,
            "polling_enabled": True
        }
    },
    {
        "identifier": "ts",
        "protocol_server_id": "123",
        "protocol": ProtocolName.MODBUS_TCP.value,
        "protocol_data": {
            "address": 20002,
            "table_offset": 20000,
            "zero_based_pdu_address": 2,
            "table": "input register",
            "direction": "monitoring",
            "field_id": "RTU 101",
            "type_id": "float64",
            "unit_id": 0,
            "width": 4,
            "endian": "big-endian",
            "polling_interval": 5,
            "polling_enabled": True
        }
    },
    {
        "identifier": "hr",
        "protocol_server_id": "123",
        "protocol": ProtocolName.MODBUS_TCP.value,
        "protocol_data": {
            "address": 40000,
            "table_offset": 40000,
            "zero_based_pdu_address": 0,
            "table": "holding register",
            "direction": "control",
            "field_id": "RTU 101",
            "type_id": "float32",
            "unit_id": 0,
            "width": 2,
            "endian": "little-endian",
            "polling_interval": 0
        }
    },
    {
        "identifier": "di",
        "protocol_server_id": "123",
        "protocol": ProtocolName.MODBUS_TCP.value,
        "protocol_data": {
            "address": 10000,
            "table_offset": 10000,
            "zero_based_pdu_address": 0,
            "table": "discrete input",
            "direction": "monitoring",
            "field_id": "RTU 101",
            "type_id": "bool",
            "unit_id": 0,
            "width": 1,
            "endian": "big-endian",
            "polling_interval": 0
        }
    },
    {
        "identifier": "c",
        "protocol_server_id": "123",
        "protocol": ProtocolName.MODBUS_TCP.value,
        "protocol_data": {
            "address": 5,
            "table_offset": 0,
            "zero_based_pdu_address": 5,
            "table": "coil",
            "direction": "control",
            "field_id": "RTU 101",
            "type_id": "bool",
            "unit_id": 0,
            "width": 1,
            "endian": "big-endian",
            "polling_interval": 0
        }
    }
]
